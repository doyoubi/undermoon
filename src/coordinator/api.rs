use super::core::CoordinateError;
use super::service::CoordinatorConfig;
use crate::common::response;
use crate::common::track::TrackedFutureRegistry;
use crate::common::utils::resolve_first_address;
use crate::protocol::{Array, BulkStr, Resp, RespPacket, RespVec};
use crate::proxy::command::{new_command_pair, CmdType, Command, TaskReply};
use crate::proxy::session::{handle_session, CmdHandler, CmdReplyFuture};
use crate::proxy::slowlog::Slowlog;
use futures::{FutureExt, StreamExt};
use std::num::NonZeroUsize;
use std::str;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;

const SESSION_CHANNEL_SIZE: usize = 1024;
const SESSION_BATCH_MIN_TIME: usize = 0;
const SESSION_BATCH_MAX_TIME: usize = 10000;
const SESSION_BATCH_BUF: usize = 1;

pub struct ApiService {
    config: Arc<CoordinatorConfig>,
    future_registry: Arc<TrackedFutureRegistry>,
}

impl ApiService {
    pub fn new(config: Arc<CoordinatorConfig>) -> ApiService {
        let future_registry = Arc::new(TrackedFutureRegistry::default());
        Self {
            config,
            future_registry,
        }
    }

    pub async fn run(&self) -> Result<(), CoordinateError> {
        info!("config: {:?}", self.config);

        let address = self.config.address.clone();
        let address = resolve_first_address(&address).ok_or_else(|| {
            let err_str = format!("failed to resolve address: {}", address);
            error!("{}", err_str);
            CoordinateError::InvalidAddress
        })?;

        let mut listener = TcpListener::bind(&address).await.map_err(|err| {
            error!("unable to bind address: {} {:?}", address, err);
            CoordinateError::Io(err)
        })?;

        let session_id = AtomicUsize::new(0);
        let config = self.config.clone();
        let session_batch_buf =
            NonZeroUsize::new(SESSION_BATCH_BUF).ok_or_else(|| CoordinateError::InvalidConfig)?;

        let future_registry = self.future_registry.clone();

        let mut s = listener.incoming();
        while let Some(sock) = s.next().await {
            let sock = sock.map_err(CoordinateError::Io)?;

            if let Err(err) = sock.set_nodelay(true) {
                error!("failed to set TCP_NODELAY: {:?}", err);
                return Err(CoordinateError::Io(err));
            }

            let peer = match sock.peer_addr() {
                Ok(address) => address.to_string(),
                Err(e) => format!("Failed to get peer {}", e),
            };
            info!("accept connection: {}", peer);

            let curr_session_id = session_id.fetch_add(1, Ordering::SeqCst);

            let session_handler = handle_session(
                Arc::new(CoordCmdHandler::new(
                    config.clone(),
                    future_registry.clone(),
                )),
                sock,
                SESSION_CHANNEL_SIZE,
                SESSION_BATCH_MIN_TIME,
                SESSION_BATCH_MAX_TIME,
                session_batch_buf,
            );

            let desc = format!("session: session_id={} peer={}", curr_session_id, peer);
            let fut = session_handler.map(move |res| match res {
                Ok(()) => info!("session IO closed {}", peer),
                Err(err) => error!("session IO error {:?} {}", err, peer),
            });
            let fut = TrackedFutureRegistry::wrap(future_registry.clone(), fut, desc);
            tokio::spawn(fut);
        }
        Ok(())
    }
}

struct CoordCmdHandler {
    config: Arc<CoordinatorConfig>,
    future_registry: Arc<TrackedFutureRegistry>,
}

impl CoordCmdHandler {
    fn new(config: Arc<CoordinatorConfig>, future_registry: Arc<TrackedFutureRegistry>) -> Self {
        Self {
            config,
            future_registry,
        }
    }

    fn handle_config(&self, cmd: &Command) -> RespVec {
        let sub_cmd = match Self::get_sub_arg(&cmd, 1) {
            Ok(sub_cmd) => sub_cmd.to_uppercase(),
            Err(err) => {
                return Resp::Error(err.into_bytes());
            }
        };

        let field = match Self::get_sub_arg(&cmd, 2) {
            Ok(field) => field,
            Err(err) => {
                return Resp::Error(err.into_bytes());
            }
        };

        match sub_cmd.as_str() {
            "GET" => {
                let value = match field.as_str() {
                    "brokers" => self.config.get_broker_addresses().join(","),
                    "reporter_id" => self.config.reporter_id.clone(),
                    _ => {
                        return Resp::Error(format!("Invalid CONFIG field {}", field).into_bytes());
                    }
                };
                Resp::Bulk(BulkStr::Str(value.into_bytes()))
            }
            "SET" => {
                let value = match Self::get_sub_arg(&cmd, 3) {
                    Ok(value) => value,
                    Err(err) => {
                        return Resp::Error(err.into_bytes());
                    }
                };

                match field.as_str() {
                    "brokers" => {
                        let addresses = value.split(',').map(|s| s.to_string()).collect();
                        self.config.set_broker_addresses(addresses);
                    }
                    _ => {
                        return Resp::Error(
                            format!("Invalid CONFIG SET field {}", field).into_bytes(),
                        );
                    }
                };
                Resp::Simple(response::OK_REPLY.as_bytes().to_vec())
            }
            _ => Resp::Error(format!("Invalid config sub command: {}", sub_cmd).into_bytes()),
        }
    }

    fn handle_umctl(&self, cmd: &Command) -> RespVec {
        let sub_cmd = match Self::get_sub_arg(&cmd, 1) {
            Ok(sub_cmd) => sub_cmd.to_uppercase(),
            Err(err) => {
                return Resp::Error(err.into_bytes());
            }
        };

        let second_sub_cmd = match Self::get_sub_arg(&cmd, 2) {
            Ok(sub_cmd) => sub_cmd.to_uppercase(),
            Err(err) => {
                return Resp::Error(err.into_bytes());
            }
        };

        if sub_cmd != "DEBUG" || second_sub_cmd != "FUTURE" {
            return Resp::Error(b"Invalid sub command for UMCTL".to_vec());
        }

        let mut fut_desc_arr = self.future_registry.get_all_futures();
        fut_desc_arr.sort_unstable_by_key(|desc| desc.get_start_time());

        let elements = fut_desc_arr
            .into_iter()
            .map(|desc| Resp::Bulk(BulkStr::Str(format!("{}", desc).into_bytes())))
            .collect();
        Resp::Arr(Array::Arr(elements))
    }

    fn get_sub_arg(cmd: &Command, index: usize) -> Result<String, String> {
        let sub_cmd = match cmd.get_command_element(index) {
            None => return Err("Missing sub argument".to_string()),
            Some(k) => match str::from_utf8(k) {
                Ok(sub_cmd) => sub_cmd.to_string(),
                Err(_) => return Err("Invalid sub argument".to_string()),
            },
        };
        Ok(sub_cmd)
    }
}

impl CmdHandler for CoordCmdHandler {
    fn handle_cmd(&self, cmd: Command) -> CmdReplyFuture {
        let (mut reply_sender, reply_receiver) = new_command_pair(&cmd);

        let cmd_type = cmd.get_type();
        let resp = match cmd_type {
            CmdType::Config => self.handle_config(&cmd),
            CmdType::UmCtl => self.handle_umctl(&cmd),
            _ => {
                let cmd_name = match cmd.get_command_name() {
                    Some(cmd_name) => cmd_name.to_string(),
                    None => format!("{:?}", cmd.get_command_element(0)),
                };
                Resp::Error(format!("{}: {}", response::CMD_NOT_SUPPORTED, cmd_name).into_bytes())
            }
        };

        let request = cmd.into_packet();
        let response = Box::new(RespPacket::Data(resp));
        let slowlog = Slowlog::new(0, false); // not used

        let res = reply_sender.send(Ok(Box::new(TaskReply::new(request, response, slowlog))));
        if let Err(err) = res {
            error!("Failed to set reply: {:?}", err);
        }
        CmdReplyFuture::Left(reply_receiver)
    }

    fn handle_slowlog(&self, _request: Box<RespPacket>, _slowlog: Slowlog) {}
}
