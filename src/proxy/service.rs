use super::session::CmdCtxHandler;
use super::session::{handle_conn, Session};
use super::slowlog::SlowRequestLogger;
use crate::common::config::ConfigError;
use crate::common::future_group::new_future_group;
use crate::common::utils::{revolve_first_address, ThreadSafe};
use futures::compat::Future01CompatExt;
use futures::TryFutureExt;
use futures01::{future, Future};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;

#[derive(Debug)]
pub struct ServerProxyConfig {
    pub address: String,
    pub announce_address: String,
    pub auto_select_db: bool,
    pub slowlog_len: usize,
    pub slowlog_log_slower_than: AtomicI64,
    pub thread_number: usize,
    pub session_channel_size: usize,
    pub backend_channel_size: usize,
    pub backend_conn_num: usize,
    pub backend_batch_min_time: usize,
    pub backend_batch_max_time: usize,
    pub backend_batch_buf: usize,
    pub session_batch_min_time: usize,
    pub session_batch_max_time: usize,
    pub session_batch_buf: usize,
}

impl ServerProxyConfig {
    pub fn get_field(&self, field: &str) -> Result<String, ConfigError> {
        match field.to_lowercase().as_ref() {
            "address" => Ok(self.address.clone()),
            "announce_address" => Ok(self.announce_address.clone()),
            "auto_select_db" => Ok(self.auto_select_db.to_string()),
            "slowlog_len" => Ok(self.slowlog_len.to_string()),
            "thread_number" => Ok(self.thread_number.to_string()),
            "session_channel_size" => Ok(self.session_channel_size.to_string()),
            "backend_channel_size" => Ok(self.backend_channel_size.to_string()),
            "backend_conn_num" => Ok(self.backend_conn_num.to_string()),
            "slowlog_log_slower_than" => Ok(self
                .slowlog_log_slower_than
                .load(Ordering::SeqCst)
                .to_string()),
            "backend_batch_min_time" => Ok(self.backend_batch_min_time.to_string()),
            "backend_batch_max_time" => Ok(self.backend_batch_max_time.to_string()),
            "backend_batch_buf" => Ok(self.backend_batch_buf.to_string()),
            "session_batch_min_time" => Ok(self.session_batch_min_time.to_string()),
            "session_batch_max_time" => Ok(self.session_batch_max_time.to_string()),
            "session_batch_buf" => Ok(self.session_batch_buf.to_string()),
            _ => Err(ConfigError::FieldNotFound),
        }
    }

    pub fn set_value(&self, field: &str, value: &str) -> Result<(), ConfigError> {
        match field.to_lowercase().as_ref() {
            "address" => Err(ConfigError::ReadonlyField),
            "announce_address" => Err(ConfigError::ReadonlyField),
            "auto_select_db" => Err(ConfigError::ReadonlyField),
            "slowlog_len" => Err(ConfigError::ReadonlyField),
            "thread_number" => Err(ConfigError::ReadonlyField),
            "session_channel_size" => Err(ConfigError::ReadonlyField),
            "backend_channel_size" => Err(ConfigError::ReadonlyField),
            "backend_conn_num" => Err(ConfigError::ReadonlyField),
            "slowlog_log_slower_than" => {
                let int_value = value
                    .parse::<i64>()
                    .map_err(|_| ConfigError::InvalidValue)?;
                self.slowlog_log_slower_than
                    .store(int_value, Ordering::SeqCst);
                Ok(())
            }
            "backend_batch_max_time" => Err(ConfigError::ReadonlyField),
            "backend_batch_min_time" => Err(ConfigError::ReadonlyField),
            "backend_batch_buf" => Err(ConfigError::ReadonlyField),
            "session_batch_min_time" => Err(ConfigError::ReadonlyField),
            "session_batch_max_time" => Err(ConfigError::ReadonlyField),
            "session_batch_buf" => Err(ConfigError::ReadonlyField),
            _ => Err(ConfigError::FieldNotFound),
        }
    }
}

#[derive(Clone)]
pub struct ServerProxyService<H: CmdCtxHandler + ThreadSafe + Clone> {
    config: Arc<ServerProxyConfig>,
    cmd_ctx_handler: H,
    slow_request_logger: Arc<SlowRequestLogger>,
}

impl<H: CmdCtxHandler + ThreadSafe + Clone> ServerProxyService<H> {
    pub fn new(
        config: Arc<ServerProxyConfig>,
        cmd_ctx_handler: H,
        slow_request_logger: Arc<SlowRequestLogger>,
    ) -> Self {
        Self {
            config,
            cmd_ctx_handler,
            slow_request_logger,
        }
    }

    pub fn run(&self) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        info!("config: {:?}", self.config);

        let address = self.config.address.clone();

        let address = match revolve_first_address(&address) {
            Some(a) => a,
            None => {
                error!("failed to resolve address: {}", address);
                return Box::new(future::err(()));
            }
        };

        let listener_fut = TcpListener::bind(&address).compat();

        let forward_handler = self.cmd_ctx_handler.clone();
        let slow_request_logger = self.slow_request_logger.clone();

        let session_id = AtomicUsize::new(0);
        let config = self.config.clone();

        Box::new(
            listener_fut
                .map_err(|e| {
                    error!("unable to bind address: {} {:?}", address, e);
                    return Box::new(future::err(()));
                })
                .and_then(|listener| {
                    listener
                        .incoming()
                        .map_err(|e| error!("accept failed: {:?}", e))
                        .for_each(move |sock| {
                            if let Err(err) = sock.set_nodelay(true) {
                                error!("failed to set TCP_NODELAY: {:?}", err);
                                return future::err(());
                            }

                            let peer = match sock.peer_addr() {
                                Ok(address) => address.to_string(),
                                Err(e) => format!("Failed to get peer {}", e),
                            };

                            info!("accept conn: {}", peer);
                            let curr_session_id = session_id.fetch_add(1, Ordering::SeqCst);

                            let handle_clone = forward_handler.clone();
                            let (reader_handler, writer_handler) = handle_conn(
                                Arc::new(Session::new(
                                    curr_session_id,
                                    handle_clone,
                                    slow_request_logger.clone(),
                                )),
                                sock,
                                config.session_channel_size,
                                config.session_batch_min_time,
                                config.session_batch_max_time,
                                config.session_batch_buf,
                            );
                            let (reader_handler, writer_handler) =
                                new_future_group(reader_handler, writer_handler);

                            let (p1, p2, p3, p4) = (peer.clone(), peer.clone(), peer.clone(), peer);
                            tokio::spawn(
                                reader_handler
                                    .map(move |()| info!("Read IO closed {}", p1))
                                    .map_err(move |err| error!("Read IO error {:?} {}", err, p2))
                                    .compat(),
                            );
                            tokio::spawn(
                                writer_handler
                                    .map(move |()| info!("Write IO closed {}", p3))
                                    .map_err(move |err| error!("Write IO error {:?} {}", err, p4))
                                    .compat(),
                            );
                            future::ok(())
                        })
                }),
        )
    }
}
