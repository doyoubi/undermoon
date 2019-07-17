use super::session::CmdCtxHandler;
use super::session::{handle_conn, Session};
use super::slowlog::SlowRequestLogger;
use common::future_group::new_future_group;
use common::utils::{revolve_first_address, ThreadSafe};
use futures::{future, Future, Stream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;

#[derive(Debug, Clone)]
pub struct ServerProxyConfig {
    pub address: String,
    pub announce_address: String,
    pub auto_select_db: bool,
    pub slowlog_len: usize,
    pub slowlog_log_slower_than: i64,
    pub thread_number: usize,
    pub session_channel_size: usize,
    pub backend_channel_size: usize,
    pub backend_conn_num: usize,
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

        let listener = match TcpListener::bind(&address) {
            Ok(l) => l,
            Err(e) => {
                error!("unable to bind address: {} {:?}", address, e);
                return Box::new(future::err(()));
            }
        };

        let forward_handler = self.cmd_ctx_handler.clone();
        let slow_request_logger = self.slow_request_logger.clone();

        let session_id = AtomicUsize::new(0);
        let config = self.config.clone();

        Box::new(
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
                    );
                    let (reader_handler, writer_handler) =
                        new_future_group(reader_handler, writer_handler);

                    let (p1, p2, p3, p4) = (peer.clone(), peer.clone(), peer.clone(), peer.clone());
                    tokio::spawn(
                        reader_handler
                            .map(move |()| info!("Read IO closed {}", p1))
                            .map_err(move |err| error!("Read IO error {:?} {}", err, p2)),
                    );
                    tokio::spawn(
                        writer_handler
                            .map(move |()| info!("Write IO closed {}", p3))
                            .map_err(move |err| error!("Write IO error {:?} {}", err, p4)),
                    );
                    future::ok(())
                }),
        )
    }
}
