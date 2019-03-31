use super::session::CmdCtxHandler;
use super::session::{handle_conn, Session};
use common::future_group::new_future_group;
use common::utils::{ThreadSafe, revolve_first_address};
use futures::{future, Future, Stream};
use tokio::net::TcpListener;

#[derive(Debug, Clone)]
pub struct ServerProxyConfig {
    pub address: String,
}

#[derive(Clone)]
pub struct ServerProxyService<H: CmdCtxHandler + ThreadSafe + Clone> {
    config: ServerProxyConfig,
    cmd_ctx_handler: H,
}

impl<H: CmdCtxHandler + ThreadSafe + Clone> ServerProxyService<H> {
    pub fn new(config: ServerProxyConfig, cmd_ctx_handler: H) -> Self {
        Self {
            config,
            cmd_ctx_handler,
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

        Box::new(
            listener
                .incoming()
                .map_err(|e| error!("accept failed: {:?}", e))
                .for_each(move |sock| {
                    info!("accept conn {:?}", sock.peer_addr());
                    let handle_clone = forward_handler.clone();
                    let (reader_handler, writer_handler) =
                        handle_conn(Session::new(handle_clone), sock);
                    let (reader_handler, writer_handler) =
                        new_future_group(reader_handler, writer_handler);
                    tokio::spawn(
                        reader_handler
                            .map(|()| error!("Read IO closed"))
                            .map_err(|err| error!("Read IO error {:?}", err)),
                    );
                    tokio::spawn(
                        writer_handler
                            .map(|()| error!("Write IO closed"))
                            .map_err(|err| error!("Write IO error {:?}", err)),
                    );
                    future::ok(())
                }),
        )
    }
}
