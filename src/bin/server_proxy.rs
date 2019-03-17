extern crate undermoon;
extern crate tokio;
#[macro_use] extern crate log;
extern crate env_logger;

use undermoon::proxy::executor::SharedForwardHandler;
use undermoon::proxy::service::ServerProxyService;

fn main() {
    env_logger::init();

    let service_address = "127.0.0.1:5299".to_string();
    let forward_handler = SharedForwardHandler::new(service_address.to_string());
    let server = ServerProxyService::new(service_address, forward_handler);

    tokio::run(server.run());
}