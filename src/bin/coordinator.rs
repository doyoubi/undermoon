extern crate undermoon;
extern crate tokio;
extern crate futures;
extern crate reqwest;
extern crate env_logger;

use futures::Future;
use undermoon::coordinator::service::CoordinatorService;
use undermoon::coordinator::http_broker::HttpMetaBroker;
use undermoon::protocol::SimpleRedisClient;

fn main() {
    env_logger::init();

    let broker_address = "127.0.0.1:7799".to_string();
    let http_client = reqwest::async::ClientBuilder::new().build().unwrap();
    let broker = HttpMetaBroker::new(broker_address, http_client);
    let redis_client = SimpleRedisClient::new();
    let reporter_id = "reporter1".to_string();
    let service = CoordinatorService::new(reporter_id, broker, redis_client);
    tokio::run(service.run().map_err(|e| {
        println!("coordinator error {:?}", e);
    }));
}