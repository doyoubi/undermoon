extern crate undermoon;
extern crate tokio;
extern crate futures;
extern crate reqwest;
extern crate env_logger;

use futures::Future;
use undermoon::coordinator::service::CoordinatorService;
use undermoon::coordinator::http_meta_broker::HttpMetaBroker;
use undermoon::coordinator::http_mani_broker::HttpMetaManipulationBroker;
use undermoon::protocol::SimpleRedisClient;

fn main() {
    env_logger::init();

    let broker_address = "127.0.0.1:7799".to_string();
    let reporter_id = "reporter1".to_string();

    let http_client = reqwest::async::ClientBuilder::new().build().unwrap();
    let data_broker = HttpMetaBroker::new(broker_address.clone(), http_client.clone());
    let mani_broker = HttpMetaManipulationBroker::new(broker_address, http_client);
    let redis_client = SimpleRedisClient::new();
    let service = CoordinatorService::new(reporter_id, data_broker, mani_broker, redis_client);
    tokio::run(service.run().map_err(|e| {
        println!("coordinator error {:?}", e);
    }));
}