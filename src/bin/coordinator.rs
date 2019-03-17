extern crate undermoon;
extern crate tokio;
extern crate futures;
extern crate reqwest;
#[macro_use] extern crate log;
extern crate env_logger;
extern crate config;

use std::env;
use futures::Future;
use undermoon::coordinator::service::{CoordinatorService, CoordinatorConfig};
use undermoon::coordinator::http_meta_broker::HttpMetaBroker;
use undermoon::coordinator::http_mani_broker::HttpMetaManipulationBroker;
use undermoon::protocol::SimpleRedisClient;

fn gen_conf() -> CoordinatorConfig {
    let conf_file_path = env::args().skip(1).next().unwrap_or("coordinator.toml".to_string());

    let mut s = config::Config::new();
    s.merge(config::File::with_name(&conf_file_path))
        .map(|_| ())
        .unwrap_or_else(|e| warn!("failed to read config file: {:?}", e));
    // e.g. UNDERMOON_BROKER_ADDRESS='127.0.0.1:7799'
    s.merge(config::Environment::with_prefix("undermoon"))
        .map(|_| ())
        .unwrap_or_else(|e| warn!("failed to read broker_address from env vars {:?}", e));
    // e.g. UNDERMOON_REPORTER_ID='127.0.0.1:6699'
    s.merge(config::Environment::with_prefix("undermoon"))
        .map(|_| ())
        .unwrap_or_else(|e| warn!("failed to read reporter_id from env vars {:?}", e));

    CoordinatorConfig{
        broker_address: s.get::<String>("broker_address").unwrap_or("127.0.0.1:7799".to_string()),
        reporter_id: s.get::<String>("broker_address").unwrap_or("127.0.0.1:6699".to_string()),
    }
}

fn main() {
    env_logger::init();

    let config = gen_conf();

    let http_client = reqwest::async::ClientBuilder::new().build().unwrap();
    let data_broker = HttpMetaBroker::new(config.broker_address.clone(), http_client.clone());
    let mani_broker = HttpMetaManipulationBroker::new(config.broker_address.clone(), http_client);
    let redis_client = SimpleRedisClient::new();
    let service = CoordinatorService::new(config, data_broker, mani_broker, redis_client);
    tokio::run(service.run().map_err(|e| {
        println!("coordinator error {:?}", e);
    }));
}