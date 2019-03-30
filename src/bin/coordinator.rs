extern crate futures;
extern crate reqwest;
extern crate tokio;
extern crate undermoon;
#[macro_use]
extern crate log;
extern crate config;
extern crate env_logger;

use futures::Future;
use reqwest::r#async as request_async; // async is a keyword later
use std::env;
use undermoon::coordinator::http_mani_broker::HttpMetaManipulationBroker;
use undermoon::coordinator::http_meta_broker::HttpMetaBroker;
use undermoon::coordinator::service::{CoordinatorConfig, CoordinatorService};
use undermoon::protocol::SimpleRedisClient;

fn gen_conf() -> CoordinatorConfig {
    let conf_file_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "coordinator.toml".to_string());

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

    CoordinatorConfig {
        broker_address: s
            .get::<String>("broker_address")
            .unwrap_or_else(|_| "127.0.0.1:7799".to_string()),
        reporter_id: s
            .get::<String>("broker_address")
            .unwrap_or_else(|_| "127.0.0.1:6699".to_string()),
    }
}

fn main() {
    env_logger::init();

    let config = gen_conf();

    let http_client = request_async::ClientBuilder::new().build().unwrap();
    let data_broker = HttpMetaBroker::new(config.broker_address.clone(), http_client.clone());
    let mani_broker = HttpMetaManipulationBroker::new(config.broker_address.clone(), http_client);
    let redis_client = SimpleRedisClient::new();
    let service = CoordinatorService::new(config, data_broker, mani_broker, redis_client);
    tokio::run(service.run().map_err(|e| {
        error!("coordinator error {:?}", e);
    }));
}
