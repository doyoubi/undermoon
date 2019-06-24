extern crate futures;
extern crate reqwest;
extern crate tokio;
extern crate undermoon;
#[macro_use]
extern crate log;
extern crate config;
extern crate env_logger;

use futures::future::select_all;
use futures::Future;
use reqwest::r#async as request_async; // async is a keyword later
use std::env;
use std::sync::Arc;
use std::time::Duration;
use undermoon::coordinator::http_mani_broker::HttpMetaManipulationBroker;
use undermoon::coordinator::http_meta_broker::HttpMetaBroker;
use undermoon::coordinator::service::{CoordinatorConfig, CoordinatorService};
use undermoon::protocol::PooledRedisClientFactory;

fn gen_conf() -> Vec<CoordinatorConfig> {
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

    let mut broker_address_list = vec![];

    if let Ok(list) = s.get::<Vec<String>>("broker_address") {
        info!("load multiple broker addresses {:?}", list);
        broker_address_list = list;
    } else {
        broker_address_list.push(
            s.get::<String>("broker_address")
                .unwrap_or_else(|_| "127.0.0.1:7799".to_string()),
        )
    }

    let reporter_id = s
        .get::<String>("broker_address")
        .unwrap_or_else(|_| "127.0.0.1:6699".to_string());

    broker_address_list
        .into_iter()
        .map(|broker_address| CoordinatorConfig {
            broker_address,
            reporter_id: reporter_id.clone(),
        })
        .collect()
}

fn gen_service(
    config: CoordinatorConfig,
) -> CoordinatorService<HttpMetaBroker, HttpMetaManipulationBroker, PooledRedisClientFactory> {
    let http_client = request_async::ClientBuilder::new()
        .build()
        .expect("request_async_client");
    let data_broker = Arc::new(HttpMetaBroker::new(
        config.broker_address.clone(),
        http_client.clone(),
    ));
    let mani_broker = Arc::new(HttpMetaManipulationBroker::new(
        config.broker_address.clone(),
        http_client,
    ));

    let timeout = Duration::new(2, 0);
    let pool_size = 2;
    let client_factory = PooledRedisClientFactory::new(pool_size, timeout);

    CoordinatorService::new(config, data_broker, mani_broker, client_factory)
}

fn main() {
    env_logger::init();

    let configs = gen_conf();
    let services = configs.into_iter().map(gen_service);
    let futs = select_all(services.map(|service| {
        service.run().map_err(|e| {
            error!("coordinator error {:?}", e);
        })
    }));
    tokio::run(futs.map(|_| ()).map_err(|_| ()));
}
