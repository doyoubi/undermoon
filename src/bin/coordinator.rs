extern crate futures;
extern crate tokio;
extern crate undermoon;
#[macro_use]
extern crate log;
extern crate config;
extern crate env_logger;

use futures::future::select_all;
use reqwest;
use std::env;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use undermoon::coordinator::http_mani_broker::HttpMetaManipulationBroker;
use undermoon::coordinator::http_meta_broker::HttpMetaBroker;
use undermoon::coordinator::service::{CoordinatorConfig, CoordinatorService};
use undermoon::protocol::PooledRedisClientFactory;

fn gen_conf() -> Vec<CoordinatorConfig> {
    let mut s = config::Config::new();
    // If config file is specified, load it.
    if let Some(conf_file_path) = env::args().nth(1) {
        s.merge(config::File::with_name(&conf_file_path))
            .map(|_| ())
            .unwrap_or_else(|e| warn!("failed to read config file: {:?}", e));
    }
    // e.g. UNDERMOON_ADDRESS_LIST='127.0.0.1:5299'
    s.merge(config::Environment::with_prefix("undermoon"))
        .map(|_| ())
        .unwrap_or_else(|e| warn!("failed to read config from env vars: {:?}", e));

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

    // Currently this address it not used. Later we should open a http
    // api to expose some inner states.
    let reporter_id = s
        .get::<String>("reporter_id")
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
    let http_client = reqwest::Client::new();
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

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let configs = gen_conf();
    let services = configs.into_iter().map(gen_service);
    let futs = select_all(services.map(|service| {
        Box::pin(async move {
            if let Err(err) = service.run().await {
                error!("coordinator error {:?}", err);
            }
        })
    }));

    let mut runtime = Runtime::new()?;
    runtime.block_on(futs);
    Ok(())
}
