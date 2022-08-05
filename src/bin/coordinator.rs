extern crate futures;
extern crate tokio;
extern crate undermoon;
#[macro_use]
extern crate log;
extern crate config;
extern crate env_logger;

use arc_swap::ArcSwap;
use std::cmp::max;
use std::env;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use undermoon::coordinator::http_mani_broker::HttpMetaManipulationBroker;
use undermoon::coordinator::http_meta_broker::HttpMetaBroker;
use undermoon::coordinator::service::{CoordinatorConfig, CoordinatorService};
use undermoon::protocol::PooledRedisClientFactory;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn gen_conf() -> Result<CoordinatorConfig, Box<dyn Error>> {
    let mut config_builder = config::Config::builder();
    // If config file is specified, load it.
    if let Some(conf_file_path) = env::args().nth(1) {
        config_builder = config_builder.add_source(config::File::with_name(&conf_file_path));
    }
    // e.g. UNDERMOON_ADDRESS_LIST='127.0.0.1:5299'
    let s = config_builder
        .add_source(config::Environment::with_prefix("undermoon"))
        .build()
        .map_err(|e| {
            warn!("failed to read config from file or env vars {:?}", e);
            "failed to read config"
        })?;

    let address = s
        .get::<String>("address")
        .unwrap_or_else(|_| "127.0.0.1:6699".to_string());

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
        .get::<String>("reporter_id")
        .unwrap_or_else(|_| address.clone());

    let thread_number = s.get::<usize>("thread_number").unwrap_or(4);
    let thread_number = max(1, thread_number);

    let proxy_timeout = s.get::<usize>("proxy_timeout").unwrap_or(2);

    let enable_compression = s.get::<bool>("enable_compression").unwrap_or(false);
    let disable_failover = s.get::<bool>("disable_failover").unwrap_or(false);

    let config = CoordinatorConfig {
        address,
        broker_addresses: Arc::new(ArcSwap::new(Arc::new(broker_address_list))),
        reporter_id,
        thread_number,
        proxy_timeout,
        enable_compression,
        disable_failover,
    };
    Ok(config)
}

fn gen_service(
    config: CoordinatorConfig,
) -> CoordinatorService<HttpMetaBroker, HttpMetaManipulationBroker, PooledRedisClientFactory> {
    let http_client = reqwest::Client::new();
    let data_broker = Arc::new(HttpMetaBroker::new(
        config.broker_addresses.clone(),
        http_client.clone(),
        config.enable_compression,
    ));
    let mani_broker = Arc::new(HttpMetaManipulationBroker::new(
        config.broker_addresses.clone(),
        http_client,
    ));

    let timeout = Duration::new(config.proxy_timeout as u64, 0);
    let pool_size = 2;
    let client_factory = PooledRedisClientFactory::new(pool_size, timeout);

    CoordinatorService::new(config, data_broker, mani_broker, client_factory)
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let config = gen_conf()?;
    let thread_number = config.thread_number;

    let service = gen_service(config);
    let fut = async move {
        if let Err(err) = service.run().await {
            error!("coordinator error {:?}", err);
        }
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(thread_number)
        .enable_all()
        .build()?;
    runtime.block_on(fut);
    Ok(())
}
