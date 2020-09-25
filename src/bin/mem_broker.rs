extern crate actix_web;
extern crate undermoon;
#[macro_use]
extern crate log;
extern crate config;
extern crate env_logger;
use actix_web::{middleware, App, HttpServer};
use arc_swap::ArcSwap;
use futures_timer::Delay;
use std::env;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;
use undermoon::broker::{
    configure_app, JsonFileStorage, JsonMetaReplicator, MemBrokerConfig, MemBrokerService,
    MetaPersistence, MetaStoreError, MetaSyncError,
};

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn gen_conf() -> MemBrokerConfig {
    let mut s = config::Config::new();
    if let Some(conf_file_path) = env::args().nth(1) {
        s.merge(config::File::with_name(&conf_file_path))
            .map(|_| ())
            .unwrap_or_else(|e| warn!("failed to read config file: {:?}", e));
    }
    // e.g. UNDERMOON_ADDRESS='127.0.0.1:7799'
    s.merge(config::Environment::with_prefix("undermoon"))
        .map(|_| ())
        .unwrap_or_else(|e| warn!("failed to read config from env vars {:?}", e));

    let replica_addresses = s
        .get::<Vec<String>>("replica_addresses")
        .unwrap_or_else(|_| {
            s.get::<String>("replica_addresses")
                .unwrap_or_else(|_| String::new())
                .split_terminator(',')
                .map(|s| s.to_string())
                .collect()
        });
    let replica_addresses = Arc::new(ArcSwap::new(Arc::new(replica_addresses)));

    let debug = s.get::<bool>("debug").unwrap_or(false);

    MemBrokerConfig {
        address: s
            .get::<String>("address")
            .unwrap_or_else(|_| "127.0.0.1:7799".to_string()),
        failure_ttl: s.get::<u64>("failure_ttl").unwrap_or_else(|_| 60),
        failure_quorum: s.get::<u64>("failure_quorum").unwrap_or_else(|_| 1),
        migration_limit: s.get::<u64>("migration_limit").unwrap_or_else(|_| 1),
        recover_from_meta_file: s
            .get::<bool>("recover_from_meta_file")
            .unwrap_or_else(|_| false),
        meta_filename: s
            .get::<String>("meta_filename")
            .unwrap_or_else(|_| "metadata".to_string()),
        auto_update_meta_file: s
            .get::<bool>("auto_update_meta_file")
            .unwrap_or_else(|_| false),
        update_meta_file_interval: NonZeroU64::new(
            s.get::<u64>("update_meta_file_interval")
                .unwrap_or_else(|_| 0),
        ),
        replica_addresses,
        sync_meta_interval: NonZeroU64::new(
            s.get::<u64>("sync_meta_interval").unwrap_or_else(|_| 0),
        ),
        enable_ordered_proxy: s
            .get::<bool>("enable_ordered_proxy")
            .unwrap_or_else(|_| false),
        debug,
    }
}

fn meta_sync_error_to_io_err(err: MetaSyncError) -> std::io::Error {
    match err {
        MetaSyncError::Io(io_err) => io_err,
        other_err => {
            error!("meta data sync error: {}", other_err);
            std::io::Error::new(std::io::ErrorKind::Other, other_err)
        }
    }
}

fn meta_error_to_io_error(err: MetaStoreError) -> std::io::Error {
    match err {
        MetaStoreError::SyncError(sync_err) => meta_sync_error_to_io_err(sync_err),
        other_err => {
            error!("meta data error: {}", other_err);
            std::io::Error::new(std::io::ErrorKind::Other, other_err)
        }
    }
}

async fn update_meta_file(service: Arc<MemBrokerService>, interval: Duration) {
    loop {
        Delay::new(interval).await;
        trace!("periodically update meta file");
        if let Err(err) = service.update_meta_file().await {
            error!("failed to update meta file: {}", err);
        }
    }
}

async fn sync_meta_to_replicas(service: Arc<MemBrokerService>, interval: Duration) {
    loop {
        Delay::new(interval).await;
        trace!("periodically sync metadata to replicas");
        if let Err(err) = service.sync_meta().await {
            error!("failed to sync metadata to replicas: {}", err);
        }
    }
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    let config = gen_conf();
    let address = config.address.clone();
    let update_file_interval = config.update_meta_file_interval;
    let sync_meta_interval = config.sync_meta_interval;

    let meta_storage = Arc::new(JsonFileStorage::new(config.meta_filename.clone()));
    let meta_store = if config.recover_from_meta_file {
        meta_storage
            .load()
            .await
            .map_err(meta_sync_error_to_io_err)?
    } else {
        None
    };

    let http_client = reqwest::Client::new();
    let meta_replicator = JsonMetaReplicator::new(config.replica_addresses.clone(), http_client);
    let meta_replicator = Arc::new(meta_replicator);

    let service = MemBrokerService::new(config, meta_storage, meta_replicator, meta_store)
        .map_err(meta_error_to_io_error)?;
    let service = Arc::new(service);

    if let Some(interval) = update_file_interval {
        info!("start periodically updating meta file");
        let interval = Duration::from_secs(interval.get());
        actix_rt::spawn(update_meta_file(service.clone(), interval));
    }

    if let Some(interval) = sync_meta_interval {
        info!("start periodically sync meta to replicas");
        let interval = Duration::from_secs(interval.get());
        actix_rt::spawn(sync_meta_to_replicas(service.clone(), interval));
    }

    HttpServer::new(move || {
        let service = service.clone();
        App::new()
            .wrap(middleware::Logger::default())
            .configure(|cfg| configure_app(cfg, service.clone()))
    })
    .bind(&address)?
    .keep_alive(300)
    .shutdown_timeout(1)
    .run()
    .await
}
