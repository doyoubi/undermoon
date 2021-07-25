// Complex path configuration requires high recursion_limit.
#![recursion_limit = "256"]

extern crate undermoon;
#[macro_use]
extern crate log;
extern crate config;
extern crate env_logger;
use arc_swap::ArcSwap;
use std::env;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;
use undermoon::broker::{
    run_server, JsonFileStorage, JsonMetaReplicator, MemBrokerConfig, MemBrokerService,
    MetaPersistence, MetaStoreError, MetaSyncError, StorageConfig,
};
use undermoon::common::config::ClusterConfig;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn gen_conf() -> Result<(MemBrokerConfig, ClusterConfig), &'static str> {
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

    let storage = match s.get::<String>("storage_type") {
        Ok(t) if t.to_lowercase() == "http" => {
            let address = s
                .get::<String>("http_storage_address")
                .unwrap_or_else(|_| "localhost:9999".to_string());
            let refresh_interval = s.get::<u64>("refresh_interval").unwrap_or(30);
            let refresh_interval = Duration::from_secs(refresh_interval);
            let storage_name = s
                .get::<String>("storage_name")
                .unwrap_or_else(|_| "my_storage_name".to_string());
            let storage_password = s
                .get::<String>("storage_password")
                .unwrap_or_else(|_| "my_storage_password".to_string());
            StorageConfig::ExternalHttp {
                storage_name,
                storage_password,
                address,
                refresh_interval,
            }
        }
        Ok(t) if t.to_lowercase() == "memory" => StorageConfig::Memory,
        others => {
            error!(
                "unexpected storage_type: {:?}. Will fall back to memory.",
                others
            );
            StorageConfig::Memory
        }
    };

    let debug = s.get::<bool>("debug").unwrap_or(false);

    let config = MemBrokerConfig {
        address: s
            .get::<String>("address")
            .unwrap_or_else(|_| "127.0.0.1:7799".to_string()),
        failure_ttl: s.get::<u64>("failure_ttl").unwrap_or(60),
        failure_quorum: s.get::<u64>("failure_quorum").unwrap_or(1),
        migration_limit: s.get::<u64>("migration_limit").unwrap_or(1),
        recover_from_meta_file: s.get::<bool>("recover_from_meta_file").unwrap_or(false),
        meta_filename: s
            .get::<String>("meta_filename")
            .unwrap_or_else(|_| "metadata".to_string()),
        auto_update_meta_file: s.get::<bool>("auto_update_meta_file").unwrap_or(false),
        update_meta_file_interval: NonZeroU64::new(
            s.get::<u64>("update_meta_file_interval").unwrap_or(0),
        ),
        replica_addresses,
        sync_meta_interval: NonZeroU64::new(s.get::<u64>("sync_meta_interval").unwrap_or(0)),
        enable_ordered_proxy: s.get::<bool>("enable_ordered_proxy").unwrap_or(false),
        storage,
        debug,
    };

    let mut cluster_config = ClusterConfig::default();
    let cluster_fields = [
        "compression_strategy",
        "migration_max_migration_time",
        "migration_max_blocking_time",
        "migration_scan_interval",
        "migration_scan_count",
    ];
    for field in cluster_fields.iter() {
        if let Ok(value) = s.get::<String>(*field) {
            if cluster_config.set_field(*field, value.as_str()).is_err() {
                return Err(*field);
            }
        }
    }

    Ok((config, cluster_config))
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
        tokio::time::sleep(interval).await;
        trace!("periodically update meta file");
        if let Err(err) = service.update_meta_file().await {
            error!("failed to update meta file: {}", err);
        }
    }
}

async fn sync_meta_to_replicas(service: Arc<MemBrokerService>, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        trace!("periodically sync metadata to replicas");
        if let Err(err) = service.sync_meta().await {
            error!("failed to sync metadata to replicas: {}", err);
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let (config, cluster_config) = gen_conf().map_err(|err| {
        error!("config error: {}", err);
        std::io::Error::new(std::io::ErrorKind::Other, err)
    })?;
    let address = config.address.clone();
    let update_file_interval = config.update_meta_file_interval;
    let sync_meta_interval = config.sync_meta_interval;

    let meta_persistence = Arc::new(JsonFileStorage::new(config.meta_filename.clone()));
    let meta_store = if config.recover_from_meta_file {
        meta_persistence
            .load()
            .await
            .map_err(meta_sync_error_to_io_err)?
    } else {
        None
    };

    let http_client = reqwest::Client::new();
    let meta_replicator = JsonMetaReplicator::new(config.replica_addresses.clone(), http_client);
    let meta_replicator = Arc::new(meta_replicator);

    let service = MemBrokerService::new(
        config,
        cluster_config,
        meta_persistence,
        meta_replicator,
        meta_store,
    )
    .map_err(meta_error_to_io_error)?;
    let service = Arc::new(service);

    if let Some(interval) = update_file_interval {
        info!("start periodically updating meta file");
        let interval = Duration::from_secs(interval.get());
        tokio::spawn(update_meta_file(service.clone(), interval));
    }

    if let Some(interval) = sync_meta_interval {
        info!("start periodically sync meta to replicas");
        let interval = Duration::from_secs(interval.get());
        tokio::spawn(sync_meta_to_replicas(service.clone(), interval));
    }

    let address: std::net::SocketAddr = address.parse().map_err(|err| {
        let err: Box<dyn std::error::Error> = Box::new(err);
        err
    })?;

    run_server(service, address).await;
    Ok(())
}
