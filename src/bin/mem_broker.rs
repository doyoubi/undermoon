extern crate actix_web;
extern crate undermoon;
#[macro_use]
extern crate log;
extern crate config;
extern crate env_logger;
use actix_web::{middleware, App, HttpServer};
use std::env;
use std::sync::Arc;
use undermoon::broker::{
    configure_app, JsonFileStorage, MemBrokerConfig, MemBrokerService, MetaStorage, MetaStoreError,
    MetaSyncError,
};

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

    MemBrokerConfig {
        address: s
            .get::<String>("address")
            .unwrap_or_else(|_| "127.0.0.1:7799".to_string()),
        failure_ttl: s.get::<u64>("failure_ttl").unwrap_or_else(|_| 60),
        failure_quorum: s.get::<u64>("failure_quorum").unwrap_or_else(|_| 1),
        migration_limit: s.get::<u64>("migration_limit").unwrap_or_else(|_| 1),
        meta_filename: s
            .get::<String>("meta_filename")
            .unwrap_or_else(|_| "metadata".to_string()),
        auto_update_meta_file: s
            .get::<bool>("auto_update_meta_file")
            .unwrap_or_else(|_| false),
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

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    let config = gen_conf();
    let address = config.address.clone();

    let meta_storage = Arc::new(JsonFileStorage::new(config.meta_filename.clone()));
    let meta_store = meta_storage
        .load()
        .await
        .map_err(meta_sync_error_to_io_err)?;
    let service =
        MemBrokerService::new(config, meta_storage, meta_store).map_err(meta_error_to_io_error)?;

    let service = Arc::new(service);
    HttpServer::new(move || {
        let service = service.clone();
        App::new()
            .wrap(middleware::Logger::default())
            .configure(|cfg| configure_app(cfg, service.clone()))
    })
    .bind(&address)?
    .keep_alive(300)
    .run()
    .await
}
