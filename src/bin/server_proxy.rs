extern crate tokio;
extern crate undermoon;
#[macro_use]
extern crate log;
extern crate arc_swap;
extern crate config;
extern crate env_logger;

use arc_swap::ArcSwap;
use std::cmp::min;
use std::env;
use std::error::Error;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::Arc;
use std::time::Duration;
use string_error::into_err;
use undermoon::common::config::ClusterConfig;
use undermoon::common::track::TrackedFutureRegistry;
use undermoon::common::utils::extract_host_from_address;
use undermoon::protocol::SimpleRedisClientFactory;
use undermoon::proxy::backend::DefaultConnFactory;
use undermoon::proxy::executor::SharedForwardHandler;
use undermoon::proxy::manager::MetaMap;
use undermoon::proxy::service::{ServerProxyConfig, ServerProxyService};
use undermoon::proxy::slowlog::SlowRequestLogger;
use undermoon::MAX_REDIRECTIONS;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn gen_conf() -> Result<(ServerProxyConfig, ClusterConfig), &'static str> {
    let mut s = config::Config::new();
    // If config file is specified, load it.
    if let Some(conf_file_path) = env::args().nth(1) {
        s.merge(config::File::with_name(&conf_file_path))
            .map(|_| ())
            .unwrap_or_else(|e| warn!("failed to read config file: {:?}", e));
    }
    // e.g. UNDERMOON_ADDRESS='127.0.0.1:5299'
    s.merge(config::Environment::with_prefix("undermoon"))
        .map(|_| ())
        .unwrap_or_else(|e| warn!("failed to read config from env vars: {:?}", e));

    let address = s
        .get::<String>("address")
        .unwrap_or_else(|_| "127.0.0.1:5299".to_string());
    let announce_address = s
        .get::<String>("announce_address")
        .unwrap_or_else(|_| address.clone());
    let announce_host = extract_host_from_address(announce_address.as_str())
        .ok_or_else(|| "announce_address")?
        .to_string();

    let slowlog_len = NonZeroUsize::new(s.get::<usize>("slowlog_len").unwrap_or_else(|_| 1024))
        .ok_or_else(|| "slowlog_len")?;
    let thread_number = NonZeroUsize::new(s.get::<usize>("thread_number").unwrap_or_else(|_| 2))
        .ok_or_else(|| "thread_number")?;
    let backend_conn_num =
        NonZeroUsize::new(s.get::<usize>("backend_conn_num").unwrap_or_else(|_| 2))
            .ok_or_else(|| "backend_conn_num")?;
    let backend_batch_buf =
        NonZeroUsize::new(s.get::<usize>("backend_batch_buf").unwrap_or_else(|_| 10))
            .ok_or_else(|| "backend_batch_buf")?;
    let session_batch_buf =
        NonZeroUsize::new(s.get::<usize>("session_batch_buf").unwrap_or_else(|_| 10))
            .ok_or_else(|| "session_batch_buf")?;

    let mut max_redirections = s.get::<usize>("max_redirections").unwrap_or_else(|_| 0);
    if max_redirections != 0 {
        max_redirections = min(MAX_REDIRECTIONS, max_redirections);
    }
    let max_redirections = NonZeroUsize::new(max_redirections);

    let default_redirection_address = s
        .get::<String>("default_redirection_address")
        .unwrap_or_else(|_| "".to_string());
    let default_redirection_address = if default_redirection_address.is_empty() {
        None
    } else {
        Some(default_redirection_address)
    };

    let config = ServerProxyConfig {
        address,
        announce_address,
        announce_host,
        auto_select_cluster: s
            .get::<bool>("auto_select_cluster")
            .unwrap_or_else(|_| true),
        slowlog_len,
        slowlog_log_slower_than: AtomicI64::new(
            s.get::<i64>("slowlog_log_slower_than")
                .unwrap_or_else(|_| 50000),
        ),
        slowlog_sample_rate: AtomicU64::new(
            s.get::<u64>("slowlog_sample_rate").unwrap_or_else(|_| 1000),
        ),
        thread_number,
        session_channel_size: s
            .get::<usize>("session_channel_size")
            .unwrap_or_else(|_| 4096),
        backend_channel_size: s
            .get::<usize>("backend_channel_size")
            .unwrap_or_else(|_| 4096),
        backend_conn_num,
        backend_batch_min_time: s
            .get::<usize>("backend_batch_min_time")
            .unwrap_or_else(|_| 20000),
        backend_batch_max_time: s
            .get::<usize>("backend_batch_max_time")
            .unwrap_or_else(|_| 400_000),
        backend_batch_buf,
        session_batch_min_time: s
            .get::<usize>("session_batch_min_time")
            .unwrap_or_else(|_| 20000),
        session_batch_max_time: s
            .get::<usize>("session_batch_max_time")
            .unwrap_or_else(|_| 400_000),
        session_batch_buf,
        active_redirection: s
            .get::<bool>("active_redirection")
            .unwrap_or_else(|_| false),
        max_redirections,
        default_redirection_address,
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

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let (config, cluster_config) = gen_conf().map_err(|field| {
        let err_msg = format!("invalid field {}", field);
        into_err(err_msg)
    })?;

    info!("config: {:?}", config);
    info!("cluster default config: {:?}", cluster_config);

    let config = Arc::new(config);

    let timeout = Duration::new(1, 0);
    let client_factory = SimpleRedisClientFactory::new(timeout);

    let slow_request_logger = Arc::new(SlowRequestLogger::new(config.clone()));
    let meta_map = Arc::new(ArcSwap::new(Arc::new(MetaMap::empty())));
    let future_registry = Arc::new(TrackedFutureRegistry::default());

    let forward_handler = SharedForwardHandler::new(
        config.clone(),
        cluster_config,
        Arc::new(client_factory),
        slow_request_logger.clone(),
        meta_map,
        Arc::new(DefaultConnFactory::default()),
        future_registry.clone(),
    );
    let server = ServerProxyService::new(
        config.clone(),
        forward_handler,
        slow_request_logger,
        future_registry,
    );

    let mut runtime = tokio::runtime::Builder::new()
        .threaded_scheduler()
        .core_threads(config.thread_number.get())
        .enable_all()
        .build()?;

    if let Err(err) = runtime.block_on(server.run()) {
        error!("tokio runtime failed: {}", err);
        return Err(err);
    }
    Ok(())
}
