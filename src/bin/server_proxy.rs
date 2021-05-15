extern crate tokio;
extern crate undermoon;
#[macro_use]
extern crate log;
extern crate arc_swap;
extern crate config;
extern crate env_logger;

use arc_swap::ArcSwap;
use futures::channel::mpsc;
use std::cmp::min;
use std::env;
use std::error::Error;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::Arc;
use std::time::Duration;
use string_error::into_err;
use undermoon::common::batch::BatchStrategy;
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

fn gen_conf() -> Result<ServerProxyConfig, &'static str> {
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
        .ok_or("announce_address")?
        .to_string();

    let slowlog_len =
        NonZeroUsize::new(s.get::<usize>("slowlog_len").unwrap_or(1024)).ok_or("slowlog_len")?;
    let thread_number =
        NonZeroUsize::new(s.get::<usize>("thread_number").unwrap_or(2)).ok_or("thread_number")?;
    let backend_conn_num = NonZeroUsize::new(s.get::<usize>("backend_conn_num").unwrap_or(2))
        .ok_or("backend_conn_num")?;
    let backend_batch_strategy = s
        .get::<String>("backend_batch_strategy")
        .map(|strategy| match strategy.to_lowercase().as_str() {
            "disabled" => BatchStrategy::Disabled,
            "fixed" => BatchStrategy::Fixed,
            "dynamic" => BatchStrategy::Dynamic,
            _ => BatchStrategy::Fixed,
        })
        .unwrap_or_else(|_| BatchStrategy::Fixed);
    let backend_flush_size =
        NonZeroUsize::new(s.get::<usize>("backend_flush_size").unwrap_or(1024))
            .ok_or("backend_flush_size")?;
    let backend_low_flush_interval = NonZeroU64::new(
        s.get::<u64>("backend_low_flush_interval")
            .unwrap_or(200_000),
    )
    .ok_or("backend_low_flush_interval")?;
    let backend_high_flush_interval = NonZeroU64::new(
        s.get::<u64>("backend_high_flush_interval")
            .unwrap_or(600_000),
    )
    .ok_or("backend_high_flush_interval")?;
    let backend_timeout = NonZeroU64::new(s.get::<u64>("backend_timeout").unwrap_or(3_000))
        .ok_or("backend_timeout")?;

    let password = s.get::<String>("password").ok();

    let mut max_redirections = s.get::<usize>("max_redirections").unwrap_or(0);
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
        slowlog_len,
        slowlog_log_slower_than: AtomicI64::new(
            s.get::<i64>("slowlog_log_slower_than").unwrap_or(50000),
        ),
        slowlog_sample_rate: AtomicU64::new(s.get::<u64>("slowlog_sample_rate").unwrap_or(1000)),
        thread_number,
        backend_conn_num,
        active_redirection: s.get::<bool>("active_redirection").unwrap_or(false),
        max_redirections,
        default_redirection_address,
        backend_batch_strategy,
        backend_flush_size,
        backend_low_flush_interval: Duration::from_nanos(backend_low_flush_interval.get()),
        backend_high_flush_interval: Duration::from_nanos(backend_high_flush_interval.get()),
        backend_timeout: Duration::from_millis(backend_timeout.get()),
        password,
    };

    Ok(config)
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let config = gen_conf().map_err(|field| {
        let err_msg = format!("invalid field {}", field);
        into_err(err_msg)
    })?;

    info!("config: {:?}", config);
    let config = Arc::new(config);

    let timeout = Duration::new(1, 0);
    let client_factory = SimpleRedisClientFactory::new(timeout);

    let slow_request_logger = Arc::new(SlowRequestLogger::new(config.clone()));
    let meta_map = Arc::new(ArcSwap::new(Arc::new(MetaMap::empty())));
    let future_registry = Arc::new(TrackedFutureRegistry::default());

    let (service_stopped_sender, service_stopped_receiver) = mpsc::unbounded();

    let forward_handler = SharedForwardHandler::new(
        config.clone(),
        Arc::new(client_factory),
        slow_request_logger.clone(),
        meta_map,
        Arc::new(DefaultConnFactory::default()),
        future_registry.clone(),
        service_stopped_sender,
    );
    let server = ServerProxyService::new(
        config.clone(),
        forward_handler,
        slow_request_logger,
        future_registry,
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.thread_number.get())
        .enable_all()
        .build()?;

    if let Err(err) = runtime.block_on(server.run(service_stopped_receiver)) {
        error!("tokio runtime failed: {}", err);
        return Err(err);
    }
    Ok(())
}
