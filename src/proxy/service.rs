use super::session::CmdCtxHandler;
use super::session::{handle_session, Session};
use super::slowlog::SlowRequestLogger;
use crate::common::config::ConfigError;
use crate::common::track::TrackedFutureRegistry;
use crate::common::utils::{resolve_first_address, ThreadSafe};
use futures::{FutureExt, StreamExt};
use std::error::Error;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use string_error::into_err;
use tokio::net::TcpListener;

#[derive(Debug)]
pub struct ServerProxyConfig {
    pub address: String,
    pub announce_address: String,
    pub auto_select_cluster: bool,
    pub slowlog_len: NonZeroUsize,
    pub slowlog_log_slower_than: AtomicI64,
    pub slowlog_sample_rate: AtomicU64,
    pub thread_number: NonZeroUsize,
    pub backend_channel_size: usize,
    pub backend_conn_num: NonZeroUsize,
    pub backend_batch_min_time: usize,
    pub backend_batch_max_time: usize,
    pub backend_batch_buf: NonZeroUsize,
    pub active_redirection: bool,
    pub max_redirections: Option<NonZeroUsize>,
    pub default_redirection_address: Option<String>,
}

impl ServerProxyConfig {
    pub fn get_slowlog_log_slower_than(&self) -> i64 {
        self.slowlog_log_slower_than.load(Ordering::Relaxed)
    }

    pub fn set_slowlog_log_slower_than(&self, n: i64) {
        self.slowlog_log_slower_than.store(n, Ordering::Relaxed)
    }

    pub fn get_slowlog_sample_rate(&self) -> u64 {
        self.slowlog_sample_rate.load(Ordering::Relaxed)
    }

    pub fn set_slowlog_sample_rate(&self, slowlog_sample_rate: u64) {
        self.slowlog_sample_rate
            .store(slowlog_sample_rate, Ordering::Relaxed)
    }
}

impl ServerProxyConfig {
    pub fn get_field(&self, field: &str) -> Result<String, ConfigError> {
        match field.to_lowercase().as_ref() {
            "address" => Ok(self.address.clone()),
            "announce_address" => Ok(self.announce_address.clone()),
            "auto_select_cluster" => Ok(self.auto_select_cluster.to_string()),
            "slowlog_len" => Ok(self.slowlog_len.to_string()),
            "thread_number" => Ok(self.thread_number.to_string()),
            "backend_channel_size" => Ok(self.backend_channel_size.to_string()),
            "backend_conn_num" => Ok(self.backend_conn_num.to_string()),
            "slowlog_log_slower_than" => Ok(self.get_slowlog_log_slower_than().to_string()),
            "slowlog_sample_rate" => Ok(self.get_slowlog_sample_rate().to_string()),
            "backend_batch_min_time" => Ok(self.backend_batch_min_time.to_string()),
            "backend_batch_max_time" => Ok(self.backend_batch_max_time.to_string()),
            "backend_batch_buf" => Ok(self.backend_batch_buf.to_string()),
            "active_redirection" => Ok(self.active_redirection.to_string()),
            "max_redirections" => Ok(self
                .max_redirections
                .map(|n| n.get().to_string())
                .unwrap_or_else(|| "none".to_string())),
            _ => Err(ConfigError::FieldNotFound),
        }
    }

    pub fn set_value(&self, field: &str, value: &str) -> Result<(), ConfigError> {
        match field.to_lowercase().as_ref() {
            "address" => Err(ConfigError::ReadonlyField),
            "announce_address" => Err(ConfigError::ReadonlyField),
            "auto_select_cluster" => Err(ConfigError::ReadonlyField),
            "slowlog_len" => Err(ConfigError::ReadonlyField),
            "thread_number" => Err(ConfigError::ReadonlyField),
            "session_channel_size" => Err(ConfigError::ReadonlyField),
            "backend_channel_size" => Err(ConfigError::ReadonlyField),
            "backend_conn_num" => Err(ConfigError::ReadonlyField),
            "slowlog_log_slower_than" => {
                let int_value = value
                    .parse::<i64>()
                    .map_err(|_| ConfigError::InvalidValue)?;
                self.set_slowlog_log_slower_than(int_value);
                Ok(())
            }
            "slowlog_sample_rate" => {
                let int_value = value
                    .parse::<u64>()
                    .map_err(|_| ConfigError::InvalidValue)?;
                self.set_slowlog_sample_rate(int_value);
                Ok(())
            }
            "backend_batch_max_time" => Err(ConfigError::ReadonlyField),
            "backend_batch_min_time" => Err(ConfigError::ReadonlyField),
            "backend_batch_buf" => Err(ConfigError::ReadonlyField),
            "session_batch_min_time" => Err(ConfigError::ReadonlyField),
            "session_batch_max_time" => Err(ConfigError::ReadonlyField),
            "session_batch_buf" => Err(ConfigError::ReadonlyField),
            "active_redirection" => Err(ConfigError::ReadonlyField),
            "max_redirections" => Err(ConfigError::ReadonlyField),
            _ => Err(ConfigError::FieldNotFound),
        }
    }
}

#[derive(Clone)]
pub struct ServerProxyService<H: CmdCtxHandler + ThreadSafe + Clone> {
    config: Arc<ServerProxyConfig>,
    cmd_ctx_handler: H,
    slow_request_logger: Arc<SlowRequestLogger>,
    future_registry: Arc<TrackedFutureRegistry>,
}

impl<H: CmdCtxHandler + ThreadSafe + Clone> ServerProxyService<H> {
    pub fn new(
        config: Arc<ServerProxyConfig>,
        cmd_ctx_handler: H,
        slow_request_logger: Arc<SlowRequestLogger>,
        future_registry: Arc<TrackedFutureRegistry>,
    ) -> Self {
        Self {
            config,
            cmd_ctx_handler,
            slow_request_logger,
            future_registry,
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn Error>> {
        let address = self.config.address.clone();
        let address = resolve_first_address(&address).await.ok_or_else(|| {
            let err_str = format!("failed to resolve address: {}", address);
            error!("{}", err_str);
            into_err(err_str)
        })?;

        let mut listener = TcpListener::bind(&address).await.map_err(|err| {
            error!("unable to bind address: {} {:?}", address, err);
            err
        })?;

        let forward_handler = self.cmd_ctx_handler.clone();
        let slow_request_logger = self.slow_request_logger.clone();

        let session_id = AtomicUsize::new(0);
        let config = self.config.clone();

        let future_registry = self.future_registry.clone();

        let mut s = listener.incoming();
        while let Some(sock) = s.next().await {
            let sock = sock?;

            if let Err(err) = sock.set_nodelay(true) {
                let err_str = format!("failed to set TCP_NODELAY: {:?}", err);
                error!("{}", err_str);
                return Err(into_err(err_str));
            }

            let peer = match sock.peer_addr() {
                Ok(address) => address.to_string(),
                Err(e) => format!("Failed to get peer {}", e),
            };
            info!("accept conn: {}", peer);

            let curr_session_id = session_id.fetch_add(1, Ordering::SeqCst);

            let handle_clone = forward_handler.clone();
            let session_handler = handle_session(
                Arc::new(Session::new(
                    curr_session_id,
                    handle_clone,
                    slow_request_logger.clone(),
                    config.clone(),
                )),
                sock,
            );

            let desc = format!("session: session_id={} peer={}", curr_session_id, peer);
            let fut = session_handler.map(move |res| match res {
                Ok(()) => info!("session IO closed {}", peer),
                Err(err) => error!("session IO error {:?} {}", err, peer),
            });
            let fut = TrackedFutureRegistry::wrap(future_registry.clone(), fut, desc);
            tokio::spawn(fut);
        }
        Ok(())
    }
}
