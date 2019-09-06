use super::replicator::{
    MasterMeta, MasterReplicator, ReplicaMeta, ReplicaReplicator, ReplicatorError,
};
use common::resp_execution::{retry_handle_func, I64Retriever};
use common::utils::{revolve_first_address, ThreadSafe};
use futures::{future, Future};
use protocol::{RedisClientError, RedisClientFactory, Resp};
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

pub struct RedisMasterReplicator<F: RedisClientFactory> {
    meta: MasterMeta,
    role_sync: I64Retriever<F>,
}

impl<F: RedisClientFactory> ThreadSafe for RedisMasterReplicator<F> {}

impl<F: RedisClientFactory> RedisMasterReplicator<F> {
    pub fn new(meta: MasterMeta, client_factory: Arc<F>) -> Self {
        let address = meta.master_node_address.clone();
        let interval = Duration::new(5, 0);
        let cmd = vec!["SLAVEOF".to_string(), "NO".to_string(), "ONE".to_string()];

        Self {
            meta,
            role_sync: I64Retriever::new(0, client_factory, address, cmd, interval),
        }
    }

    fn send_stop_signal(&self) -> Result<(), ReplicatorError> {
        if self.role_sync.stop() {
            Ok(())
        } else {
            Err(ReplicatorError::AlreadyEnded)
        }
    }

    pub fn already_master(&self) -> bool {
        self.role_sync.get_data() != 0
    }

    fn handle_result(resp: Resp, data: &Arc<AtomicI64>) -> Result<(), RedisClientError> {
        let r = retry_handle_func(resp);
        if r.is_ok() {
            data.store(1, Ordering::SeqCst);
        }
        r
    }
}

impl<F: RedisClientFactory> MasterReplicator for RedisMasterReplicator<F> {
    fn start(&self) -> Option<Box<dyn Future<Item = (), Error = ReplicatorError> + Send>> {
        let meta = self.meta.clone();
        self.role_sync.start(Self::handle_result).map(|f| {
            let fut: Box<dyn Future<Item = (), Error = ReplicatorError> + Send> =
                Box::new(f.map_err(ReplicatorError::RedisError).then(move |r| {
                    warn!("RedisMasterReplicator {:?} stopped {:?}", meta, r);
                    future::ok(())
                }));
            fut
        })
    }

    fn stop(&self) -> Result<(), ReplicatorError> {
        self.send_stop_signal()
    }

    fn get_meta(&self) -> &MasterMeta {
        &self.meta
    }
}

pub struct RedisReplicaReplicator<F: RedisClientFactory> {
    meta: ReplicaMeta,
    role_sync: I64Retriever<F>,
}

impl<F: RedisClientFactory> ThreadSafe for RedisReplicaReplicator<F> {}

impl<F: RedisClientFactory> RedisReplicaReplicator<F> {
    pub fn new(meta: ReplicaMeta, client_factory: Arc<F>) -> Self {
        // Just get the first one.
        let cmd = match Self::gen_cmd(&meta) {
            Ok(cmd) => cmd,
            Err(err) => {
                error!("FATAL ERROR: invalid meta {:?}, keep going with just PING. Migration will get stuck.", err);
                vec!["PING".to_string()]
            }
        };
        let address = meta.replica_node_address.clone();
        let interval = Duration::new(5, 0);

        Self {
            meta,
            role_sync: I64Retriever::new(0, client_factory, address, cmd, interval),
        }
    }

    fn gen_cmd(meta: &ReplicaMeta) -> Result<Vec<String>, ReplicatorError> {
        let master_node_address = match meta.masters.get(0) {
            Some(repl_meta) => &repl_meta.node_address,
            None => {
                error!("No master for replica {}", meta.replica_node_address);
                return Err(ReplicatorError::InvalidMeta);
            }
        };

        match revolve_first_address(master_node_address) {
            Some(address) => {
                let host = address.ip().to_string();
                let port = address.port().to_string();
                Ok(vec!["SLAVEOF".to_string(), host, port])
            }
            None => Err(ReplicatorError::InvalidAddress),
        }
    }

    fn send_stop_signal(&self) -> Result<(), ReplicatorError> {
        if self.role_sync.stop() {
            Ok(())
        } else {
            Err(ReplicatorError::AlreadyEnded)
        }
    }

    fn handle_result(resp: Resp, _data: &Arc<AtomicI64>) -> Result<(), RedisClientError> {
        retry_handle_func(resp)
    }
}

impl<F: RedisClientFactory> ReplicaReplicator for RedisReplicaReplicator<F> {
    fn start(&self) -> Option<Box<dyn Future<Item = (), Error = ReplicatorError> + Send>> {
        let meta = self.meta.clone();
        self.role_sync.start(Self::handle_result).map(|f| {
            let fut: Box<dyn Future<Item = (), Error = ReplicatorError> + Send> =
                Box::new(f.map_err(ReplicatorError::RedisError).then(move |r| {
                    warn!("RedisReplicaReplicator {:?} stopped {:?}", meta, r);
                    future::ok(())
                }));
            fut
        })
    }

    fn stop(&self) -> Result<(), ReplicatorError> {
        self.send_stop_signal()
    }

    fn get_meta(&self) -> &ReplicaMeta {
        &self.meta
    }
}
