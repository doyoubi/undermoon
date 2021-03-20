use super::replicator::{
    MasterMeta, MasterReplicator, ReplicaMeta, ReplicaReplicator, ReplicatorError, ReplicatorResult,
};
use crate::common::resp_execution::{retry_handle_func, I64Retriever};
use crate::common::utils::resolve_first_address;
use crate::protocol::{
    OptionalMulti, RedisClient, RedisClientError, RedisClientFactory, Resp, RespVec,
};
use futures::{future, Future};
use futures::{FutureExt, TryFutureExt};
use futures_timer::Delay;
use std::pin::Pin;
use std::str;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicI64};
use std::sync::Arc;
use std::time::Duration;

pub struct RedisMasterReplicator<F: RedisClientFactory> {
    meta: MasterMeta,
    role_sync: I64Retriever<F>,
}

impl<F: RedisClientFactory> RedisMasterReplicator<F> {
    pub fn new(meta: MasterMeta, client_factory: Arc<F>) -> Self {
        Self {
            meta,
            role_sync: I64Retriever::new(0, client_factory),
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

    fn handle_result(resp: RespVec, data: &Arc<AtomicI64>) -> Result<(), RedisClientError> {
        let r = retry_handle_func(OptionalMulti::Single(resp));
        if r.is_ok() {
            data.store(1, Ordering::SeqCst);
        }
        r
    }
}

impl<F: RedisClientFactory> MasterReplicator for RedisMasterReplicator<F> {
    fn start<'s>(&'s self) -> Pin<Box<dyn Future<Output = ReplicatorResult> + Send + 's>> {
        let meta = self.meta.clone();
        let address = meta.master_node_address.clone();
        let interval = Duration::new(5, 0);
        let cmd = vec!["SLAVEOF".to_string(), "NO".to_string(), "ONE".to_string()];
        self.role_sync
            .start(Self::handle_result, address, cmd, interval)
            .map(|f| {
                let fut: Pin<Box<dyn Future<Output = Result<(), ReplicatorError>> + Send + 's>> =
                    Box::pin(f.map_err(ReplicatorError::RedisError).then(move |r| {
                        warn!("RedisMasterReplicator {:?} stopped {:?}", meta, r);
                        future::ok(())
                    }));
                fut
            })
            .unwrap_or_else(|| {
                error!("FATAL ERROR: RedisMasterReplicator has already started");
                Box::pin(async { Err(ReplicatorError::AlreadyStarted) })
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
    client_factory: Arc<F>,
    started: AtomicBool,
    stopped: AtomicBool,
}

impl<F: RedisClientFactory> RedisReplicaReplicator<F> {
    pub fn new(meta: ReplicaMeta, client_factory: Arc<F>) -> Self {
        Self {
            meta,
            client_factory,
            started: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
        }
    }

    fn send_stop_signal(&self) -> Result<(), ReplicatorError> {
        if self
            .stopped
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            Err(ReplicatorError::AlreadyEnded)
        } else {
            Ok(())
        }
    }

    async fn start_impl(&self) -> ReplicatorResult {
        if self
            .started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            error!(
                "FATAL ERROR: RedisReplicaReplicator has already started {:?}",
                self.meta
            );
            return Err(ReplicatorError::AlreadyStarted);
        }

        let master_node_address = match self.meta.masters.get(0) {
            Some(repl_meta) => repl_meta.node_address.clone(),
            None => {
                error!("No master for replica {}", self.meta.replica_node_address);
                return Err(ReplicatorError::InvalidMeta);
            }
        };

        let interval = Duration::new(5, 0);
        let address = self.meta.replica_node_address.clone();
        let mut cached_client: Option<F::Client> = None;

        let mut first_sent = true;

        while !self.stopped.load(Ordering::Relaxed) {
            if !first_sent {
                Delay::new(interval).await;
            } else {
                first_sent = false;
            }

            let cmd = match resolve_first_address(master_node_address.as_str()).await {
                Some(address) => {
                    let host = address.ip().to_string();
                    let port = address.port().to_string();
                    vec![b"SLAVEOF".to_vec(), host.into_bytes(), port.into_bytes()]
                }
                None => {
                    error!(
                        "failed to resolve master node address in replica replicator: {}",
                        master_node_address
                    );
                    continue;
                }
            };

            let mut client = if let Some(client) = cached_client.take() {
                client
            } else {
                match self.client_factory.create_client(address.clone()).await {
                    Ok(client) => client,
                    Err(err) => {
                        error!("failed to create client in replica replicator: {:?}", err);
                        continue;
                    }
                }
            };

            let resp = match client.execute_single(cmd).await {
                Ok(resp) => resp,
                Err(err) => {
                    error!("failed to send SLAVEOF {}: {}", master_node_address, err);
                    continue;
                }
            };

            if let Resp::Error(err) = resp {
                let err_str = str::from_utf8(&err)
                    .map(ToString::to_string)
                    .unwrap_or_else(|_| format!("{:?}", err));
                error!(
                    "error reply for SLAVEOF {}: {}",
                    master_node_address, err_str
                );
                continue;
            }

            cached_client = Some(client);
        }

        warn!("RedisReplicaReplicator {:?} stopped", self.meta);
        Ok(())
    }
}

impl<F: RedisClientFactory> ReplicaReplicator for RedisReplicaReplicator<F> {
    fn start<'s>(&'s self) -> Pin<Box<dyn Future<Output = ReplicatorResult> + Send + 's>> {
        Box::pin(self.start_impl())
    }

    fn stop(&self) -> Result<(), ReplicatorError> {
        self.send_stop_signal()
    }

    fn get_meta(&self) -> &ReplicaMeta {
        &self.meta
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::cluster::{ClusterName, ReplPeer};
    use crate::protocol::{BinSafeStr, MockRedisClient, MockRedisClientFactory, OptionalMulti};
    use std::convert::TryFrom;

    fn create_master_client_func(cmd: Vec<BinSafeStr>, called: Arc<AtomicBool>) -> MockRedisClient {
        let mut mock_client = MockRedisClient::new();

        mock_client
            .expect_execute()
            .withf(move |command: &OptionalMulti<Vec<BinSafeStr>>| {
                called.store(true, Ordering::SeqCst);
                let expected = OptionalMulti::Single(cmd.clone());
                command.eq(&expected)
            })
            .times(1..)
            .returning(|_| {
                Box::pin(async { Ok(OptionalMulti::Single(Resp::Simple(b"ok".to_vec()))) })
            });

        mock_client
    }

    #[tokio::test]
    async fn test_master_replicator() {
        let called = Arc::new(AtomicBool::new(false));
        let called2 = called.clone();

        let mut client_factory = MockRedisClientFactory::new();
        client_factory
            .expect_create_client()
            .times(1..)
            .returning(move |_| {
                let called = called2.clone();
                Box::pin(async move {
                    let slaveof_cmd = vec![b"SLAVEOF".to_vec(), b"NO".to_vec(), b"ONE".to_vec()];
                    let client = create_master_client_func(slaveof_cmd, called);
                    Ok(client)
                })
            });

        let client_factory = Arc::new(client_factory);

        let meta = MasterMeta {
            cluster_name: ClusterName::try_from("test_clustername").unwrap(),
            master_node_address: "localhost:9999".to_string(),
            replicas: vec![ReplPeer {
                node_address: "127.0.0.1:6379".to_string(),
                proxy_address: "127.0.0.1:6379".to_string(),
            }],
        };
        let replicator = Arc::new(RedisMasterReplicator::new(meta, client_factory));
        let replicator_clone = replicator.clone();

        let stopped = Arc::new(AtomicBool::new(false));
        tokio::spawn(async move {
            let res = replicator_clone.start().await;
            stopped.store(true, Ordering::SeqCst);
            assert!(res.is_ok());
        });

        while !called.load(Ordering::SeqCst) {
            Delay::new(Duration::new(0, 100_000)).await;
        }

        replicator.stop().unwrap();
        let err = replicator.stop().unwrap_err();
        assert!(matches!(err, ReplicatorError::AlreadyEnded));
    }

    fn create_replica_client_func(
        cmd: Vec<BinSafeStr>,
        called: Arc<AtomicBool>,
    ) -> MockRedisClient {
        let mut mock_client = MockRedisClient::new();

        mock_client
            .expect_execute_single()
            .withf(move |command: &Vec<BinSafeStr>| {
                called.store(true, Ordering::SeqCst);
                command.eq(&cmd)
            })
            .times(1..)
            .returning(|_| Box::pin(async { Ok(Resp::Simple(b"ok".to_vec())) }));

        mock_client
    }

    #[tokio::test]
    async fn test_replica_replicator() {
        let called = Arc::new(AtomicBool::new(false));
        let called2 = called.clone();

        let mut client_factory = MockRedisClientFactory::new();
        client_factory
            .expect_create_client()
            .times(1..)
            .returning(move |_| {
                let called = called2.clone();
                Box::pin(async move {
                    let slaveof_cmd =
                        vec![b"SLAVEOF".to_vec(), b"127.0.0.1".to_vec(), b"6379".to_vec()];
                    let client = create_replica_client_func(slaveof_cmd, called);
                    Ok(client)
                })
            });

        let client_factory = Arc::new(client_factory);

        let meta = ReplicaMeta {
            cluster_name: ClusterName::try_from("test_clustername").unwrap(),
            replica_node_address: "localhost:9999".to_string(),
            masters: vec![ReplPeer {
                node_address: "127.0.0.1:6379".to_string(),
                proxy_address: "127.0.0.1:6379".to_string(),
            }],
        };
        let replicator = Arc::new(RedisReplicaReplicator::new(meta, client_factory));
        let replicator_clone = replicator.clone();

        let stopped = Arc::new(AtomicBool::new(false));
        tokio::spawn(async move {
            let res = replicator_clone.start().await;
            stopped.store(true, Ordering::SeqCst);
            assert!(res.is_ok());
        });

        while !replicator.started.load(Ordering::SeqCst) {
            Delay::new(Duration::new(0, 100_000)).await;
        }

        let err = replicator.start().await.unwrap_err();
        assert!(matches!(err, ReplicatorError::AlreadyStarted));

        while !called.load(Ordering::SeqCst) {
            Delay::new(Duration::new(0, 100_000)).await;
        }

        replicator.stop().unwrap();
        let err = replicator.stop().unwrap_err();
        assert!(matches!(err, ReplicatorError::AlreadyEnded));
    }
}
