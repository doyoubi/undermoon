use super::task::MigrationError;
use ::common::cluster::{MigrationMeta, ReplPeer};
use ::common::resp_execution::I64Retriever;
use ::common::utils::extract_info_int_field;
use ::protocol::{RedisClientError, RedisClientFactory, Resp};
use ::replication::redis_replicator::{RedisMasterReplicator, RedisReplicaReplicator};
use ::replication::replicator::{MasterMeta, ReplicaMeta, ReplicaReplicator, ReplicatorError};
use futures::{future, Future};
use replication::replicator::MasterReplicator;
use std::sync::atomic;
use std::sync::Arc;
use std::time::Duration;

pub struct RedisImportingController<F: RedisClientFactory> {
    master_replicator: RedisMasterReplicator<F>,
    replica_replicator: RedisReplicaReplicator<F>,
    loading_retriever: I64Retriever<F>,
}

impl<F: RedisClientFactory> RedisImportingController<F> {
    pub fn new(db_name: String, meta: MigrationMeta, client_factory: Arc<F>) -> Self {
        let address = meta.dst_node_address.clone();

        let master_meta = MasterMeta {
            db_name: db_name.clone(),
            master_node_address: meta.dst_node_address.clone(),
            replicas: vec![],
        };
        let replica_meta = ReplicaMeta {
            db_name,
            replica_node_address: meta.dst_node_address.clone(),
            masters: vec![ReplPeer {
                node_address: meta.src_node_address.clone(),
                proxy_address: meta.src_proxy_address.clone(),
            }],
        };

        let loading_check_interval = Duration::from_millis(5);
        let loading_check_cmd = vec!["INFO".to_string(), "PERSISTENCE".to_string()];

        Self {
            master_replicator: RedisMasterReplicator::new(master_meta, client_factory.clone()),
            replica_replicator: RedisReplicaReplicator::new(replica_meta, client_factory.clone()),
            loading_retriever: I64Retriever::new(
                1,
                client_factory,
                address,
                loading_check_cmd,
                loading_check_interval,
            ),
        }
    }

    pub fn start(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send> {
        if let Some(master_task) = self.master_replicator.start() {
            Box::new(
                self.replica_replicator
                    .start()
                    .then(move |result| {
                        warn!("replica_replicator result {:?}", result);
                        master_task
                    })
                    .map_err(MigrationError::ReplError),
            )
        } else {
            Box::new(future::err(MigrationError::AlreadyStarted))
        }
    }

    pub fn switch_to_master(&self) -> Result<(), MigrationError> {
        match self.replica_replicator.stop() {
            Ok(()) | Err(ReplicatorError::AlreadyEnded) => (),
            Err(err) => error!("failed to stop replica replicator {:?}", err),
        }

        // this is like to require a second try.
        if self.master_replicator.already_master() {
            Ok(())
        } else {
            Err(MigrationError::NotReady)
        }
    }

    pub fn wait_for_loading(&self) -> Result<(), MigrationError> {
        if let Some(fut) = self.loading_retriever.start(Self::handle_info_persistence) {
            tokio::spawn(fut.map_err(|e| {
                match e {
                    RedisClientError::Done | RedisClientError::Canceled => {
                        info!("importing controller stopped")
                    }
                    err => error!("importing controller stopped with error: {:?}", err),
                };
            }));
        }

        if self.loading_retriever.get_data() == 0 {
            Ok(())
        } else {
            Err(MigrationError::NotReady)
        }
    }

    fn handle_info_persistence(
        resp: Resp,
        data: &Arc<atomic::AtomicI64>,
    ) -> Result<(), RedisClientError> {
        let loading = match extract_info_int_field(&resp, "loading") {
            Ok(value) => value,
            Err(err_str) => {
                error!("failed to parse INFO PERSISTENCE: {}", err_str);
                return Ok(());
            }
        };
        data.store(loading, atomic::Ordering::SeqCst);
        Ok(())
    }
}
