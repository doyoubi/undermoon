use super::task::MigrationError;
use ::common::cluster::{MigrationMeta, ReplPeer};
use ::replication::redis_replicator::{RedisMasterReplicator, RedisReplicaReplicator};
use ::replication::replicator::{MasterMeta, ReplicaMeta, ReplicaReplicator, ReplicatorError};
use futures::Future;
use protocol::RedisClientFactory;
use replication::replicator::MasterReplicator;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct RedisImportingController<F: RedisClientFactory> {
    master_replicator: RedisMasterReplicator<F>,
    replica_replicator: RedisReplicaReplicator<F>,
    switched_to_master: Arc<AtomicBool>,
}

impl<F: RedisClientFactory> RedisImportingController<F> {
    pub fn new(db_name: String, meta: MigrationMeta, client_factory: Arc<F>) -> Self {
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

        Self {
            master_replicator: RedisMasterReplicator::new(master_meta, client_factory.clone()),
            replica_replicator: RedisReplicaReplicator::new(replica_meta, client_factory),
            switched_to_master: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn start(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send> {
        let master_task = self.master_replicator.start();
        let switched_to_master = self.switched_to_master.clone();
        Box::new(
            self.replica_replicator
                .start()
                .then(move |result| {
                    warn!("replica_replicator result {:?}", result);
                    switched_to_master.store(true, Ordering::SeqCst);
                    master_task
                })
                .map_err(MigrationError::ReplError),
        )
    }

    pub fn switch_to_master(&self) -> Result<(), MigrationError> {
        match self.replica_replicator.stop() {
            Ok(()) | Err(ReplicatorError::AlreadyEnded) => (),
            Err(err) => error!("failed to stop replica replicator {:?}", err),
        }

        // this is like to require a second try.
        if self.switched_to_master.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(MigrationError::NotReady)
        }
    }
}
