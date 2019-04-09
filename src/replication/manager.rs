use super::redis_replicator::{RedisMasterReplicator, RedisReplicaReplicator};
use super::replicator::{MasterMeta, MasterReplicator, ReplicaMeta, ReplicaReplicator};
use chashmap::CHashMap;
use futures::Future;
use std::sync::Arc;
use tokio;

pub struct ReplicatorManager {
    master_replicators: CHashMap<String, Arc<MasterReplicator>>,
    replica_replicators: CHashMap<String, Arc<ReplicaReplicator>>,
}

impl Default for ReplicatorManager {
    fn default() -> Self {
        Self {
            master_replicators: CHashMap::new(),
            replica_replicators: CHashMap::new(),
        }
    }
}

impl ReplicatorManager {
    pub fn update_masters(&self, meta_array: Vec<MasterMeta>) {
        for meta in meta_array.into_iter() {
            let master_address = meta.master_node_address.clone();
            let replicator = match self.master_replicators.get(&master_address) {
                Some(ref r) if r.get_meta().eq(&meta) => continue,
                _ => Arc::new(RedisMasterReplicator::new(meta)),
            };
            tokio::spawn(
                replicator
                    .start()
                    .map_err(|e| error!("master replicator exit {:?}", e)),
            );
            self.master_replicators.insert(master_address, replicator);
        }
    }

    pub fn update_replicas(&self, meta_array: Vec<ReplicaMeta>) {
        for meta in meta_array.into_iter() {
            let replica_address = meta.replica_node_address.clone();
            let replicator = match self.replica_replicators.get(&replica_address) {
                Some(ref r) if r.get_meta().eq(&meta) => continue,
                _ => Arc::new(RedisReplicaReplicator::new(meta)),
            };
            tokio::spawn(
                replicator
                    .start()
                    .map_err(|e| error!("replica replicator exit {:?}", e)),
            );
            self.replica_replicators.insert(replica_address, replicator);
        }
    }
}
