use super::redis_replicator::{RedisMasterReplicator, RedisReplicaReplicator};
use super::replicator::{
    MasterMeta, MasterReplicator, ReplicaMeta, ReplicaReplicator,
};
use chashmap::CHashMap;
use futures::Future;
use std::collections::HashMap;
use std::sync::Arc;
use tokio;

pub struct ReplicatorManager {
    master_replicators: CHashMap<String, Arc<MasterReplicator>>,
    replica_replicators: CHashMap<String, Arc<ReplicaReplicator>>,
}

impl ReplicatorManager {
    pub fn new() -> Self {
        Self {
            master_replicators: CHashMap::new(),
            replica_replicators: CHashMap::new(),
        }
    }

    pub fn update_masters(&self, meta_map: HashMap<String, MasterMeta>) {
        for (master_address, meta) in meta_map.into_iter() {
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

    pub fn update_replicas(&self, meta_map: HashMap<String, ReplicaMeta>) {
        for (replica_address, meta) in meta_map.into_iter() {
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
