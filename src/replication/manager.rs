use super::redis_replicator::{RedisMasterReplicator, RedisReplicaReplicator};
use super::replicator::{MasterMeta, MasterReplicator, ReplicaMeta, ReplicaReplicator};
use chashmap::CHashMap;
use futures::Future;
use itertools::Either;
use tokio;

type ReplicatorEnum = Either<Box<MasterReplicator>, Box<ReplicaReplicator>>;

pub struct ReplicatorManager {
    replicators: CHashMap<String, ReplicatorEnum>,
}

impl Default for ReplicatorManager {
    fn default() -> Self {
        Self {
            replicators: CHashMap::new(),
        }
    }
}

impl ReplicatorManager {
    pub fn update_masters(&self, meta_array: Vec<MasterMeta>) {
        for meta in meta_array.into_iter() {
            let master_address = meta.master_node_address.clone();
            let replicator = match self.replicators.get(&master_address) {
                Some(ref r) if r.as_ref().either(|m| m.get_meta().eq(&meta), |_| false) => continue,
                _ => Box::new(RedisMasterReplicator::new(meta)),
            };
            tokio::spawn(
                replicator
                    .start()
                    .map_err(|e| error!("master replicator exit {:?}", e)),
            );
            self.replicators
                .insert(master_address, Either::Left(replicator));
        }
    }

    pub fn update_replicas(&self, meta_array: Vec<ReplicaMeta>) {
        for meta in meta_array.into_iter() {
            let replica_address = meta.replica_node_address.clone();
            let replicator = match self.replicators.get(&replica_address) {
                Some(ref r) if r.as_ref().either(|_| false, |r| r.get_meta().eq(&meta)) => continue,
                _ => Box::new(RedisReplicaReplicator::new(meta)),
            };
            tokio::spawn(
                replicator
                    .start()
                    .map_err(|e| error!("replica replicator exit {:?}", e)),
            );
            self.replicators
                .insert(replica_address, Either::Right(replicator));
        }
    }
}
