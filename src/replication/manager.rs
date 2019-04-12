use super::redis_replicator::{RedisMasterReplicator, RedisReplicaReplicator};
use super::replicator::{MasterReplicator, ReplicaReplicator, ReplicatorMeta};
use futures::Future;
use itertools::Either;
use proxy::database::DBError;
use std::collections::HashMap;
use std::sync::{atomic, Arc, RwLock};
use tokio;

type ReplicatorRecord = Either<Arc<MasterReplicator>, Arc<ReplicaReplicator>>;
type ReplicatorMap = HashMap<(String, String), ReplicatorRecord>;

pub struct ReplicatorManager {
    updating_epoch: atomic::AtomicU64,
    replicators: RwLock<(u64, ReplicatorMap)>,
}

impl Default for ReplicatorManager {
    fn default() -> Self {
        Self {
            updating_epoch: atomic::AtomicU64::new(0),
            replicators: RwLock::new((0, HashMap::new())),
        }
    }
}

impl ReplicatorManager {
    pub fn update_replicators(&self, meta: ReplicatorMeta) -> Result<(), DBError> {
        let ReplicatorMeta {
            epoch,
            flags,
            masters,
            replicas,
        } = meta;

        let force = flags.force;
        if !force && self.updating_epoch.load(atomic::Ordering::SeqCst) >= epoch {
            return Err(DBError::OldEpoch);
        }

        // The computation below might take a long time.
        // Set epoch first to let later requests fail fast.
        // We can't update the epoch inside the lock here.
        // Because when we get the info inside it, it may be partially updated and inconsistent.
        self.updating_epoch.store(epoch, atomic::Ordering::SeqCst);
        // After this, other threads might accidentally change `updating_epoch` to a lower epoch,
        // we will correct his later.

        let mut master_key_set = HashMap::new();
        let mut replica_key_set = HashMap::new();
        for meta in masters.iter() {
            master_key_set.insert(
                (meta.db_name.clone(), meta.master_node_address.clone()),
                meta.clone(),
            );
        }
        for meta in replicas.iter() {
            replica_key_set.insert(
                (meta.db_name.clone(), meta.replica_node_address.clone()),
                meta.clone(),
            );
        }

        let mut new_replicators = HashMap::new();
        // Add existing replicators
        for (key, replicator) in self.replicators.read().unwrap().1.iter() {
            if Some(true)
                == master_key_set
                    .get(key)
                    .and_then(|meta| replicator.as_ref().left().map(|m| m.get_meta() == meta))
            {
                info!("reuse master replicator {} {}", key.0, key.1);
                new_replicators.insert(key.clone(), replicator.clone());
            }
            if Some(true)
                == replica_key_set
                    .get(key)
                    .and_then(|meta| replicator.as_ref().right().map(|m| m.get_meta() == meta))
            {
                info!("reuse replica replicator {} {}", key.0, key.1);
                new_replicators.insert(key.clone(), replicator.clone());
            }
        }

        let mut new_masters = HashMap::new();
        let mut new_replicas = HashMap::new();

        // Add new masters
        for meta in masters.into_iter() {
            let key = (meta.db_name.clone(), meta.master_node_address.clone());
            if new_replicators.contains_key(&key) {
                continue;
            }
            let replicator = Arc::new(RedisMasterReplicator::new(meta));
            new_masters.insert(key.clone(), replicator.clone());
            new_replicators.insert(key, Either::Left(replicator));
        }
        // Add new replicas
        for meta in replicas.into_iter() {
            let key = (meta.db_name.clone(), meta.replica_node_address.clone());
            if new_replicators.contains_key(&key) {
                continue;
            }
            let replicator = Arc::new(RedisReplicaReplicator::new(meta));
            new_replicas.insert(key.clone(), replicator.clone());
            new_replicators.insert(key, Either::Right(replicator));
        }

        {
            let mut replicators = self.replicators.write().unwrap();
            if !force && epoch <= replicators.0 {
                // We're fooled by the `updating_epoch`, update it.
                self.updating_epoch
                    .store(replicators.0, atomic::Ordering::SeqCst);
                return Err(DBError::OldEpoch);
            }
            for (key, master) in new_masters.into_iter() {
                debug!("spawn master {} {}", key.0, key.1);
                tokio::spawn(master.start().map_err(move |e| {
                    error!("master replicator {} {} exit {:?}", key.0, key.1, e)
                }));
            }
            for (key, replica) in new_replicas.into_iter() {
                debug!("spawn replica {} {}", key.0, key.1);
                tokio::spawn(replica.start().map_err(move |e| {
                    error!("replica replicator {} {} exit {:?}", key.0, key.1, e)
                }));
            }
            *replicators = (epoch, new_replicators);
        }
        Ok(())
    }
}
