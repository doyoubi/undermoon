use super::redis_replicator::{RedisMasterReplicator, RedisReplicaReplicator};
use super::replicator::{
    MasterMeta, MasterReplicator, ReplicaMeta, ReplicaReplicator, ReplicatorMeta,
};
use crate::common::cluster::ClusterName;
use crate::common::future_group::{new_auto_drop_future, FutureAutoStopHandle};
use crate::common::track::TrackedFutureRegistry;
use crate::common::utils::extract_host_from_address;
use crate::protocol::{Array, BulkStr, RedisClientFactory, Resp, RespVec};
use crate::proxy::cluster::ClusterMetaError;
use itertools::Either;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::{atomic, Arc};

type ReplicatorRecord = Either<Arc<dyn MasterReplicator>, Arc<dyn ReplicaReplicator>>;
type ReplicatorMap = HashMap<(ClusterName, String), (ReplicatorRecord, Arc<FutureAutoStopHandle>)>;

pub struct ReplicatorManager<F: RedisClientFactory> {
    updating_epoch: atomic::AtomicU64,
    replicators: RwLock<(u64, ReplicatorMap)>,
    client_factory: Arc<F>,
    future_registry: Arc<TrackedFutureRegistry>,
}

impl<F: RedisClientFactory> ReplicatorManager<F> {
    pub fn new(client_factory: Arc<F>, future_registry: Arc<TrackedFutureRegistry>) -> Self {
        Self {
            updating_epoch: atomic::AtomicU64::new(0),
            replicators: RwLock::new((0, HashMap::new())),
            client_factory,
            future_registry,
        }
    }

    pub fn update_replicators(
        &self,
        meta: ReplicatorMeta,
        announce_host: String,
    ) -> Result<(), ClusterMetaError> {
        let ReplicatorMeta {
            epoch,
            flags,
            masters,
            replicas,
        } = meta;

        // validation
        for meta in masters.iter() {
            if Some(true)
                != extract_host_from_address(meta.master_node_address.as_str())
                    .map(|host| host == announce_host)
            {
                return Err(ClusterMetaError::NotMyMeta);
            }
        }
        for meta in replicas.iter() {
            if Some(true)
                != extract_host_from_address(meta.replica_node_address.as_str())
                    .map(|host| host == announce_host)
            {
                return Err(ClusterMetaError::NotMyMeta);
            }
        }

        let force = flags.force;
        if !force && self.updating_epoch.load(atomic::Ordering::SeqCst) >= epoch {
            return Err(ClusterMetaError::OldEpoch);
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
                (meta.cluster_name.clone(), meta.master_node_address.clone()),
                meta.clone(),
            );
        }
        for meta in replicas.iter() {
            replica_key_set.insert(
                (meta.cluster_name.clone(), meta.replica_node_address.clone()),
                meta.clone(),
            );
        }

        let mut new_replicators = HashMap::new();
        // Add existing replicators
        for (key, (replicator, handle)) in self.replicators.read().1.iter() {
            if Some(true)
                == master_key_set
                    .get(key)
                    .and_then(|meta| replicator.as_ref().left().map(|m| m.get_meta() == meta))
            {
                info!("reuse master replicator {} {}", key.0, key.1);
                new_replicators.insert(key.clone(), (replicator.clone(), handle.clone()));
            }
            if Some(true)
                == replica_key_set
                    .get(key)
                    .and_then(|meta| replicator.as_ref().right().map(|m| m.get_meta() == meta))
            {
                info!("reuse replica replicator {} {}", key.0, key.1);
                new_replicators.insert(key.clone(), (replicator.clone(), handle.clone()));
            }
        }

        let mut new_masters = HashMap::new();
        let mut new_replicas = HashMap::new();

        // Add new masters
        for meta in masters.into_iter() {
            let key = (meta.cluster_name.clone(), meta.master_node_address.clone());
            if new_replicators.contains_key(&key) {
                continue;
            }
            let replicator = Arc::new(RedisMasterReplicator::new(
                meta,
                self.client_factory.clone(),
            ));
            new_masters.insert(key.clone(), replicator.clone());
        }
        // Add new replicas
        for meta in replicas.into_iter() {
            let key = (meta.cluster_name.clone(), meta.replica_node_address.clone());
            if new_replicators.contains_key(&key) {
                continue;
            }
            let replicator = Arc::new(RedisReplicaReplicator::new(
                meta,
                self.client_factory.clone(),
            ));
            new_replicas.insert(key.clone(), replicator.clone());
        }

        {
            let mut replicators = self.replicators.write();
            if !force && epoch <= replicators.0 {
                // We're fooled by the `updating_epoch`, update it.
                self.updating_epoch
                    .store(replicators.0, atomic::Ordering::SeqCst);
                return Err(ClusterMetaError::OldEpoch);
            }

            for (key, master) in new_masters.into_iter() {
                debug!("spawn master {} {}", key.0, key.1);
                let desc = format!(
                    "replicator: role=master cluster_name={} master_node_address={}",
                    key.0, key.1
                );
                let master_clone = master.clone();
                let key_clone = key.clone();

                let fut = async move {
                    if let Err(err) = master_clone.start().await {
                        error!(
                            "master replicator {} {} exit {:?}",
                            key_clone.0, key_clone.1, err
                        );
                    }
                };
                let (fut, handle) = new_auto_drop_future(fut);
                new_replicators.insert(key, (Either::Left(master), Arc::new(handle)));

                let fut = TrackedFutureRegistry::wrap(self.future_registry.clone(), fut, desc);
                tokio::spawn(fut);
            }
            for (key, replica) in new_replicas.into_iter() {
                debug!("spawn replica {} {}", key.0, key.1);
                let desc = format!(
                    "replicator: role=replica cluster_name={} replica_node_address={}",
                    key.0, key.1
                );
                let replica_clone = replica.clone();
                let key_clone = key.clone();

                let fut = async move {
                    if let Err(err) = replica_clone.start().await {
                        error!(
                            "replica replicator {} {} exit {:?}",
                            key_clone.0, key_clone.1, err
                        );
                    }
                };
                let (fut, handle) = new_auto_drop_future(fut);
                new_replicators.insert(key, (Either::Right(replica), Arc::new(handle)));

                let fut = TrackedFutureRegistry::wrap(self.future_registry.clone(), fut, desc);
                tokio::spawn(fut);
            }
            *replicators = (epoch, new_replicators);
        }
        Ok(())
    }

    // Returns (master number, replica number)
    pub fn get_role_num(&self) -> (usize, usize) {
        let replicators = self.replicators.read();
        let master_num = replicators
            .1
            .values()
            .filter(|(replicator, _handle)| replicator.is_left())
            .count();
        let replica_num = replicators
            .1
            .values()
            .filter(|(replicator, _handle)| replicator.is_right())
            .count();
        (master_num, replica_num)
    }

    pub fn get_metadata(&self) -> (Vec<MasterMeta>, Vec<ReplicaMeta>) {
        let mut master_metadata = Vec::new();
        let mut replica_metadata = Vec::new();

        let replicators = self.replicators.read();
        for (_key, (replicator, _handle)) in replicators.1.iter() {
            match replicator {
                Either::Left(master) => {
                    let meta = master.get_meta().clone();
                    master_metadata.push(meta);
                }
                Either::Right(replica) => {
                    let meta = replica.get_meta().clone();
                    replica_metadata.push(meta);
                }
            }
        }

        (master_metadata, replica_metadata)
    }

    pub fn get_metadata_report(&self) -> RespVec {
        let (master_metadata, replica_metadata) = self.get_metadata();

        let mut reports = vec![];

        for meta in master_metadata.into_iter() {
            let MasterMeta {
                cluster_name,
                master_node_address,
                replicas,
            } = meta;
            let mut master_meta = vec![
                format!("cluster:{}\n", cluster_name),
                "role:master\n".to_string(),
                format!("node_address:{}\n", master_node_address),
            ];
            for replica in replicas.into_iter() {
                master_meta.push(format!(
                    "replica:{}@{}\n",
                    replica.node_address, replica.proxy_address
                ));
            }

            let master_meta = Resp::Arr(Array::Arr(
                master_meta
                    .into_iter()
                    .map(|s| Resp::Bulk(BulkStr::Str(s.into_bytes())))
                    .collect(),
            ));
            reports.push(master_meta);
        }

        for meta in replica_metadata.into_iter() {
            let ReplicaMeta {
                cluster_name,
                replica_node_address,
                masters,
            } = meta;
            let mut replica_meta = vec![
                format!("cluster:{}\n", cluster_name),
                "role:replica\n".to_string(),
                format!("node_address:{}\n", replica_node_address),
            ];
            for master in masters.into_iter() {
                replica_meta.push(format!(
                    "master:{}@{}\n",
                    master.node_address, master.proxy_address
                ));
            }
            let replica_meta = Resp::Arr(Array::Arr(
                replica_meta
                    .into_iter()
                    .map(|s| Resp::Bulk(BulkStr::Str(s.into_bytes())))
                    .collect(),
            ));
            reports.push(replica_meta);
        }

        Resp::Arr(Array::Arr(reports))
    }
}
