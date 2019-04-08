use super::replicator::{MasterReplicator, ReplicatorError, ReplicaReplicator, MasterMeta, ReplicaMeta};
use chashmap::CHashMap;
use std::sync::Arc;
use std::collections::HashMap;

pub struct ReplicatorManager {
    master_replicators: CHashMap<String, Arc<MasterReplicator>>,
    replica_replicators: CHashMap<String, Arc<ReplicaReplicator>>,
}

impl ReplicatorManager {
    pub fn new() -> Self {
        Self{
            master_replicators: CHashMap::new(),
            replica_replicators: CHashMap::new(),
        }
    }

    pub fn update_masters(&self, meta_map: HashMap<String, MasterMeta>) {}
    pub fn update_replicas(&self, meta_map: HashMap<String, ReplicaMeta>) {}
}
