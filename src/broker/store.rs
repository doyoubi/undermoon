use super::migrate::MetaStoreMigrate;
use super::persistence::MetaSyncError;
use super::query::MetaStoreQuery;
use super::update::MetaStoreUpdate;
use crate::common::cluster::ClusterName;
use crate::common::cluster::{
    Cluster, MigrationMeta, MigrationTaskMeta, Node, Proxy, Range, RangeList, SlotRange,
    SlotRangeTag,
};
use crate::common::config::ClusterConfig;
use crate::common::version::UNDERMOON_MEM_BROKER_META_VERSION;
use serde::ser::{Serialize, SerializeStruct, Serializer};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;

pub const NODES_PER_PROXY: usize = 2;
pub const CHUNK_PARTS: usize = 2;
pub const CHUNK_HALF_NODE_NUM: usize = 2;
pub const CHUNK_NODE_NUM: usize = 4;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProxyResource {
    pub proxy_address: String,
    pub node_addresses: [String; NODES_PER_PROXY],
    pub host: String,
    pub cluster: Option<ClusterName>,
}

pub struct HostProxy {
    pub host: String,
    pub proxy_address: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
pub enum ChunkRolePosition {
    Normal,
    FirstChunkMaster,
    SecondChunkMaster,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct MigrationSlotRangeStore {
    pub range_list: RangeList,
    pub is_migrating: bool, // migrating or importing
    pub meta: MigrationMetaStore,
}

impl MigrationSlotRangeStore {
    pub fn to_slot_range(&self, chunks: &[ChunkStore]) -> SlotRange {
        let src_chunk = chunks.get(self.meta.src_chunk_index).expect("get_cluster");
        let src_proxy_index =
            Self::chunk_part_to_proxy_index(self.meta.src_chunk_part, src_chunk.role_position);
        let src_proxy_address = src_chunk
            .proxy_addresses
            .get(src_proxy_index)
            .expect("get_cluster")
            .clone();
        let src_node_index =
            Self::chunk_part_to_node_index(self.meta.src_chunk_part, src_chunk.role_position);
        let src_node_address = src_chunk
            .node_addresses
            .get(src_node_index)
            .expect("get_cluster")
            .clone();

        let dst_chunk = chunks.get(self.meta.dst_chunk_index).expect("get_cluster");
        let dst_proxy_index =
            Self::chunk_part_to_proxy_index(self.meta.dst_chunk_part, dst_chunk.role_position);
        let dst_proxy_address = dst_chunk
            .proxy_addresses
            .get(dst_proxy_index)
            .expect("get_cluster")
            .clone();
        let dst_node_index =
            Self::chunk_part_to_node_index(self.meta.dst_chunk_part, dst_chunk.role_position);
        let dst_node_address = dst_chunk
            .node_addresses
            .get(dst_node_index)
            .expect("get_cluster")
            .clone();

        let meta = MigrationMeta {
            epoch: self.meta.epoch,
            src_proxy_address,
            src_node_address,
            dst_proxy_address,
            dst_node_address,
        };
        if self.is_migrating {
            SlotRange {
                range_list: self.range_list.clone(),
                tag: SlotRangeTag::Migrating(meta),
            }
        } else {
            SlotRange {
                range_list: self.range_list.clone(),
                tag: SlotRangeTag::Importing(meta),
            }
        }
    }

    fn chunk_part_to_proxy_index(chunk_part: usize, role_position: ChunkRolePosition) -> usize {
        match (chunk_part, role_position) {
            (0, ChunkRolePosition::SecondChunkMaster) => 1,
            (1, ChunkRolePosition::FirstChunkMaster) => 0,
            (i, _) => i,
        }
    }

    fn chunk_part_to_node_index(chunk_part: usize, role_position: ChunkRolePosition) -> usize {
        match (chunk_part, role_position) {
            (0, ChunkRolePosition::SecondChunkMaster) => 3,
            (1, ChunkRolePosition::FirstChunkMaster) => 1,
            (i, _) => 2 * i,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct MigrationMetaStore {
    pub epoch: u64,
    pub src_chunk_index: usize,
    pub src_chunk_part: usize,
    pub dst_chunk_index: usize,
    pub dst_chunk_part: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChunkStore {
    pub role_position: ChunkRolePosition,
    pub stable_slots: [Option<SlotRange>; CHUNK_PARTS],
    pub migrating_slots: [Vec<MigrationSlotRangeStore>; CHUNK_PARTS],
    pub proxy_addresses: [String; CHUNK_PARTS],
    pub hosts: [String; CHUNK_PARTS],
    pub node_addresses: [String; CHUNK_NODE_NUM],
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClusterStore {
    pub epoch: u64,
    pub name: ClusterName,
    pub chunks: Vec<ChunkStore>,
    pub config: ClusterConfig,
}

impl ClusterStore {
    pub fn set_epoch(&mut self, new_epoch: u64) {
        self.epoch = new_epoch;
    }

    // LimitMigration reduces the concurrent running migration.
    // This implementation is a bit tricky. The stored data do not allow some of shards
    // are migrating while others not. They are all set with the migration metadata.
    // We only reduce the migration in the query API.
    // (1) Only the former shards finish the migration, will the later ones get started.
    // (2) And once started, since the former one will not restart migration again,
    // the later ones will not stop until they are done.
    // (3) The later migration flags will be updated to server proxies with new epoch
    // bumped by the committing of former ones.
    pub fn limit_migration(&self, migration_limit: u64) -> ClusterStore {
        if migration_limit == 0 {
            return self.clone();
        }

        let mut chunks = vec![];
        for chunk in self.chunks.iter() {
            let new_chunk = ChunkStore {
                role_position: chunk.role_position,
                stable_slots: chunk.stable_slots.clone(),
                migrating_slots: [vec![], vec![]],
                proxy_addresses: chunk.proxy_addresses.clone(),
                hosts: chunk.hosts.clone(),
                node_addresses: chunk.node_addresses.clone(),
            };
            chunks.push(new_chunk);
        }
        let mut migration_num = 0;

        const MAX_MIGRATING_OUT: usize = 1;
        // When migrating out, the server proxy will have very high CPU usage.
        let mut migrating_out: HashMap<(usize, usize), usize> = HashMap::new();

        for chunk in self.chunks.iter() {
            for migrating_slots in chunk.migrating_slots.iter() {
                for slot_range_store in migrating_slots.iter() {
                    // The importing part will also be set by the migrating part.
                    if !slot_range_store.is_migrating {
                        continue;
                    }

                    let mut range_list = slot_range_store.range_list.clone();
                    let meta = &slot_range_store.meta;
                    let migrating_out_count = migrating_out
                        .entry((meta.src_chunk_index, meta.src_chunk_part))
                        .or_insert(0);

                    if migration_num >= migration_limit || *migrating_out_count >= MAX_MIGRATING_OUT
                    {
                        let stable_slots = chunks
                            .get_mut(meta.src_chunk_index)
                            .and_then(|chunk| chunk.stable_slots.get_mut(meta.src_chunk_part))
                            .expect("limit_migration")
                            .get_or_insert_with(|| SlotRange {
                                range_list: RangeList::new(vec![]),
                                tag: SlotRangeTag::None,
                            });
                        stable_slots
                            .get_mut_range_list()
                            .merge_another(&mut range_list);
                    } else {
                        chunks
                            .get_mut(meta.src_chunk_index)
                            .and_then(|chunk| chunk.migrating_slots.get_mut(meta.src_chunk_part))
                            .expect("limit_migration")
                            .push(slot_range_store.clone());

                        let mut importing_slot_range_store = slot_range_store.clone();
                        importing_slot_range_store.is_migrating = false;
                        chunks
                            .get_mut(meta.dst_chunk_index)
                            .and_then(|chunk| chunk.migrating_slots.get_mut(meta.dst_chunk_part))
                            .expect("limit_migration")
                            .push(importing_slot_range_store);

                        migration_num += 1;
                        *migrating_out_count += 1;
                    }
                }
            }
        }

        ClusterStore {
            epoch: self.epoch,
            name: self.name.clone(),
            chunks,
            config: self.config.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MigrationSlots {
    pub ranges: Vec<Range>,
    pub meta: MigrationMetaStore,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct MetaStore {
    pub version: String,
    pub global_epoch: u64,
    pub clusters: HashMap<ClusterName, ClusterStore>,
    // proxy_address => nodes and cluster_name
    pub all_proxies: HashMap<String, ProxyResource>,
    // proxy addresses
    pub failed_proxies: HashSet<String>,
    // failed_proxy_address => reporter_id => time,
    pub failures: HashMap<String, HashMap<String, i64>>,
}

impl Default for MetaStore {
    fn default() -> Self {
        Self {
            version: UNDERMOON_MEM_BROKER_META_VERSION.to_string(),
            global_epoch: 0,
            clusters: HashMap::new(),
            all_proxies: HashMap::new(),
            failed_proxies: HashSet::new(),
            failures: HashMap::new(),
        }
    }
}

impl MetaStore {
    pub fn restore(&mut self, other: MetaStore) -> Result<(), MetaStoreError> {
        if self.version != other.version {
            return Err(MetaStoreError::InvalidMetaVersion);
        }
        *self = other;
        Ok(())
    }

    pub fn get_global_epoch(&self) -> u64 {
        self.global_epoch
    }

    pub fn bump_global_epoch(&mut self) -> u64 {
        self.global_epoch += 1;
        self.global_epoch
    }

    pub fn get_proxies(&self) -> Vec<String> {
        MetaStoreQuery::new(self).get_proxies()
    }

    pub fn get_proxies_with_pagination(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Vec<String> {
        MetaStoreQuery::new(self).get_proxies_with_pagination(offset, limit)
    }

    pub fn get_proxy_by_address(&self, address: &str, migration_limit: u64) -> Option<Proxy> {
        MetaStoreQuery::new(self).get_proxy_by_address(address, migration_limit)
    }

    pub fn get_cluster_names(&self) -> Vec<ClusterName> {
        MetaStoreQuery::new(self).get_cluster_names()
    }

    pub fn get_cluster_names_with_pagination(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Vec<ClusterName> {
        MetaStoreQuery::new(self).get_cluster_names_with_pagination(offset, limit)
    }

    pub fn get_cluster_by_name(&self, cluster_name: &str, migration_limit: u64) -> Option<Cluster> {
        MetaStoreQuery::new(self).get_cluster_by_name(cluster_name, migration_limit)
    }

    pub fn add_failure(&mut self, address: String, reporter_id: String) {
        MetaStoreUpdate::new(self).add_failure(address, reporter_id)
    }

    pub fn get_failures(
        &mut self,
        falure_ttl: chrono::Duration,
        failure_quorum: u64,
    ) -> Vec<String> {
        MetaStoreUpdate::new(self).get_failures(falure_ttl, failure_quorum)
    }

    pub fn add_proxy(
        &mut self,
        proxy_address: String,
        nodes: [String; NODES_PER_PROXY],
        host: Option<String>,
    ) -> Result<(), MetaStoreError> {
        MetaStoreUpdate::new(self).add_proxy(proxy_address, nodes, host)
    }

    pub fn add_cluster(
        &mut self,
        cluster_name: String,
        node_num: usize,
    ) -> Result<(), MetaStoreError> {
        MetaStoreUpdate::new(self).add_cluster(cluster_name, node_num)
    }

    pub fn remove_cluster(&mut self, cluster_name: String) -> Result<(), MetaStoreError> {
        MetaStoreUpdate::new(self).remove_cluster(cluster_name)
    }

    pub fn auto_add_nodes(
        &mut self,
        cluster_name: String,
        num: usize,
    ) -> Result<Vec<Node>, MetaStoreError> {
        MetaStoreUpdate::new(self).auto_add_nodes(cluster_name, num)
    }

    pub fn audo_delete_free_nodes(&mut self, cluster_name: String) -> Result<(), MetaStoreError> {
        MetaStoreUpdate::new(self).audo_delete_free_nodes(cluster_name)
    }

    pub fn remove_proxy(&mut self, proxy_address: String) -> Result<(), MetaStoreError> {
        MetaStoreUpdate::new(self).remove_proxy(proxy_address)
    }

    pub fn migrate_slots(&mut self, cluster_name: String) -> Result<(), MetaStoreError> {
        MetaStoreMigrate::new(self).migrate_slots(cluster_name)
    }

    pub fn migrate_slots_to_scale_down(
        &mut self,
        cluster_name: String,
        new_node_num: usize,
    ) -> Result<(), MetaStoreError> {
        MetaStoreMigrate::new(self).migrate_slots_to_scale_down(cluster_name, new_node_num)
    }

    pub fn commit_migration(&mut self, task: MigrationTaskMeta) -> Result<(), MetaStoreError> {
        MetaStoreMigrate::new(self).commit_migration(task)
    }

    pub fn get_free_proxies(&self) -> Vec<HostProxy> {
        MetaStoreQuery::new(&self).get_free_proxies()
    }

    pub fn replace_failed_proxy(
        &mut self,
        failed_proxy_address: String,
        migration_limit: u64,
    ) -> Result<Option<Proxy>, MetaStoreError> {
        MetaStoreUpdate::new(self).replace_failed_proxy(failed_proxy_address, migration_limit)
    }

    pub fn change_config(
        &mut self,
        cluster_name: String,
        config: HashMap<String, String>,
    ) -> Result<(), MetaStoreError> {
        MetaStoreUpdate::new(self).change_config(cluster_name, config)
    }

    pub fn balance_masters(&mut self, cluster_name: String) -> Result<(), MetaStoreError> {
        MetaStoreUpdate::new(self).balance_masters(cluster_name)
    }

    pub fn get_failed_proxies(&self) -> Vec<String> {
        self.failed_proxies.iter().cloned().collect()
    }

    pub fn force_bump_all_epoch(&mut self, new_epoch: u64) -> Result<(), MetaStoreError> {
        if new_epoch <= self.global_epoch {
            return Err(MetaStoreError::SmallEpoch);
        }
        self.global_epoch = new_epoch;

        for cluster in self.clusters.values_mut() {
            cluster.epoch = new_epoch;
        }
        Ok(())
    }

    pub fn recover_epoch(&mut self, exsting_largest_epoch: u64) {
        let new_epoch = max(exsting_largest_epoch, self.global_epoch + 1);
        self.global_epoch = new_epoch;

        for cluster in self.clusters.values_mut() {
            cluster.epoch = new_epoch;
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum MetaStoreError {
    InUse,
    NotInUse,
    NoAvailableResource,
    ResourceNotBalance,
    AlreadyExisted,
    ClusterNotFound,
    FreeNodeNotFound,
    FreeNodeFound,
    ProxyNotFound,
    InvalidNodeNum,
    InvalidClusterName,
    InvalidMigrationTask,
    InvalidProxyAddress,
    MigrationTaskNotFound,
    MigrationRunning,
    InvalidConfig {
        key: String,
        value: String,
        error: String,
    },
    SlotsAlreadyEven,
    SyncError(MetaSyncError),
    InvalidMetaVersion,
    SmallEpoch,
}

impl MetaStoreError {
    pub fn to_code(&self) -> &str {
        match self {
            Self::InUse => "IN_USE",
            Self::NotInUse => "NOT_IN_USE",
            Self::NoAvailableResource => "NO_AVAILABLE_RESOURCE",
            Self::ResourceNotBalance => "RESOURCE_NOT_BALANCE",
            Self::AlreadyExisted => "ALREADY_EXISTED",
            Self::ClusterNotFound => "CLUSTER_NOT_FOUND",
            Self::FreeNodeNotFound => "FREE_NODE_NOT_FOUND",
            Self::FreeNodeFound => "FREE_NODE_NOT_FOUND",
            Self::ProxyNotFound => "PROXY_NOT_FOUND",
            Self::InvalidNodeNum => "INVALID_NODE_NUMBER",
            Self::InvalidClusterName => "INVALID_CLUSTER_NAME",
            Self::InvalidMigrationTask => "INVALID_MIGRATION_TASK",
            Self::InvalidProxyAddress => "INVALID_PROXY_ADDRESS",
            Self::MigrationTaskNotFound => "MIGRATION_TASK_NOT_FOUND",
            Self::MigrationRunning => "MIGRATION_RUNNING",
            Self::InvalidConfig { .. } => "INVALID_CONFIG",
            Self::SlotsAlreadyEven => "SLOTS_ALREADY_EVEN",
            Self::SyncError(err) => err.to_code(),
            Self::InvalidMetaVersion => "INVALID_META_VERSION",
            Self::SmallEpoch => "EPOCH_SMALLER_THAN_CURRENT",
        }
    }
}

impl fmt::Display for MetaStoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_code())
    }
}

impl Error for MetaStoreError {
    fn cause(&self) -> Option<&dyn Error> {
        None
    }
}

impl From<MetaSyncError> for MetaStoreError {
    fn from(sync_err: MetaSyncError) -> Self {
        MetaStoreError::SyncError(sync_err)
    }
}

impl Serialize for MetaStoreError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let error_code = self.to_string();
        match self {
            Self::InvalidConfig { key, value, error } => {
                let mut state = serializer.serialize_struct("MetaStoreError", 4)?;
                state.serialize_field("error", &error_code)?;
                state.serialize_field("key", &key)?;
                state.serialize_field("value", &value)?;
                state.serialize_field("message", &error)?;
                state.end()
            }
            _ => {
                let mut state = serializer.serialize_struct("MetaStoreError", 1)?;
                state.serialize_field("error", &error_code)?;
                state.end()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::cluster::Role;
    use crate::common::config::CompressionStrategy;
    use crate::common::utils::SLOT_NUM;
    use std::convert::TryFrom;

    fn add_testing_proxies(store: &mut MetaStore, host_num: usize, proxy_per_host: usize) {
        for host_index in 1..=host_num {
            for i in 1..=proxy_per_host {
                let proxy_address = format!("127.0.0.{}:70{:02}", host_index, i);
                let node_addresses = [
                    format!("127.0.0.{}:60{:02}", host_index, i * 2),
                    format!("127.0.0.{}:60{:02}", host_index, i * 2 + 1),
                ];
                store
                    .add_proxy(proxy_address, node_addresses, None)
                    .unwrap();
            }
        }
    }

    #[test]
    fn test_add_and_remove_proxy() {
        let migration_limit = 0;

        let mut store = MetaStore::default();
        let proxy_address = "127.0.0.1:7000";
        let nodes = ["127.0.0.1:6000".to_string(), "127.0.0.1:6001".to_string()];

        assert!(store
            .add_proxy("127.0.0.1".to_string(), nodes.clone(), None)
            .is_err());

        store
            .add_proxy(proxy_address.to_string(), nodes.clone(), None)
            .unwrap();
        assert_eq!(store.get_global_epoch(), 1);
        assert_eq!(store.all_proxies.len(), 1);
        let resource = store.all_proxies.get(proxy_address).unwrap();
        assert_eq!(resource.proxy_address, proxy_address);
        assert_eq!(resource.node_addresses, nodes);

        assert_eq!(store.get_proxies(), vec![proxy_address.to_string()]);

        let proxy = store
            .get_proxy_by_address(proxy_address, migration_limit)
            .unwrap();
        assert_eq!(proxy.get_address(), proxy_address);
        assert_eq!(proxy.get_epoch(), 1);
        assert_eq!(proxy.get_nodes().len(), 0);
        assert_eq!(proxy.get_peers().len(), 0);
        assert_eq!(proxy.get_free_nodes().len(), 2);

        store.remove_proxy(proxy_address.to_string()).unwrap();
    }

    #[test]
    fn test_specifying_host_when_adding_proxy() {
        let proxy_address = "127.0.0.1:7000";
        let nodes = ["127.0.0.1:6000".to_string(), "127.0.0.1:6001".to_string()];

        {
            let mut store = MetaStore::default();
            store
                .add_proxy(proxy_address.to_string(), nodes.clone(), None)
                .unwrap();
            let proxies = store.get_free_proxies();
            let proxy = proxies.get(0).unwrap();
            assert_eq!(proxy.proxy_address, proxy_address);
            assert_eq!(proxy.host, "127.0.0.1");
        }
        {
            let mut store = MetaStore::default();
            store
                .add_proxy(
                    proxy_address.to_string(),
                    nodes.clone(),
                    Some("localhost".to_string()),
                )
                .unwrap();
            let proxies = store.get_free_proxies();
            let proxy = proxies.get(0).unwrap();
            assert_eq!(proxy.proxy_address, proxy_address);
            assert_eq!(proxy.host, "localhost");
        }
    }

    fn check_cluster_and_proxy(store: &MetaStore) {
        for cluster in store.clusters.values() {
            for chunk in cluster.chunks.iter() {
                for proxy_address in chunk.proxy_addresses.iter() {
                    let proxy = store.all_proxies.get(proxy_address).unwrap();
                    assert_eq!(proxy.cluster.as_ref().unwrap(), &cluster.name);
                }
            }
        }
        for proxy in store.all_proxies.values() {
            let cluster_name = match proxy.cluster.as_ref() {
                Some(name) => name,
                None => continue,
            };
            let cluster = store.clusters.get(cluster_name).unwrap();
            assert!(cluster.chunks.iter().any(|chunk| chunk
                .proxy_addresses
                .iter()
                .any(|addr| addr == &proxy.proxy_address)));
        }
    }

    #[test]
    fn test_add_and_remove_cluster() {
        let migration_limit = 0;

        let mut store = MetaStore::default();
        add_testing_proxies(&mut store, 4, 3);
        let proxies: Vec<_> = store
            .get_proxies()
            .into_iter()
            .filter_map(|proxy_address| store.get_proxy_by_address(&proxy_address, migration_limit))
            .collect();
        let original_free_node_num: usize = proxies
            .iter()
            .map(|proxy| proxy.get_free_nodes().len())
            .sum();

        let epoch1 = store.get_global_epoch();

        check_cluster_and_proxy(&store);
        let cluster_name = "testcluster".to_string();
        store.add_cluster(cluster_name.clone(), 4).unwrap();
        let epoch2 = store.get_global_epoch();
        assert!(epoch1 < epoch2);
        check_cluster_and_proxy(&store);

        let names: Vec<String> = store
            .get_cluster_names()
            .into_iter()
            .map(|cluster_name| cluster_name.to_string())
            .collect();
        assert_eq!(names, vec![cluster_name.clone()]);

        let cluster = store
            .get_cluster_by_name(&cluster_name, migration_limit)
            .unwrap();
        assert_eq!(cluster.get_nodes().len(), 4);

        check_cluster_slots(cluster.clone(), 4);

        let proxies: Vec<_> = store
            .get_proxies()
            .into_iter()
            .filter_map(|proxy_address| store.get_proxy_by_address(&proxy_address, migration_limit))
            .collect();
        let free_node_num: usize = proxies
            .iter()
            .map(|proxy| proxy.get_free_nodes().len())
            .sum();
        assert_eq!(free_node_num, original_free_node_num - 4);

        let another_cluster = "another_cluster".to_string();
        store.add_cluster(another_cluster.clone(), 4).unwrap();
        let epoch3 = store.get_global_epoch();
        assert!(epoch2 <= epoch3);
        check_cluster_and_proxy(&store);

        for node in cluster.get_nodes() {
            let proxy = store
                .get_proxy_by_address(&node.get_proxy_address(), migration_limit)
                .unwrap();
            assert_eq!(proxy.get_free_nodes().len(), 0);
            assert_eq!(proxy.get_nodes().len(), 2);
            let proxy_port = node
                .get_proxy_address()
                .split(':')
                .nth(1)
                .unwrap()
                .parse::<usize>()
                .unwrap();
            let node_port = node
                .get_address()
                .split(':')
                .nth(1)
                .unwrap()
                .parse::<usize>()
                .unwrap();
            assert_eq!(proxy_port - 7000, (node_port - 6000) / 2);
        }

        let node_addresses_set: HashSet<String> = cluster
            .get_nodes()
            .iter()
            .map(|node| node.get_address().to_string())
            .collect();
        assert_eq!(node_addresses_set.len(), cluster.get_nodes().len());
        let proy_addresses_set: HashSet<String> = cluster
            .get_nodes()
            .iter()
            .map(|node| node.get_proxy_address().to_string())
            .collect();
        assert_eq!(proy_addresses_set.len() * 2, cluster.get_nodes().len());

        store.remove_cluster(cluster_name.clone()).unwrap();
        let epoch4 = store.get_global_epoch();
        assert!(epoch3 < epoch4);
        check_cluster_and_proxy(&store);

        let proxies: Vec<_> = store
            .get_proxies()
            .into_iter()
            .filter_map(|proxy_address| store.get_proxy_by_address(&proxy_address, migration_limit))
            .collect();
        let free_node_num: usize = proxies
            .iter()
            .map(|proxy| proxy.get_free_nodes().len())
            .sum();
        assert_eq!(free_node_num, original_free_node_num - 4);

        store.remove_cluster(another_cluster.clone()).unwrap();
        check_cluster_and_proxy(&store);
        let proxies: Vec<_> = store
            .get_proxies()
            .into_iter()
            .filter_map(|proxy_address| store.get_proxy_by_address(&proxy_address, migration_limit))
            .collect();
        let free_node_num: usize = proxies
            .iter()
            .map(|proxy| proxy.get_free_nodes().len())
            .sum();
        assert_eq!(free_node_num, original_free_node_num);
    }

    #[test]
    fn test_allocation_distribution() {
        let migration_limit = 0;

        let mut store = MetaStore::default();
        let host_num = 11;
        assert_eq!(host_num % 2, 1);
        add_testing_proxies(&mut store, host_num, 3);
        {
            let mut count_map: HashMap<String, usize> = HashMap::new();
            for addr in store.get_proxies() {
                let host = addr.split(':').next().unwrap().to_string();
                count_map.entry(host.clone()).or_insert(0);
                *count_map.get_mut(&host).unwrap() += 1;
            }
            assert_eq!(count_map.len(), host_num);
        }

        {
            store
                .add_cluster("testcluster".to_string(), (host_num - 1) * 2)
                .unwrap();
            let mut count_map: HashMap<String, usize> = HashMap::new();
            for addr in store.get_proxies() {
                let proxy = store.get_proxy_by_address(&addr, migration_limit).unwrap();
                if !proxy.get_free_nodes().is_empty() {
                    continue;
                }
                let host = addr.split(':').next().unwrap().to_string();
                count_map.entry(host.clone()).or_insert(0);
                *count_map.get_mut(&host).unwrap() += 1;
            }
            assert_eq!(count_map.len(), host_num - 1);
            for count in count_map.values() {
                assert_eq!(*count, 1);
            }
        }

        {
            store.add_cluster("testcluster2".to_string(), 4).unwrap();
            let mut count_map: HashMap<String, usize> = HashMap::new();
            for addr in store.get_proxies() {
                let proxy = store.get_proxy_by_address(&addr, migration_limit).unwrap();
                if !proxy.get_free_nodes().is_empty() {
                    continue;
                }
                let host = addr.split(':').next().unwrap().to_string();
                count_map.entry(host.clone()).or_insert(0);
                *count_map.get_mut(&host).unwrap() += 1;
            }
            assert_eq!(count_map.len(), host_num);
            assert_eq!(count_map.values().sum::<usize>(), host_num + 1);
            for count in count_map.values() {
                assert!(*count >= 1);
                assert!(*count <= 2);
            }
        }
    }

    #[test]
    fn test_failures() {
        let migration_limit = 0;

        let mut store = MetaStore::default();
        const ALL_PROXIES: usize = 4 * 3;
        add_testing_proxies(&mut store, 4, 3);
        assert_eq!(store.get_free_proxies().len(), ALL_PROXIES);

        let original_proxy_num = store.get_proxies().len();
        let failed_address = "127.0.0.1:7001";
        assert!(store
            .get_proxy_by_address(failed_address, migration_limit)
            .is_some());
        let epoch1 = store.get_global_epoch();

        store.add_failure(failed_address.to_string(), "reporter_id".to_string());
        let epoch2 = store.get_global_epoch();
        assert!(epoch1 < epoch2);
        let proxy_num = store.get_proxies().len();
        assert_eq!(proxy_num, original_proxy_num);
        assert_eq!(store.get_free_proxies().len(), ALL_PROXIES - 1);

        assert_eq!(
            store.get_failures(chrono::Duration::max_value(), 1),
            vec![failed_address.to_string()],
        );
        assert!(store
            .get_failures(chrono::Duration::max_value(), 2)
            .is_empty(),);
        store.remove_proxy(failed_address.to_string()).unwrap();
        let epoch3 = store.get_global_epoch();
        assert!(epoch2 < epoch3);

        let cluster_name = "testcluster".to_string();
        store.add_cluster(cluster_name.clone(), 4).unwrap();
        assert_eq!(store.get_free_proxies().len(), ALL_PROXIES - 3);
        let epoch4 = store.get_global_epoch();
        assert!(epoch3 < epoch4);

        let cluster = store
            .get_cluster_by_name(&cluster_name, migration_limit)
            .unwrap();
        check_cluster_slots(cluster.clone(), 4);

        let (failed_proxy_address, peer_proxy_address) = cluster
            .get_nodes()
            .get(0)
            .map(|node| {
                (
                    node.get_proxy_address().to_string(),
                    node.get_repl_meta().get_peers()[0].proxy_address.clone(),
                )
            })
            .unwrap();
        store.add_failure(failed_proxy_address.clone(), "reporter_id".to_string());
        assert_eq!(store.get_free_proxies().len(), 9);
        let epoch5 = store.get_global_epoch();
        assert!(epoch4 < epoch5);

        let proxy_num = store.get_proxies().len();
        assert_eq!(proxy_num, original_proxy_num - 1);
        assert_eq!(
            store.get_failures(chrono::Duration::max_value(), 1),
            vec![failed_proxy_address.clone()]
        );

        let new_proxy = store
            .replace_failed_proxy(failed_proxy_address.clone(), migration_limit)
            .unwrap()
            .unwrap();
        assert_ne!(new_proxy.get_address(), failed_proxy_address);
        let epoch6 = store.get_global_epoch();
        assert!(epoch5 < epoch6);

        let cluster = store
            .get_cluster_by_name(&cluster_name, migration_limit)
            .unwrap();
        assert_eq!(
            cluster
                .get_nodes()
                .iter()
                .filter(|node| node.get_proxy_address() == &failed_proxy_address)
                .count(),
            0
        );
        assert_eq!(
            cluster
                .get_nodes()
                .iter()
                .filter(|node| node.get_proxy_address() == new_proxy.get_address())
                .count(),
            2
        );
        for node in cluster.get_nodes().iter() {
            if node.get_proxy_address() == peer_proxy_address {
                assert_eq!(node.get_role(), Role::Master);
            } else if node.get_proxy_address() == new_proxy.get_address() {
                assert_eq!(node.get_role(), Role::Replica);
            }
        }

        // Recover proxy
        let nodes = store
            .all_proxies
            .get(&failed_proxy_address)
            .unwrap()
            .node_addresses
            .clone();
        let err = store
            .add_proxy(failed_proxy_address.clone(), nodes, None)
            .unwrap_err();
        assert_eq!(err, MetaStoreError::AlreadyExisted);
        assert_eq!(
            store.get_failures(chrono::Duration::max_value(), 1).len(),
            0
        );
        let epoch7 = store.get_global_epoch();
        assert!(epoch6 < epoch7);
    }

    const CLUSTER_NAME: &'static str = "testcluster";

    #[test]
    fn test_add_and_delete_free_nodes() {
        let migration_limit = 0;

        let mut store = MetaStore::default();
        let host_num = 4;
        let proxy_per_host = 3;
        let all_proxy_num = host_num * proxy_per_host;
        add_testing_proxies(&mut store, host_num, proxy_per_host);
        assert_eq!(store.get_free_proxies().len(), all_proxy_num);

        let cluster_name = CLUSTER_NAME.to_string();

        store.add_cluster(cluster_name.clone(), 4).unwrap();
        let epoch1 = store.get_global_epoch();
        assert_eq!(store.get_free_proxies().len(), all_proxy_num - 2);
        assert_eq!(
            store
                .get_cluster_by_name(&cluster_name, migration_limit)
                .unwrap()
                .get_nodes()
                .len(),
            4
        );

        store.auto_add_nodes(cluster_name.clone(), 4).unwrap();
        let epoch2 = store.get_global_epoch();
        assert!(epoch1 < epoch2);
        assert_eq!(store.get_free_proxies().len(), all_proxy_num - 4);
        assert_eq!(
            store
                .get_cluster_by_name(&cluster_name, migration_limit)
                .unwrap()
                .get_nodes()
                .len(),
            8
        );

        store.audo_delete_free_nodes(cluster_name.clone()).unwrap();
        let epoch3 = store.get_global_epoch();
        assert!(epoch2 < epoch3);
        assert_eq!(store.get_free_proxies().len(), all_proxy_num - 2);
        assert_eq!(
            store
                .get_cluster_by_name(&cluster_name, migration_limit)
                .unwrap()
                .get_nodes()
                .len(),
            4
        );
    }

    fn test_migration_helper(
        host_num: usize,
        proxy_per_host: usize,
        start_node_num: usize,
        added_node_num: usize,
        migration_limit: u64,
    ) {
        let mut store =
            init_migration_test_store(host_num, proxy_per_host, start_node_num, migration_limit);
        test_scaling(
            &mut store,
            host_num * proxy_per_host,
            added_node_num,
            migration_limit,
        );
    }

    fn init_migration_test_store(
        host_num: usize,
        proxy_per_host: usize,
        start_node_num: usize,
        migration_limit: u64,
    ) -> MetaStore {
        let mut store = MetaStore::default();
        let all_proxy_num = host_num * proxy_per_host;
        add_testing_proxies(&mut store, host_num, proxy_per_host);
        assert_eq!(store.get_free_proxies().len(), all_proxy_num);

        let cluster_name = CLUSTER_NAME.to_string();
        store
            .add_cluster(cluster_name.clone(), start_node_num)
            .unwrap();
        let cluster = store
            .get_cluster_by_name(&cluster_name, migration_limit)
            .unwrap();
        assert_eq!(cluster.get_nodes().len(), start_node_num);
        assert_eq!(
            store.get_free_proxies().len(),
            all_proxy_num - start_node_num / 2
        );

        store
    }

    fn no_op(_: &mut MetaStore, _: u64) {}

    fn test_scaling(
        store: &mut MetaStore,
        all_proxy_num: usize,
        added_node_num: usize,
        migration_limit: u64,
    ) {
        test_scaling_helper(store, all_proxy_num, added_node_num, migration_limit, no_op);
    }

    fn test_scaling_helper<F>(
        store: &mut MetaStore,
        all_proxy_num: usize,
        added_node_num: usize,
        migration_limit: u64,
        injection: F,
    ) where
        F: Fn(&mut MetaStore, u64),
    {
        let cluster_name = CLUSTER_NAME.to_string();
        let start_node_num = store
            .get_cluster_by_name(&cluster_name, migration_limit)
            .unwrap()
            .get_nodes()
            .len();

        let epoch1 = store.get_global_epoch();
        let nodes = store
            .auto_add_nodes(cluster_name.clone(), added_node_num)
            .unwrap();
        let epoch2 = store.get_global_epoch();
        assert!(epoch1 < epoch2);
        assert_eq!(nodes.len(), added_node_num);
        let cluster = store
            .get_cluster_by_name(&cluster_name, migration_limit)
            .unwrap();
        assert_eq!(cluster.get_nodes().len(), start_node_num + added_node_num);
        assert_eq!(
            store.get_free_proxies().len()
                + store.get_failures(chrono::Duration::max_value(), 1).len(),
            all_proxy_num - start_node_num / 2 - added_node_num / 2
        );

        store.migrate_slots(cluster_name.clone()).unwrap();
        let epoch3 = store.get_global_epoch();
        assert!(epoch2 < epoch3);

        injection(store, migration_limit);

        for limit in [0, migration_limit].iter() {
            let cluster = store.get_cluster_by_name(&cluster_name, *limit).unwrap();
            assert_eq!(cluster.get_nodes().len(), start_node_num + added_node_num);
            for (i, node) in cluster.get_nodes().iter().enumerate() {
                if i < start_node_num {
                    if node.get_role() == Role::Replica {
                        continue;
                    }
                    let slots = node.get_slots();
                    // Some src slots might not need to transfer.
                    assert!(slots.len() >= 1);
                    assert!(slots[0].tag.is_stable());
                    for slot_range in slots.iter().skip(1) {
                        assert!(slot_range.tag.is_migrating());
                    }
                } else {
                    if node.get_role() == Role::Replica {
                        continue;
                    }
                    let slots = node.get_slots();
                    // zero for no limit
                    if *limit != 0 {
                        continue;
                    }
                    assert!(slots.len() >= 1);
                    for slot_range in slots.iter() {
                        assert!(slot_range.tag.is_importing());
                    }
                }
            }
        }

        // Due to migration limit, we might need to commit migration and get remaining ones multiple times
        loop {
            injection(store, migration_limit);

            let cluster = store
                .get_cluster_by_name(&cluster_name, migration_limit)
                .unwrap();

            let slot_range_set: HashSet<_> = cluster
                .get_nodes()
                .iter()
                .filter(|node| node.get_role() == Role::Master)
                .flat_map(|node| node.get_slots().iter())
                .filter_map(|slot_range| match slot_range.tag {
                    SlotRangeTag::Migrating(_) => Some(slot_range.clone()),
                    _ => None,
                })
                .collect();

            if slot_range_set.is_empty() {
                break;
            }

            for slot_range in slot_range_set.into_iter() {
                let task_meta = MigrationTaskMeta {
                    cluster_name: ClusterName::try_from(cluster_name.as_str()).unwrap(),
                    slot_range,
                };
                store.commit_migration(task_meta).unwrap();
            }
        }

        let cluster = store
            .get_cluster_by_name(&cluster_name, migration_limit)
            .unwrap();
        check_cluster_slots(cluster, start_node_num + added_node_num);
    }

    fn check_cluster_slots(cluster: Cluster, node_num: usize) {
        assert_eq!(cluster.get_nodes().len(), node_num);
        let master_num = cluster.get_nodes().len() / 2;
        let average_slots_num = SLOT_NUM / master_num;

        let mut visited = Vec::with_capacity(SLOT_NUM);
        for _ in 0..SLOT_NUM {
            visited.push(false);
        }

        for node in cluster.get_nodes() {
            let slots = node.get_slots();
            if node.get_role() == Role::Master {
                assert_eq!(slots.len(), 1);
                assert_eq!(slots[0].tag, SlotRangeTag::None);
                let slots_num = slots[0].get_range_list().get_slots_num();
                let delta = slots_num.checked_sub(average_slots_num).unwrap();
                assert!(delta <= 1);

                for range in slots[0].get_range_list().get_ranges().iter() {
                    for i in range.start()..=range.end() {
                        assert!(!visited.get(i).unwrap());
                        *visited.get_mut(i).unwrap() = true;
                    }
                }

                let mut sorted_range_list = slots[0].get_range_list().clone();
                sorted_range_list.compact();
                assert_eq!(&sorted_range_list, slots[0].get_range_list());
            } else {
                assert!(slots.is_empty());
            }
        }
        for v in visited.iter() {
            assert!(*v);
        }

        let mut last_node_slot_num = usize::max_value();
        for node in cluster.get_nodes() {
            if node.get_role() == Role::Replica {
                continue;
            }
            let curr_num = node
                .get_slots()
                .iter()
                .map(|slots| slots.get_range_list().get_slots_num())
                .sum();
            assert!(last_node_slot_num >= curr_num);
            last_node_slot_num = curr_num;
        }
    }

    #[test]
    fn test_migration() {
        // Can increase them to cover more cases.
        const MAX_HOST_NUM: usize = 6;
        const MAX_PROXY_PER_HOST: usize = 6;
        const MAX_MIGRATION_LIMIT: u64 = 1;

        for host_num in 2..=MAX_HOST_NUM {
            for proxy_per_host in 1..=MAX_PROXY_PER_HOST {
                for migration_limit in 0..=MAX_MIGRATION_LIMIT {
                    let chunk_num = host_num * proxy_per_host / 2;
                    for i in 1..chunk_num {
                        let added_chunk_num = chunk_num - i;
                        if added_chunk_num == 0 {
                            continue;
                        }
                        for j in 1..=added_chunk_num {
                            assert!(i + j <= chunk_num);
                            test_migration_helper(
                                host_num,
                                proxy_per_host,
                                4 * i,
                                4 * j,
                                migration_limit,
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn test_multiple_migration() {
        const MAX_HOST_NUM: usize = 6;
        const MAX_PROXY_PER_HOST: usize = 6;
        assert_eq!(MAX_HOST_NUM * MAX_PROXY_PER_HOST % 2, 0);
        const MAX_MIGRATION_LIMIT: u64 = 1;

        for host_num in 2..=MAX_HOST_NUM {
            for proxy_per_host in 1..=MAX_PROXY_PER_HOST {
                for migration_limit in 0..=MAX_MIGRATION_LIMIT {
                    let sum_node_num = host_num * proxy_per_host * 2;
                    let mut store =
                        init_migration_test_store(host_num, proxy_per_host, 4, migration_limit);
                    let mut remnant = sum_node_num - 4;
                    while remnant > 4 {
                        test_scaling(&mut store, host_num * proxy_per_host, 4, migration_limit);
                        remnant -= 4;
                    }
                }
            }
        }
    }

    fn add_failure_and_replace_proxy(store: &mut MetaStore, migration_limit: u64) {
        if store.get_free_proxies().is_empty() {
            return;
        }

        let cluster_name = CLUSTER_NAME;
        let cluster = store
            .get_cluster_by_name(cluster_name, migration_limit)
            .unwrap();
        let (failed_proxy_address, peer_proxy_address) = cluster
            .get_nodes()
            .iter()
            .find(|node| {
                node.get_slots()
                    .iter()
                    .any(|slot_range| slot_range.tag != SlotRangeTag::None)
            })
            .cloned()
            .or_else(|| Some(cluster.get_nodes()[0].clone()))
            .map(|node| {
                (
                    node.get_proxy_address().to_string(),
                    node.get_repl_meta().get_peers()[0].proxy_address.clone(),
                )
            })
            .unwrap();
        store.add_failure(failed_proxy_address.clone(), "reporter_id".to_string());

        assert!(store
            .get_failures(chrono::Duration::max_value(), 1)
            .contains(&failed_proxy_address),);
        assert!(store.get_free_proxies().len() > 0);

        let new_proxy = store
            .replace_failed_proxy(failed_proxy_address.clone(), migration_limit)
            .unwrap()
            .unwrap();
        assert_ne!(new_proxy.get_address(), failed_proxy_address);

        let cluster = store
            .get_cluster_by_name(cluster_name, migration_limit)
            .unwrap();
        assert_eq!(
            cluster
                .get_nodes()
                .iter()
                .filter(|node| node.get_proxy_address() == &failed_proxy_address)
                .count(),
            0
        );
        assert_eq!(
            cluster
                .get_nodes()
                .iter()
                .filter(|node| node.get_proxy_address() == new_proxy.get_address())
                .count(),
            2
        );

        for node in cluster.get_nodes().iter() {
            if node.get_proxy_address() == peer_proxy_address {
                assert_eq!(node.get_role(), Role::Master);
            } else if node.get_proxy_address() == new_proxy.get_address() {
                assert_eq!(node.get_role(), Role::Replica);
            }
        }
    }

    #[test]
    fn test_failure_on_migration() {
        const MAX_HOST_NUM: usize = 6;
        const MAX_PROXY_PER_HOST: usize = 6;

        let host_num = 6;
        let proxy_per_host = 6;
        assert_eq!(MAX_HOST_NUM * MAX_PROXY_PER_HOST % 2, 0);
        let migration_limit = 0;

        let mut store = init_migration_test_store(host_num, proxy_per_host, 4, migration_limit);
        let added_node_num = 4;
        assert!(store.get_free_proxies().len() >= host_num * proxy_per_host / 2);
        while store.get_free_proxies().len() >= host_num * proxy_per_host / 2 {
            test_scaling_helper(
                &mut store,
                host_num * proxy_per_host,
                added_node_num,
                migration_limit,
                add_failure_and_replace_proxy,
            );
        }
    }

    fn test_scaling_down_helper<F>(
        store: &mut MetaStore,
        all_proxy_num: usize,
        removed_node_num: usize,
        migration_limit: u64,
        injection: F,
    ) where
        F: Fn(&mut MetaStore, u64),
    {
        let cluster_name = CLUSTER_NAME.to_string();
        let start_node_num = store
            .get_cluster_by_name(&cluster_name, migration_limit)
            .unwrap()
            .get_nodes()
            .len();

        let epoch1 = store.get_global_epoch();
        store
            .migrate_slots_to_scale_down(cluster_name.clone(), start_node_num - removed_node_num)
            .unwrap();
        let epoch2 = store.get_global_epoch();
        assert!(epoch1 < epoch2);

        injection(store, migration_limit);

        for limit in [0, migration_limit].iter() {
            let cluster = store.get_cluster_by_name(&cluster_name, *limit).unwrap();
            assert_eq!(cluster.get_nodes().len(), start_node_num);
            for (i, node) in cluster.get_nodes().iter().enumerate() {
                if i >= start_node_num - removed_node_num {
                    if node.get_role() == Role::Replica {
                        continue;
                    }
                    let slots = node.get_slots();
                    assert!(slots.len() >= 1);
                    // zero for no limit
                    if *limit != 0 {
                        continue;
                    }
                    for slot_range in slots.iter() {
                        assert!(slot_range.tag.is_migrating());
                    }
                } else {
                    if node.get_role() == Role::Replica {
                        continue;
                    }
                    let slots = node.get_slots();
                    // Some dst might not get the new slots.
                    assert!(slots.len() >= 1);
                    assert!(slots[0].tag.is_stable());
                    // zero for no limit
                    if *limit != 0 {
                        continue;
                    }
                    for slot_range in slots.iter().skip(1) {
                        assert!(slot_range.tag.is_importing());
                    }
                }
            }
        }

        // Due to migration limit, we might need to commit migration and get remaining ones multiple times
        loop {
            injection(store, migration_limit);

            let cluster = store
                .get_cluster_by_name(&cluster_name, migration_limit)
                .unwrap();

            let slot_range_set: HashSet<_> = cluster
                .get_nodes()
                .iter()
                .filter(|node| node.get_role() == Role::Master)
                .flat_map(|node| node.get_slots().iter())
                .filter_map(|slot_range| match slot_range.tag {
                    SlotRangeTag::Migrating(_) => Some(slot_range.clone()),
                    _ => None,
                })
                .collect();

            if slot_range_set.is_empty() {
                break;
            }

            for slot_range in slot_range_set.into_iter() {
                let task_meta = MigrationTaskMeta {
                    cluster_name: ClusterName::try_from(cluster_name.as_str()).unwrap(),
                    slot_range,
                };
                store.commit_migration(task_meta).unwrap();
            }
        }

        store.audo_delete_free_nodes(cluster_name.clone()).unwrap();
        let epoch3 = store.get_global_epoch();
        assert!(epoch2 < epoch3);

        let cluster = store
            .get_cluster_by_name(&cluster_name, migration_limit)
            .unwrap();
        assert_eq!(cluster.get_nodes().len(), start_node_num - removed_node_num);
        assert_eq!(
            store.get_free_proxies().len()
                + store.get_failures(chrono::Duration::max_value(), 1).len(),
            all_proxy_num - start_node_num / 2 + removed_node_num / 2
        );

        let cluster = store
            .get_cluster_by_name(&cluster_name, migration_limit)
            .unwrap();
        check_cluster_slots(cluster, start_node_num - removed_node_num);
    }

    fn test_migration_to_scale_down_helper(
        host_num: usize,
        proxy_per_host: usize,
        start_node_num: usize,
        removed_node_num: usize,
        migration_limit: u64,
    ) {
        let mut store =
            init_migration_test_store(host_num, proxy_per_host, start_node_num, migration_limit);
        test_scaling_down_helper(
            &mut store,
            host_num * proxy_per_host,
            removed_node_num,
            migration_limit,
            no_op,
        );
    }

    #[test]
    fn test_scaling_down() {
        // Can increase them to cover more cases.
        const MAX_HOST_NUM: usize = 6;
        const MAX_PROXY_PER_HOST: usize = 6;
        const MAX_MIGRATION_LIMIT: u64 = 1;

        for host_num in 2..=MAX_HOST_NUM {
            for proxy_per_host in 1..=MAX_PROXY_PER_HOST {
                for migration_limit in 0..=MAX_MIGRATION_LIMIT {
                    let chunk_num = host_num * proxy_per_host / 2;
                    for i in 1..chunk_num {
                        let added_chunk_num = chunk_num - i;
                        if added_chunk_num == 0 {
                            continue;
                        }
                        for j in 1..=added_chunk_num {
                            assert!(i + j <= chunk_num);
                            if i <= j {
                                continue;
                            }
                            test_migration_to_scale_down_helper(
                                host_num,
                                proxy_per_host,
                                4 * i,
                                4 * j,
                                migration_limit,
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn test_multiple_migration_for_scaling_down() {
        const MAX_HOST_NUM: usize = 6;
        const MAX_PROXY_PER_HOST: usize = 6;
        assert_eq!(MAX_HOST_NUM * MAX_PROXY_PER_HOST % 2, 0);
        const MAX_MIGRATION_LIMIT: u64 = 1;

        for host_num in 2..=MAX_HOST_NUM {
            for proxy_per_host in 1..=MAX_PROXY_PER_HOST {
                let start_node_num = (host_num * proxy_per_host * 2) / 4 * 4;
                for migration_limit in 0..=MAX_MIGRATION_LIMIT {
                    let mut store = init_migration_test_store(
                        host_num,
                        proxy_per_host,
                        start_node_num,
                        migration_limit,
                    );
                    let mut remnant = start_node_num;
                    while remnant > 4 {
                        test_scaling_down_helper(
                            &mut store,
                            host_num * proxy_per_host,
                            4,
                            migration_limit,
                            no_op,
                        );
                        remnant -= 4;
                    }
                }
            }
        }
    }

    #[test]
    fn test_failure_on_scaling_down() {
        let host_num = 6;
        let proxy_per_host = 6;
        assert_eq!(host_num * proxy_per_host % 2, 0);
        let migration_limit = 0;
        let start_node_num = 12;

        let mut store =
            init_migration_test_store(host_num, proxy_per_host, start_node_num, migration_limit);
        let removed_node_num = 4;
        test_scaling_down_helper(
            &mut store,
            host_num * proxy_per_host,
            removed_node_num,
            migration_limit,
            add_failure_and_replace_proxy,
        );
    }

    #[test]
    fn test_scaling_up_and_down() {
        let host_num = 12;
        let proxy_per_host = 1;
        let migration_limit = 0;
        let start_node_num = 24;

        let mut store =
            init_migration_test_store(host_num, proxy_per_host, start_node_num, migration_limit);

        test_scaling_down_helper(
            &mut store,
            host_num * proxy_per_host,
            12,
            migration_limit,
            no_op,
        );

        test_scaling_helper(
            &mut store,
            host_num * proxy_per_host,
            8,
            migration_limit,
            no_op,
        );

        test_scaling_helper(
            &mut store,
            host_num * proxy_per_host,
            4,
            migration_limit,
            no_op,
        );

        test_scaling_down_helper(
            &mut store,
            host_num * proxy_per_host,
            12,
            migration_limit,
            no_op,
        );
    }

    #[test]
    fn test_config() {
        let migration_limit = 0;

        let mut store = MetaStore::default();
        add_testing_proxies(&mut store, 4, 3);

        let cluster_name = CLUSTER_NAME.to_string();
        store.add_cluster(cluster_name.clone(), 4).unwrap();
        let cluster_config = store
            .get_cluster_by_name(&cluster_name, migration_limit)
            .unwrap()
            .get_config();
        assert_eq!(
            cluster_config.compression_strategy,
            CompressionStrategy::Disabled
        );

        let mut config = HashMap::new();
        config.insert(
            "compression_strategy".to_string(),
            "set_get_only".to_string(),
        );
        store.change_config(cluster_name.clone(), config).unwrap();

        let cluster_config = store
            .get_cluster_by_name(&cluster_name, migration_limit)
            .unwrap()
            .get_config();
        assert_eq!(
            cluster_config.compression_strategy,
            CompressionStrategy::SetGetOnly
        );
    }

    #[test]
    fn test_limited_migration() {
        let mut store = MetaStore::default();
        add_testing_proxies(&mut store, 4, 3);

        let migration_limit = 1;
        let cluster_name = CLUSTER_NAME.to_string();
        store.add_cluster(cluster_name.clone(), 4).unwrap();
        store.auto_add_nodes(cluster_name.clone(), 4).unwrap();
        store.migrate_slots(cluster_name.clone()).unwrap();
        let cluster = store
            .get_cluster_by_name(CLUSTER_NAME, migration_limit)
            .unwrap();
        let migrating_masters = cluster
            .get_nodes()
            .iter()
            .filter(|node| {
                node.get_role() == Role::Master
                    && node
                        .get_slots()
                        .iter()
                        .any(|slots| slots.tag != SlotRangeTag::None)
            })
            .count();
        assert_eq!(migrating_masters, 2);
    }

    #[test]
    fn test_unlimited_migration() {
        let mut store = MetaStore::default();
        add_testing_proxies(&mut store, 4, 3);

        let migration_limit = 0;
        let cluster_name = CLUSTER_NAME.to_string();
        store.add_cluster(cluster_name.clone(), 4).unwrap();
        store.auto_add_nodes(cluster_name.clone(), 4).unwrap();
        store.migrate_slots(cluster_name.clone()).unwrap();
        let cluster = store
            .get_cluster_by_name(CLUSTER_NAME, migration_limit)
            .unwrap();
        let migrating_masters = cluster
            .get_nodes()
            .iter()
            .filter(|node| {
                node.get_role() == Role::Master
                    && node
                        .get_slots()
                        .iter()
                        .any(|slots| slots.tag != SlotRangeTag::None)
            })
            .count();
        assert_eq!(migrating_masters, 4);
    }

    // Docs examples:
    #[test]
    fn test_one_proxy_per_host() {
        let host_num = 6;
        let proxy_per_host = 1;
        let migration_limit = 2;
        let added_node_num = 4;
        let removed_node_num = 4;

        let mut store = init_migration_test_store(host_num, proxy_per_host, 4, migration_limit);
        test_scaling(
            &mut store,
            host_num * proxy_per_host,
            added_node_num,
            migration_limit,
        );
        assert_eq!(
            store
                .get_cluster_by_name(CLUSTER_NAME, 1)
                .unwrap()
                .get_nodes()
                .len(),
            8
        );
        test_scaling_down_helper(
            &mut store,
            host_num * proxy_per_host,
            removed_node_num,
            migration_limit,
            no_op,
        );
        assert_eq!(
            store
                .get_cluster_by_name(CLUSTER_NAME, 1)
                .unwrap()
                .get_nodes()
                .len(),
            4
        );
        assert!(!store.get_free_proxies().is_empty());
        add_failure_and_replace_proxy(&mut store, migration_limit);
    }

    #[test]
    fn test_balance_masters() {
        let mut store = MetaStore::default();
        let host_num = 3;
        let proxy_per_host = 1;
        let all_proxy_num = host_num * proxy_per_host;
        add_testing_proxies(&mut store, host_num, proxy_per_host);
        assert_eq!(store.get_free_proxies().len(), all_proxy_num);

        let cluster_name = CLUSTER_NAME.to_string();
        store.add_cluster(cluster_name.clone(), 4).unwrap();
        let cluster = store.get_cluster_by_name(&cluster_name, 1).unwrap();

        let proxy_address = cluster
            .get_nodes()
            .get(0)
            .unwrap()
            .get_proxy_address()
            .to_string();
        store
            .replace_failed_proxy(proxy_address.clone(), 1)
            .unwrap()
            .unwrap();

        for chunk in store
            .clusters
            .get(&ClusterName::try_from(cluster_name.as_str()).unwrap())
            .unwrap()
            .chunks
            .iter()
        {
            assert_ne!(chunk.role_position, ChunkRolePosition::Normal);
        }

        let cluster = store.get_cluster_by_name(&cluster_name, 1).unwrap();
        let epoch1 = cluster.get_epoch();
        store.balance_masters(cluster_name.clone()).unwrap();
        for chunk in store
            .clusters
            .get(&ClusterName::try_from(cluster_name.as_str()).unwrap())
            .unwrap()
            .chunks
            .iter()
        {
            assert_eq!(chunk.role_position, ChunkRolePosition::Normal);
        }
        let cluster = store.get_cluster_by_name(&cluster_name, 1).unwrap();
        let epoch2 = cluster.get_epoch();
        assert!(epoch2 > epoch1);

        // Won't change role for failed proxy
        let cluster = store.get_cluster_by_name(cluster_name.as_str(), 1).unwrap();
        let proxy_address = cluster
            .get_nodes()
            .get(0)
            .unwrap()
            .get_proxy_address()
            .to_string();
        let err = store
            .replace_failed_proxy(proxy_address.clone(), 1)
            .unwrap_err();
        assert_eq!(err, MetaStoreError::NoAvailableResource);

        for chunk in store
            .clusters
            .get(&ClusterName::try_from(cluster_name.as_str()).unwrap())
            .unwrap()
            .chunks
            .iter()
        {
            assert_ne!(chunk.role_position, ChunkRolePosition::Normal);
        }

        assert!(store.get_free_proxies().is_empty());
        store.balance_masters(cluster_name.clone()).unwrap();
        for chunk in store
            .clusters
            .get(&ClusterName::try_from(cluster_name.as_str()).unwrap())
            .unwrap()
            .chunks
            .iter()
        {
            assert_ne!(chunk.role_position, ChunkRolePosition::Normal);
        }
    }

    #[test]
    fn test_bump_epoch() {
        let mut store = MetaStore::default();
        let host_num = 3;
        let proxy_per_host = 1;
        let all_proxy_num = host_num * proxy_per_host;
        add_testing_proxies(&mut store, host_num, proxy_per_host);
        assert_eq!(store.get_free_proxies().len(), all_proxy_num);

        let cluster_name = CLUSTER_NAME.to_string();
        store.add_cluster(cluster_name.clone(), 4).unwrap();
        let cluster = store.get_cluster_by_name(&cluster_name, 1).unwrap();

        const NEW_EPOCH: u64 = 233;

        assert!(cluster.get_epoch() < NEW_EPOCH);

        store.force_bump_all_epoch(NEW_EPOCH).unwrap();
        let cluster = store.get_cluster_by_name(&cluster_name, 1).unwrap();
        assert_eq!(cluster.get_epoch(), NEW_EPOCH);
        assert_eq!(store.get_global_epoch(), NEW_EPOCH);
    }

    #[test]
    fn test_recover_epoch() {
        let mut store = MetaStore::default();
        let host_num = 3;
        let proxy_per_host = 1;
        let all_proxy_num = host_num * proxy_per_host;
        add_testing_proxies(&mut store, host_num, proxy_per_host);
        assert_eq!(store.get_free_proxies().len(), all_proxy_num);

        let cluster_name = CLUSTER_NAME.to_string();
        store.add_cluster(cluster_name.clone(), 4).unwrap();
        let cluster = store.get_cluster_by_name(&cluster_name, 1).unwrap();

        // The epoch of free proxy will be global epoch.
        let new_epoch = store.get_global_epoch() + 1;
        assert!(cluster.get_epoch() < new_epoch);

        store.recover_epoch(new_epoch);
        let cluster = store.get_cluster_by_name(&cluster_name, 1).unwrap();
        assert_eq!(cluster.get_epoch(), new_epoch);
        assert_eq!(store.get_global_epoch(), new_epoch);
    }

    #[test]
    fn test_recover_epoch_without_free_proxy() {
        let mut store = MetaStore::default();
        let host_num = 2;
        let proxy_per_host = 1;
        let all_proxy_num = host_num * proxy_per_host;
        add_testing_proxies(&mut store, host_num, proxy_per_host);
        assert_eq!(store.get_free_proxies().len(), all_proxy_num);

        let cluster_name = CLUSTER_NAME.to_string();
        store.add_cluster(cluster_name.clone(), 4).unwrap();
        let cluster = store.get_cluster_by_name(&cluster_name, 1).unwrap();

        let new_epoch = cluster.get_epoch() + 1;
        store.bump_global_epoch();
        assert!(cluster.get_epoch() < store.get_global_epoch());

        store.recover_epoch(new_epoch);
        let cluster = store.get_cluster_by_name(&cluster_name, 1).unwrap();
        assert!(new_epoch <= store.get_global_epoch());
        assert_eq!(cluster.get_epoch(), store.get_global_epoch());
    }
}
