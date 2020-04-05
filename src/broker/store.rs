use super::persistence::MetaSyncError;
use crate::common::cluster::{
    Cluster, MigrationMeta, MigrationTaskMeta, Node, PeerProxy, Proxy, Range, RangeList, ReplMeta,
    ReplPeer, SlotRange, SlotRangeTag,
};
use crate::common::cluster::{ClusterName, Role};
use crate::common::config::ClusterConfig;
use crate::common::utils::SLOT_NUM;
use crate::common::version::UNDERMOON_MEM_BROKER_META_VERSION;
use chrono::{DateTime, NaiveDateTime, Utc};
use itertools::Itertools;
use serde::ser::{Serialize, SerializeStruct, Serializer};
use std::cmp::{min, Ordering};
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;

const NODES_PER_PROXY: usize = 2;
const CHUNK_PARTS: usize = 2;
pub const CHUNK_HALF_NODE_NUM: usize = 2;
const CHUNK_NODE_NUM: usize = 4;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProxyResource {
    pub proxy_address: String,
    pub node_addresses: [String; NODES_PER_PROXY],
    pub host: String,
    pub cluster: Option<ClusterName>,
}

struct HostProxy {
    pub host: String,
    pub proxy_address: String,
}

type ProxySlot = String;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
enum ChunkRolePosition {
    Normal,
    FirstChunkMaster,
    SecondChunkMaster,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
struct MigrationSlotRangeStore {
    range_list: RangeList,
    is_migrating: bool, // migrating or importing
    meta: MigrationMetaStore,
}

impl MigrationSlotRangeStore {
    fn to_slot_range(&self, chunks: &[ChunkStore]) -> SlotRange {
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
struct MigrationMetaStore {
    epoch: u64,
    src_chunk_index: usize,
    src_chunk_part: usize,
    dst_chunk_index: usize,
    dst_chunk_part: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ChunkStore {
    role_position: ChunkRolePosition,
    stable_slots: [Option<SlotRange>; CHUNK_PARTS],
    migrating_slots: [Vec<MigrationSlotRangeStore>; CHUNK_PARTS],
    proxy_addresses: [String; CHUNK_PARTS],
    hosts: [String; CHUNK_PARTS],
    node_addresses: [String; CHUNK_NODE_NUM],
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ClusterStore {
    epoch: u64,
    name: ClusterName,
    chunks: Vec<ChunkStore>,
    config: ClusterConfig,
}

impl ClusterStore {
    fn set_epoch(&mut self, new_epoch: u64) {
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
    fn limit_migration(&self, migration_limit: u64) -> ClusterStore {
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

#[derive(Debug)]
struct MigrationSlots {
    ranges: Vec<Range>,
    meta: MigrationMetaStore,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct MetaStore {
    version: String,
    global_epoch: u64,
    clusters: HashMap<ClusterName, ClusterStore>,
    // proxy_address => nodes and cluster_name
    all_proxies: HashMap<String, ProxyResource>,
    // proxy addresses
    failed_proxies: HashSet<String>,
    // failed_proxy_address => reporter_id => time,
    failures: HashMap<String, HashMap<String, i64>>,
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
        self.all_proxies.keys().cloned().collect()
    }

    fn get_cluster_store(
        clusters: &HashMap<ClusterName, ClusterStore>,
        cluster_name: &ClusterName,
        migration_limit: u64,
    ) -> Option<ClusterStore> {
        clusters
            .get(cluster_name)
            .map(|c| c.limit_migration(migration_limit))
    }

    pub fn get_proxy_by_address(&self, address: &str, migration_limit: u64) -> Option<Proxy> {
        let all_proxies = &self.all_proxies;
        let clusters = &self.clusters;

        let proxy_resource = all_proxies.get(address)?;
        let cluster_opt = proxy_resource
            .cluster
            .as_ref()
            .and_then(|name| Self::get_cluster_store(clusters, name, migration_limit));

        let cluster = match cluster_opt {
            Some(cluster_store) => Self::cluster_store_to_cluster(&cluster_store),
            None => {
                return Some(Proxy::new(
                    address.to_string(),
                    self.global_epoch,
                    vec![],
                    proxy_resource.node_addresses.to_vec(),
                    vec![],
                    HashMap::new(),
                ));
            }
        };

        let cluster_name = cluster.get_name().clone();
        // Both global epoch and cluster epoch should work.
        // But cluster epoch avoid updating the meta of this proxy
        // if only other clusters are changing.
        let epoch = cluster.get_epoch();
        let nodes: Vec<Node> = cluster
            .get_nodes()
            .iter()
            .filter(|node| node.get_proxy_address() == address)
            .cloned()
            .collect();

        let (peers, free_nodes) = if nodes.is_empty() {
            let free_nodes = proxy_resource.node_addresses.to_vec();
            (vec![], free_nodes)
        } else {
            let peers = cluster
                .get_nodes()
                .iter()
                .filter(|n| n.get_role() == Role::Master && n.get_proxy_address() != address)
                .cloned()
                .group_by(|node| node.get_proxy_address().to_string())
                .into_iter()
                .map(|(proxy_address, nodes)| {
                    // Collect all slots from masters.
                    let slots = nodes.map(Node::into_slots).flatten().collect();
                    PeerProxy {
                        proxy_address,
                        cluster_name: cluster_name.clone(),
                        slots,
                    }
                })
                .collect();
            (peers, vec![])
        };
        let proxy = Proxy::new(
            address.to_string(),
            epoch,
            nodes,
            free_nodes,
            peers,
            HashMap::new(),
        );
        Some(proxy)
    }

    pub fn get_cluster_names(&self) -> Vec<ClusterName> {
        self.clusters.keys().cloned().collect()
    }

    pub fn get_cluster_by_name(&self, cluster_name: &str, migration_limit: u64) -> Option<Cluster> {
        let cluster_name = ClusterName::try_from(cluster_name).ok()?;

        let cluster_store =
            Self::get_cluster_store(&self.clusters, &cluster_name, migration_limit)?;
        Some(Self::cluster_store_to_cluster(&cluster_store))
    }

    fn cluster_store_to_cluster(cluster_store: &ClusterStore) -> Cluster {
        let cluster_name = cluster_store.name.clone();

        let nodes = cluster_store
            .chunks
            .iter()
            .map(|chunk| {
                let mut nodes = vec![];
                for i in 0..CHUNK_NODE_NUM {
                    let address = chunk
                        .node_addresses
                        .get(i)
                        .expect("MetaStore::get_cluster_by_name: failed to get node")
                        .clone();
                    let proxy_address = chunk
                        .proxy_addresses
                        .get(i / 2)
                        .expect("MetaStore::get_cluster_by_name: failed to get proxy")
                        .clone();

                    // get slots
                    let mut slots = vec![];
                    let (first_slot_index, second_slot_index) = match chunk.role_position {
                        ChunkRolePosition::Normal => (0, 2),
                        ChunkRolePosition::FirstChunkMaster => (0, 1),
                        ChunkRolePosition::SecondChunkMaster => (2, 3),
                    };
                    if i == first_slot_index {
                        let mut first_slots = vec![];
                        if let Some(stable_slots) = &chunk.stable_slots[0] {
                            first_slots.push(stable_slots.clone());
                        }
                        slots.append(&mut first_slots);
                        let slot_ranges: Vec<_> = chunk.migrating_slots[0]
                            .iter()
                            .map(|slot_range_store| {
                                slot_range_store.to_slot_range(&cluster_store.chunks)
                            })
                            .collect();
                        slots.extend(slot_ranges);
                    }
                    if i == second_slot_index {
                        let mut second_slots = vec![];
                        if let Some(stable_slots) = &chunk.stable_slots[1] {
                            second_slots.push(stable_slots.clone());
                        }
                        slots.append(&mut second_slots);
                        let slot_ranges: Vec<_> = chunk.migrating_slots[1]
                            .iter()
                            .map(|slot_range_store| {
                                slot_range_store.to_slot_range(&cluster_store.chunks)
                            })
                            .collect();
                        slots.extend(slot_ranges);
                    }

                    // get repl
                    let mut role = Role::Master;
                    match chunk.role_position {
                        ChunkRolePosition::Normal if i % 2 == 1 => role = Role::Replica,
                        ChunkRolePosition::FirstChunkMaster if i >= CHUNK_HALF_NODE_NUM => {
                            role = Role::Replica
                        }
                        ChunkRolePosition::SecondChunkMaster if i < CHUNK_HALF_NODE_NUM => {
                            role = Role::Replica
                        }
                        _ => (),
                    }

                    let peer_index = match i {
                        0 => 3,
                        1 => 2,
                        2 => 1,
                        _ => 0,
                    };
                    let peer = ReplPeer {
                        node_address: chunk
                            .node_addresses
                            .get(peer_index)
                            .expect("MetaStore::get_cluster_by_name: failed to get peer node")
                            .clone(),
                        proxy_address: chunk
                            .proxy_addresses
                            .get(peer_index / 2)
                            .expect("MetaStore::get_cluster_by_name: failed to get peer proxy")
                            .clone(),
                    };
                    let repl = ReplMeta::new(role, vec![peer]);

                    let node = Node::new(address, proxy_address, cluster_name.clone(), slots, repl);
                    nodes.push(node);
                }
                nodes
            })
            .flatten()
            .collect();

        Cluster::new(
            cluster_store.name.clone(),
            cluster_store.epoch,
            nodes,
            cluster_store.config.clone(),
        )
    }

    pub fn add_failure(&mut self, address: String, reporter_id: String) {
        let now = Utc::now();
        self.bump_global_epoch();
        self.failures
            .entry(address)
            .or_insert_with(HashMap::new)
            .insert(reporter_id, now.timestamp());
    }

    pub fn get_failures(
        &mut self,
        falure_ttl: chrono::Duration,
        failure_quorum: u64,
    ) -> Vec<String> {
        let now = Utc::now();
        for reporter_map in self.failures.values_mut() {
            reporter_map.retain(|_, report_time| {
                let report_datetime =
                    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(*report_time, 0), Utc);
                now - report_datetime < falure_ttl
            });
        }
        self.failures
            .retain(|_, proxy_failure_map| !proxy_failure_map.is_empty());

        let all_proxies = &self.all_proxies;
        self.failures
            .iter()
            .filter(|(_, v)| v.len() >= failure_quorum as usize)
            .filter_map(|(address, _)| {
                if all_proxies.contains_key(address) {
                    Some(address.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn add_proxy(
        &mut self,
        proxy_address: String,
        nodes: [String; NODES_PER_PROXY],
        host: Option<String>,
    ) -> Result<(), MetaStoreError> {
        if proxy_address.split(':').count() != 2 {
            return Err(MetaStoreError::InvalidProxyAddress);
        }
        let host = match (host, proxy_address.split(':').next()) {
            (Some(h), _) => h,
            (None, Some(h)) => h.to_string(),
            (None, None) => return Err(MetaStoreError::InvalidProxyAddress),
        };

        self.bump_global_epoch();

        let exists = self.all_proxies.contains_key(&proxy_address);

        self.all_proxies
            .entry(proxy_address.clone())
            .or_insert_with(|| ProxyResource {
                proxy_address: proxy_address.clone(),
                node_addresses: nodes,
                host,
                cluster: None,
            });

        self.failed_proxies.remove(&proxy_address);
        self.failures.remove(&proxy_address);

        if !exists {
            Ok(())
        } else {
            Err(MetaStoreError::AlreadyExisted)
        }
    }

    pub fn add_cluster(
        &mut self,
        cluster_name: String,
        node_num: usize,
    ) -> Result<(), MetaStoreError> {
        let cluster_name = ClusterName::try_from(cluster_name.as_str())
            .map_err(|_| MetaStoreError::InvalidClusterName)?;
        if self.clusters.contains_key(&cluster_name) {
            return Err(MetaStoreError::AlreadyExisted);
        }

        if node_num % 4 != 0 {
            return Err(MetaStoreError::InvalidNodeNum);
        }
        let proxy_num =
            NonZeroUsize::new(node_num / 2).ok_or_else(|| MetaStoreError::InvalidNodeNum)?;

        let proxy_resource_arr = self.generate_free_chunks(proxy_num)?;
        let chunk_stores = Self::proxy_resource_to_chunk_store(proxy_resource_arr, true);

        let epoch = self.bump_global_epoch();

        let cluster_store = ClusterStore {
            epoch,
            name: cluster_name.clone(),
            chunks: chunk_stores,
            config: ClusterConfig::default(),
        };

        // Tag the proxies as occupied
        for chunk in cluster_store.chunks.iter() {
            for proxy_address in chunk.proxy_addresses.iter() {
                let proxy = self
                    .all_proxies
                    .get_mut(proxy_address)
                    .expect("add_cluster: failed to get back proxy");
                proxy.cluster = Some(cluster_name.clone());
            }
        }

        self.clusters.insert(cluster_name, cluster_store);
        Ok(())
    }

    fn proxy_resource_to_chunk_store(
        proxy_resource_arr: Vec<[ProxyResource; CHUNK_HALF_NODE_NUM]>,
        with_slots: bool,
    ) -> Vec<ChunkStore> {
        let master_num = proxy_resource_arr.len() * 2;
        let average = SLOT_NUM / master_num;
        let remainder = SLOT_NUM - average * master_num;
        let mut chunk_stores = vec![];
        let mut curr_slot = 0;
        for (i, chunk) in proxy_resource_arr.into_iter().enumerate() {
            let a = 2 * i;
            let b = a + 1;

            let mut create_slots = |index| {
                let r = (index < remainder) as usize;
                let start = curr_slot;
                let end = curr_slot + average + r;
                curr_slot = end;
                SlotRange {
                    range_list: RangeList::from_single_range(Range(start, end - 1)),
                    tag: SlotRangeTag::None,
                }
            };

            let stable_slots = if with_slots {
                [Some(create_slots(a)), Some(create_slots(b))]
            } else {
                [None, None]
            };

            let first_proxy = chunk[0].clone();
            let second_proxy = chunk[1].clone();
            let chunk_store = ChunkStore {
                role_position: ChunkRolePosition::Normal,
                stable_slots,
                migrating_slots: [vec![], vec![]],
                proxy_addresses: [
                    first_proxy.proxy_address.clone(),
                    second_proxy.proxy_address.clone(),
                ],
                hosts: [first_proxy.host.clone(), second_proxy.host.clone()],
                node_addresses: [
                    first_proxy.node_addresses[0].clone(),
                    first_proxy.node_addresses[1].clone(),
                    second_proxy.node_addresses[0].clone(),
                    second_proxy.node_addresses[1].clone(),
                ],
            };
            chunk_stores.push(chunk_store);
        }
        chunk_stores
    }

    pub fn remove_cluster(&mut self, cluster_name: String) -> Result<(), MetaStoreError> {
        let cluster_name = ClusterName::try_from(cluster_name.as_str())
            .map_err(|_| MetaStoreError::InvalidClusterName)?;

        let cluster_store = match self.clusters.remove(&cluster_name) {
            None => return Err(MetaStoreError::ClusterNotFound),
            Some(cluster_store) => cluster_store,
        };

        // Set proxies free.
        for chunk in cluster_store.chunks.iter() {
            for proxy_address in chunk.proxy_addresses.iter() {
                if let Some(proxy) = self.all_proxies.get_mut(proxy_address) {
                    proxy.cluster = None;
                }
            }
        }

        self.bump_global_epoch();
        Ok(())
    }

    pub fn auto_add_nodes(
        &mut self,
        cluster_name: String,
        num: usize,
    ) -> Result<Vec<Node>, MetaStoreError> {
        let cluster_name = ClusterName::try_from(cluster_name.as_str())
            .map_err(|_| MetaStoreError::InvalidClusterName)?;

        match self.clusters.get(&cluster_name) {
            None => return Err(MetaStoreError::ClusterNotFound),
            Some(cluster) => {
                if cluster
                    .chunks
                    .iter()
                    .any(|chunk| chunk.migrating_slots.iter().any(|slots| !slots.is_empty()))
                {
                    return Err(MetaStoreError::MigrationRunning);
                }
            }
        };

        if num % 4 != 0 {
            return Err(MetaStoreError::InvalidNodeNum);
        }
        let proxy_num = NonZeroUsize::new(num / 2).ok_or_else(|| MetaStoreError::InvalidNodeNum)?;

        let proxy_resource_arr = self.generate_free_chunks(proxy_num)?;
        let mut chunks = Self::proxy_resource_to_chunk_store(proxy_resource_arr, false);

        let new_epoch = self.bump_global_epoch();

        let cluster = match self.clusters.get_mut(&cluster_name) {
            None => return Err(MetaStoreError::ClusterNotFound),
            Some(cluster_store) => {
                cluster_store.chunks.append(&mut chunks);
                cluster_store.epoch = new_epoch;
                Self::cluster_store_to_cluster(&cluster_store)
            }
        };

        // Tag the proxies as occupied
        for node in cluster.get_nodes().iter() {
            let proxy_address = node.get_proxy_address();
            let proxy = self
                .all_proxies
                .get_mut(proxy_address)
                .expect("add_cluster: failed to get back proxy");
            proxy.cluster = Some(cluster_name.clone());
        }

        let nodes = cluster.get_nodes();
        let new_nodes = nodes
            .get((nodes.len() - num)..)
            .expect("auto_add_nodes: get nodes")
            .to_vec();

        Ok(new_nodes)
    }

    pub fn audo_delete_free_nodes(&mut self, cluster_name: String) -> Result<(), MetaStoreError> {
        let cluster_name = ClusterName::try_from(cluster_name.as_str())
            .map_err(|_| MetaStoreError::InvalidClusterName)?;
        let new_epoch = self.bump_global_epoch();

        let removed_chunks = match self.clusters.get_mut(&cluster_name) {
            None => return Err(MetaStoreError::ClusterNotFound),
            Some(cluster) => {
                if cluster
                    .chunks
                    .iter()
                    .any(|chunk| chunk.migrating_slots.iter().any(|slots| !slots.is_empty()))
                {
                    return Err(MetaStoreError::MigrationRunning);
                }

                let mut removed_chunks = vec![];
                cluster.chunks.retain(|chunk| {
                    for slots in chunk.stable_slots.iter() {
                        if slots.is_some() {
                            return true;
                        }
                    }
                    for slots in chunk.migrating_slots.iter() {
                        if !slots.is_empty() {
                            return true;
                        }
                    }
                    removed_chunks.push(chunk.clone());
                    false
                });
                if removed_chunks.is_empty() {
                    return Err(MetaStoreError::FreeNodeNotFound);
                }

                cluster.set_epoch(new_epoch);
                removed_chunks
            }
        };

        // Set proxies free
        for chunk in removed_chunks.into_iter() {
            for proxy_address in chunk.proxy_addresses.iter() {
                if let Some(proxy) = self.all_proxies.get_mut(proxy_address) {
                    proxy.cluster = None;
                }
            }
        }

        Ok(())
    }

    pub fn remove_proxy(&mut self, proxy_address: String) -> Result<(), MetaStoreError> {
        match self.all_proxies.get(&proxy_address) {
            None => return Err(MetaStoreError::ProxyNotFound),
            Some(proxy) => {
                if proxy.cluster.is_some() {
                    return Err(MetaStoreError::InUse);
                }
            }
        }

        self.all_proxies.remove(&proxy_address);
        self.failed_proxies.remove(&proxy_address);
        self.failures.remove(&proxy_address);
        self.bump_global_epoch();
        Ok(())
    }

    pub fn migrate_slots(&mut self, cluster_name: String) -> Result<(), MetaStoreError> {
        let cluster_name = ClusterName::try_from(cluster_name.as_str())
            .map_err(|_| MetaStoreError::InvalidClusterName)?;
        let new_epoch = self.bump_global_epoch();

        let cluster = match self.clusters.get_mut(&cluster_name) {
            None => return Err(MetaStoreError::ClusterNotFound),
            Some(cluster) => cluster,
        };

        let empty_exists = cluster
            .chunks
            .iter()
            .any(|chunk| chunk.stable_slots.iter().any(|slots| slots.is_none()));
        if !empty_exists {
            return Err(MetaStoreError::SlotsAlreadyEven);
        }

        let running_migration = cluster
            .chunks
            .iter()
            .any(|chunk| !chunk.migrating_slots.iter().any(|slots| slots.is_empty()));
        if running_migration {
            return Err(MetaStoreError::MigrationRunning);
        }

        let migration_slots = Self::remove_slots_from_src(cluster, new_epoch);
        Self::assign_dst_slots(cluster, migration_slots);
        cluster.set_epoch(new_epoch);

        Ok(())
    }

    fn remove_slots_from_src(cluster: &mut ClusterStore, epoch: u64) -> Vec<MigrationSlots> {
        let dst_chunk_num = cluster
            .chunks
            .iter()
            .filter(|chunk| chunk.stable_slots[0].is_none() && chunk.stable_slots[1].is_none())
            .count();
        let dst_master_num = dst_chunk_num * 2;
        let master_num = cluster.chunks.len() * 2;
        let src_chunk_num = cluster.chunks.len() - dst_chunk_num;
        let src_master_num = src_chunk_num * 2;
        let average = SLOT_NUM / master_num;
        let remainder = SLOT_NUM - average * master_num;

        let mut curr_dst_master_index = 0;
        let mut migration_slots = vec![];
        let mut curr_dst_slots = vec![];
        let mut curr_slots_num = 0;

        for (src_chunk_index, src_chunk) in cluster.chunks.iter_mut().enumerate() {
            for (src_chunk_part, slot_range) in src_chunk.stable_slots.iter_mut().enumerate() {
                if let Some(slot_range) = slot_range {
                    while curr_dst_master_index != dst_master_num {
                        let src_master_index = src_chunk_index * 2 + src_chunk_part;
                        let src_r = (src_master_index < remainder) as usize; // true will be 1, false will be 0
                        let dst_master_index = src_master_num + curr_dst_master_index;
                        let dst_r = (dst_master_index < remainder) as usize; // true will be 1, false will be 0
                        let src_final_num = average + src_r;
                        let dst_final_num = average + dst_r;

                        if slot_range.get_range_list().get_slots_num() <= src_final_num {
                            break;
                        }

                        let need_num = dst_final_num - curr_slots_num;
                        let available_num =
                            slot_range.get_range_list().get_slots_num() - src_final_num;
                        let remove_num = min(need_num, available_num);
                        let num = slot_range
                            .get_range_list()
                            .get_ranges()
                            .last()
                            .map(|r| r.end() - r.start() + 1)
                            .expect("remove_slots_from_src: slots > average + src_r >= 0");

                        if remove_num >= num {
                            let range = slot_range
                                .get_mut_range_list()
                                .get_mut_ranges()
                                .pop()
                                .expect("remove_slots_from_src: need_num >= num");
                            curr_dst_slots.push(range);
                            curr_slots_num += num;
                        } else {
                            let range = slot_range
                                .get_mut_range_list()
                                .get_mut_ranges()
                                .last_mut()
                                .expect("remove_slots_from_src");
                            let end = range.end();
                            let start = end - remove_num + 1;
                            *range.end_mut() -= remove_num;
                            curr_dst_slots.push(Range(start, end));
                            curr_slots_num += remove_num;
                        }

                        // reset current state
                        if curr_slots_num >= dst_final_num
                            || slot_range.get_range_list().get_slots_num() <= src_final_num
                        {
                            // assert curr_dst_slots.is_not_empty()
                            migration_slots.push(MigrationSlots {
                                meta: MigrationMetaStore {
                                    epoch,
                                    src_chunk_index,
                                    src_chunk_part,
                                    dst_chunk_index: src_chunk_num + (curr_dst_master_index / 2),
                                    dst_chunk_part: curr_dst_master_index % 2,
                                },
                                ranges: curr_dst_slots.drain(..).collect(),
                            });
                            if curr_slots_num >= dst_final_num {
                                curr_dst_master_index += 1;
                                curr_slots_num = 0;
                            }
                            if slot_range.get_range_list().get_slots_num() <= src_final_num {
                                break;
                            }
                        }
                    }
                }
            }
        }

        migration_slots
    }

    fn assign_dst_slots(cluster: &mut ClusterStore, migration_slots: Vec<MigrationSlots>) {
        for migration_slot_range in migration_slots.into_iter() {
            let MigrationSlots { ranges, meta } = migration_slot_range;

            {
                let src_chunk = cluster
                    .chunks
                    .get_mut(meta.src_chunk_index)
                    .expect("assign_dst_slots");
                let migrating_slots = src_chunk
                    .migrating_slots
                    .get_mut(meta.src_chunk_part)
                    .expect("assign_dst_slots");
                let slot_range = MigrationSlotRangeStore {
                    range_list: RangeList::new(ranges.clone()),
                    is_migrating: true,
                    meta: meta.clone(),
                };
                migrating_slots.push(slot_range);
            }
            {
                let dst_chunk = cluster
                    .chunks
                    .get_mut(meta.dst_chunk_index)
                    .expect("assign_dst_slots");
                let migrating_slots = dst_chunk
                    .migrating_slots
                    .get_mut(meta.dst_chunk_part)
                    .expect("assign_dst_slots");
                let slot_range = MigrationSlotRangeStore {
                    range_list: RangeList::new(ranges.clone()),
                    is_migrating: false,
                    meta,
                };
                migrating_slots.push(slot_range);
            }
        }
    }

    pub fn commit_migration(&mut self, task: MigrationTaskMeta) -> Result<(), MetaStoreError> {
        let new_epoch = self.bump_global_epoch();

        let cluster_name = task.cluster_name.clone();
        let cluster = self
            .clusters
            .get_mut(&cluster_name)
            .ok_or_else(|| MetaStoreError::ClusterNotFound)?;
        let task_epoch = match &task.slot_range.tag {
            SlotRangeTag::None => return Err(MetaStoreError::InvalidMigrationTask),
            SlotRangeTag::Migrating(meta) => meta.epoch,
            SlotRangeTag::Importing(meta) => meta.epoch,
        };

        let (src_chunk_index, src_chunk_part) = cluster
            .chunks
            .iter()
            .enumerate()
            .flat_map(|(i, chunk)| {
                chunk
                    .migrating_slots
                    .iter()
                    .enumerate()
                    .map(move |(j, slot_range_stores)| (i, j, slot_range_stores))
            })
            .flat_map(|(i, j, slot_range_stores)| {
                slot_range_stores
                    .iter()
                    .map(move |slot_range_store| (i, j, slot_range_store))
            })
            .find(|(_, _, slot_range_store)| {
                slot_range_store.range_list == task.slot_range.range_list
                    && slot_range_store.meta.epoch == task_epoch
                    && slot_range_store.is_migrating
            })
            .map(|(i, j, _)| (i, j))
            .ok_or_else(|| MetaStoreError::MigrationTaskNotFound)?;

        let (dst_chunk_index, dst_chunk_part) = cluster
            .chunks
            .iter()
            .enumerate()
            .flat_map(|(i, chunk)| {
                chunk
                    .migrating_slots
                    .iter()
                    .enumerate()
                    .map(move |(j, slot_range_stores)| (i, j, slot_range_stores))
            })
            .flat_map(|(i, j, slot_range_stores)| {
                slot_range_stores
                    .iter()
                    .map(move |slot_range_store| (i, j, slot_range_store))
            })
            .find(|(_, _, slot_range_store)| {
                slot_range_store.range_list == task.slot_range.range_list
                    && slot_range_store.meta.epoch == task_epoch
                    && !slot_range_store.is_migrating
            })
            .map(|(i, j, _)| (i, j))
            .ok_or_else(|| MetaStoreError::MigrationTaskNotFound)?;

        let meta = MigrationMetaStore {
            epoch: task_epoch,
            src_chunk_index,
            src_chunk_part,
            dst_chunk_index,
            dst_chunk_part,
        };

        for chunk in &mut cluster.chunks {
            for migrating_slots in chunk.migrating_slots.iter_mut() {
                migrating_slots.retain(|slot_range_store| {
                    !(slot_range_store.is_migrating
                        && slot_range_store.range_list == task.slot_range.range_list
                        && slot_range_store.meta == meta)
                })
            }
        }

        for chunk in &mut cluster.chunks {
            let removed_slots =
                chunk
                    .migrating_slots
                    .iter_mut()
                    .enumerate()
                    .find_map(|(j, migrating_slots)| {
                        migrating_slots
                            .iter()
                            .position(|slot_range_store| {
                                !slot_range_store.is_migrating
                                    && slot_range_store.meta == meta
                                    && slot_range_store.range_list == task.slot_range.range_list
                            })
                            .map(|index| (j, migrating_slots.remove(index).range_list))
                    });
            if let Some((j, mut range_list)) = removed_slots {
                match chunk.stable_slots.get_mut(j).expect("commit_migration") {
                    Some(stable_slots) => {
                        stable_slots
                            .get_mut_range_list()
                            .merge_another(&mut range_list);
                    }
                    stable_slots => {
                        let slot_range = SlotRange {
                            range_list,
                            tag: SlotRangeTag::None,
                        };
                        *stable_slots = Some(slot_range);
                    }
                }
                break;
            }
        }

        cluster.set_epoch(new_epoch);
        Ok(())
    }

    fn get_free_proxies(&self) -> Vec<HostProxy> {
        let failed_proxies = self.failed_proxies.clone();
        let failures = self.failures.clone();

        let mut free_proxies = vec![];
        for proxy_resource in self.all_proxies.values() {
            if proxy_resource.cluster.is_some() {
                continue;
            }
            let proxy_address = &proxy_resource.proxy_address;
            if failed_proxies.contains(proxy_address) {
                continue;
            }
            if failures.contains_key(proxy_address) {
                continue;
            }
            free_proxies.push(HostProxy {
                host: proxy_resource.host.clone(),
                proxy_address: proxy_address.clone(),
            });
        }
        free_proxies
    }

    fn build_link_table(&self) -> HashMap<String, HashMap<String, usize>> {
        // Remove the fully occupied hosts or it will be severe performance problems.
        let free_hosts: HashSet<String> = self
            .all_proxies
            .values()
            .filter_map(|proxy| {
                if proxy.cluster.is_none() {
                    Some(proxy.host.clone())
                } else {
                    None
                }
            })
            .collect();

        let mut link_table: HashMap<String, HashMap<String, usize>> = HashMap::new();
        for proxy_resource in self.all_proxies.values() {
            let first_host = proxy_resource.host.clone();

            for proxy_resource in self.all_proxies.values() {
                let second_host = proxy_resource.host.clone();
                if first_host == second_host {
                    continue;
                }
                if !free_hosts.contains(&first_host) && !free_hosts.contains(&second_host) {
                    continue;
                }

                link_table
                    .entry(first_host.clone())
                    .or_insert_with(HashMap::new)
                    .entry(second_host.clone())
                    .or_insert(0);
                link_table
                    .entry(second_host.clone())
                    .or_insert_with(HashMap::new)
                    .entry(first_host.clone())
                    .or_insert(0);
            }
        }

        for cluster in self.clusters.values() {
            for chunk in cluster.chunks.iter() {
                let first_host = chunk.hosts[0].clone();
                let second_host = chunk.hosts[1].clone();
                let linked_num = link_table
                    .entry(first_host.clone())
                    .or_insert_with(HashMap::new)
                    .entry(second_host.clone())
                    .or_insert(0);
                *linked_num += 1;
                let linked_num = link_table
                    .entry(second_host)
                    .or_insert_with(HashMap::new)
                    .entry(first_host)
                    .or_insert(0);
                *linked_num += 1;
            }
        }
        link_table
    }

    fn generate_free_host_proxies(&self) -> HashMap<String, Vec<ProxySlot>> {
        // host => proxies
        let mut host_proxies: HashMap<String, Vec<ProxySlot>> = HashMap::new();
        for host_proxy in self.get_free_proxies().into_iter() {
            let HostProxy {
                host,
                proxy_address,
            } = host_proxy;
            host_proxies
                .entry(host)
                .or_insert_with(Vec::new)
                .push(proxy_address);
        }
        host_proxies
    }

    fn generate_free_chunks(
        &self,
        proxy_num: NonZeroUsize,
    ) -> Result<Vec<[ProxyResource; CHUNK_HALF_NODE_NUM]>, MetaStoreError> {
        let mut host_proxies = self.generate_free_host_proxies();

        host_proxies = Self::remove_redundant_chunks(host_proxies, proxy_num)?;

        let link_table = self.build_link_table();

        let new_added_proxy_resource = Self::allocate_chunk(host_proxies, link_table, proxy_num)?;
        let new_proxies = new_added_proxy_resource
            .into_iter()
            .map(|[a, b]| {
                [
                    self.all_proxies
                        .get(&a)
                        .expect("consume_proxy: get proxy resource")
                        .clone(),
                    self.all_proxies
                        .get(&b)
                        .expect("consume_proxy: get proxy resource")
                        .clone(),
                ]
            })
            .collect();
        Ok(new_proxies)
    }

    fn remove_redundant_chunks(
        mut host_proxies: HashMap<String, Vec<ProxySlot>>,
        expected_num: NonZeroUsize,
    ) -> Result<HashMap<String, Vec<ProxySlot>>, MetaStoreError> {
        let mut free_proxy_num: usize = host_proxies.values().map(|proxies| proxies.len()).sum();
        let mut max_proxy_num = host_proxies
            .values()
            .map(|proxies| proxies.len())
            .max()
            .unwrap_or(0);

        for proxies in host_proxies.values_mut() {
            if proxies.len() == max_proxy_num {
                // Only remove proxies in the host which as too many proxies.
                while max_proxy_num * 2 > free_proxy_num {
                    proxies.pop();
                    free_proxy_num -= 1;
                    max_proxy_num -= 1;
                }
                break;
            }
        }

        if free_proxy_num < expected_num.get() {
            return Err(MetaStoreError::NoAvailableResource);
        }
        Ok(host_proxies)
    }

    fn allocate_chunk(
        mut host_proxies: HashMap<String, Vec<ProxySlot>>,
        mut link_table: HashMap<String, HashMap<String, usize>>,
        expected_num: NonZeroUsize,
    ) -> Result<Vec<[String; CHUNK_HALF_NODE_NUM]>, MetaStoreError> {
        let max_proxy_num = host_proxies
            .values()
            .map(|proxies| proxies.len())
            .max()
            .unwrap_or(0);
        let sum_proxy_num = host_proxies.values().map(|proxies| proxies.len()).sum();

        if sum_proxy_num < expected_num.get() {
            return Err(MetaStoreError::NoAvailableResource);
        }

        if max_proxy_num * 2 > sum_proxy_num {
            return Err(MetaStoreError::ResourceNotBalance);
        }

        let mut new_proxy_pairs = vec![];
        while new_proxy_pairs.len() * 2 < expected_num.get() {
            let (first_host, first_address) = {
                let (max_host, max_proxy_host) = host_proxies
                    .iter_mut()
                    .max_by_key(|(_host, proxies)| proxies.len())
                    .expect("allocate_chunk: invalid state. cannot find any host");
                (
                    max_host.clone(),
                    max_proxy_host
                        .pop()
                        .expect("allocate_chunk: cannot find free proxy"),
                )
            };

            let (second_host, second_address) = {
                let peers = link_table
                    .get(&first_host)
                    .expect("allocate_chunk: invalid state, cannot get link table entry");

                let second_host = peers
                    .iter()
                    .filter(|(host, _)| {
                        let free_count = host_proxies.get(*host).map(|proxies| proxies.len());
                        **host != first_host && free_count != None && free_count != Some(0)
                    })
                    .min_by(|(host1, count1), (host2, count2)| {
                        Self::second_host_cmp(
                            host1.as_str(),
                            **count1,
                            host2.as_str(),
                            **count2,
                            &host_proxies,
                        )
                    })
                    .map(|t| t.0.clone())
                    .expect("allocate_chunk: invalid state, cannot get free proxy");

                let second_address = host_proxies
                    .get_mut(&second_host)
                    .expect("allocate_chunk: get second host")
                    .pop()
                    .expect("allocate_chunk: get second address");
                (second_host, second_address)
            };

            *link_table
                .get_mut(&first_host)
                .expect("allocate_chunk: link table")
                .get_mut(&second_host)
                .expect("allocate_chunk: link table") += 1;
            *link_table
                .get_mut(&second_host)
                .expect("allocate_chunk: link table")
                .get_mut(&first_host)
                .expect("allocate_chunk: link table") += 1;

            new_proxy_pairs.push([first_address, second_address]);
        }

        Ok(new_proxy_pairs)
    }

    fn second_host_cmp(
        host1: &str,
        count1: usize,
        host2: &str,
        count2: usize,
        free_host_proxies: &HashMap<String, Vec<ProxySlot>>,
    ) -> Ordering {
        let r = count1.cmp(&count2);
        if r != Ordering::Equal {
            return r;
        }
        let host1_free = free_host_proxies
            .get(host1)
            .map(|proxies| proxies.len())
            .expect("second_host_cmp: get back host");
        let host2_free = free_host_proxies
            .get(host2)
            .map(|proxies| proxies.len())
            .expect("second_host_cmp: get back host");
        // Need to reverse it as we want the host with maximum proxies.
        host2_free.cmp(&host1_free)
    }

    pub fn replace_failed_proxy(
        &mut self,
        failed_proxy_address: String,
        migration_limit: u64,
    ) -> Result<Option<Proxy>, MetaStoreError> {
        let cluster_name = match self.all_proxies.get(&failed_proxy_address) {
            None => return Err(MetaStoreError::ProxyNotFound),
            Some(proxy) => proxy.cluster.clone(),
        };

        let cluster_name = match cluster_name {
            None => {
                self.failed_proxies.insert(failed_proxy_address);
                return Ok(None);
            }
            Some(cluster_name) => cluster_name,
        };

        self.takeover_master(&cluster_name, failed_proxy_address.clone())?;

        self.failed_proxies.insert(failed_proxy_address.clone());

        let proxy_resource = self.generate_new_free_proxy(failed_proxy_address.clone())?;
        let new_epoch = self.bump_global_epoch();
        {
            let cluster = self
                .clusters
                .get_mut(&cluster_name)
                .expect("replace_failed_proxy: get cluster");
            for chunk in cluster.chunks.iter_mut() {
                if chunk.proxy_addresses[0] == failed_proxy_address {
                    chunk.proxy_addresses[0] = proxy_resource.proxy_address.clone();
                    chunk.node_addresses[0] = proxy_resource.node_addresses[0].clone();
                    chunk.node_addresses[1] = proxy_resource.node_addresses[1].clone();
                    break;
                } else if chunk.proxy_addresses[1] == failed_proxy_address {
                    chunk.proxy_addresses[1] = proxy_resource.proxy_address.clone();
                    chunk.node_addresses[2] = proxy_resource.node_addresses[0].clone();
                    chunk.node_addresses[3] = proxy_resource.node_addresses[1].clone();
                    break;
                }
            }
            cluster.set_epoch(new_epoch);
        }

        // Set this proxy free
        if let Some(proxy) = self.all_proxies.get_mut(&failed_proxy_address) {
            proxy.cluster = None;
        }
        // Tag the new proxy as occupied
        if let Some(proxy) = self.all_proxies.get_mut(&proxy_resource.proxy_address) {
            proxy.cluster = Some(cluster_name);
        }

        let proxy = self
            .get_proxy_by_address(&proxy_resource.proxy_address, migration_limit)
            .expect("replace_failed_proxy");
        Ok(Some(proxy))
    }

    fn takeover_master(
        &mut self,
        cluster_name: &ClusterName,
        failed_proxy_address: String,
    ) -> Result<(), MetaStoreError> {
        let new_epoch = self.bump_global_epoch();

        let cluster = self
            .clusters
            .get_mut(cluster_name)
            .ok_or_else(|| MetaStoreError::ClusterNotFound)?;

        let mut peer_position = HashSet::new();

        for chunk in cluster.chunks.iter_mut() {
            if chunk.proxy_addresses[0] == failed_proxy_address {
                chunk.role_position = ChunkRolePosition::SecondChunkMaster;

                for migrating_slot_range in chunk.migrating_slots[0].iter_mut() {
                    migrating_slot_range.meta.epoch = new_epoch;
                    peer_position.insert((
                        migrating_slot_range.meta.src_chunk_index,
                        migrating_slot_range.meta.src_chunk_part,
                    ));
                    peer_position.insert((
                        migrating_slot_range.meta.dst_chunk_index,
                        migrating_slot_range.meta.dst_chunk_part,
                    ));
                }
                break;
            } else if chunk.proxy_addresses[1] == failed_proxy_address {
                chunk.role_position = ChunkRolePosition::FirstChunkMaster;

                for migrating_slot_range in chunk.migrating_slots[1].iter_mut() {
                    migrating_slot_range.meta.epoch = new_epoch;
                    peer_position.insert((
                        migrating_slot_range.meta.src_chunk_index,
                        migrating_slot_range.meta.src_chunk_part,
                    ));
                    peer_position.insert((
                        migrating_slot_range.meta.dst_chunk_index,
                        migrating_slot_range.meta.dst_chunk_part,
                    ));
                }
                break;
            }
        }

        for chunk in cluster.chunks.iter_mut() {
            for migrating_slots in chunk.migrating_slots.iter_mut() {
                for migrating_slot_range in migrating_slots.iter_mut() {
                    let src_index = migrating_slot_range.meta.src_chunk_index;
                    let src_part = migrating_slot_range.meta.src_chunk_part;
                    let dst_index = migrating_slot_range.meta.dst_chunk_index;
                    let dst_part = migrating_slot_range.meta.dst_chunk_part;
                    if peer_position.contains(&(src_index, src_part))
                        || peer_position.contains(&(dst_index, dst_part))
                    {
                        migrating_slot_range.meta.epoch = new_epoch;
                    }
                }
            }
        }
        cluster.epoch = new_epoch;
        Ok(())
    }

    fn generate_new_free_proxy(
        &self,
        failed_proxy_address: String,
    ) -> Result<ProxyResource, MetaStoreError> {
        let free_host_proxies = self.generate_free_host_proxies();
        info!(
            "generate_new_free_proxy: free host proxies {:?}",
            free_host_proxies
        );
        let link_table = self.build_link_table();
        info!("generate_new_free_proxy: link table {:?}", link_table);

        let failed_proxy_host = self
            .all_proxies
            .get(&failed_proxy_address)
            .ok_or_else(|| MetaStoreError::ProxyNotFound)?
            .host
            .clone();

        let link_count_table = link_table
            .get(&failed_proxy_host)
            .expect("consume_new_proxy: cannot find failed proxy");
        let peer_host = link_count_table
            .iter()
            .filter(|(peer_host, _)| free_host_proxies.contains_key(*peer_host))
            .min_by(|(host1, count1), (host2, count2)| {
                Self::second_host_cmp(
                    host1.as_str(),
                    **count1,
                    host2.as_str(),
                    **count2,
                    &free_host_proxies,
                )
            })
            .map(|(peer_address, _)| peer_address)
            .ok_or_else(|| MetaStoreError::NoAvailableResource)?;

        let peer_proxy = self
            .get_free_proxies()
            .iter()
            .find(|host_proxy| peer_host == &host_proxy.host)
            .expect("consume_new_proxy: get peer address")
            .proxy_address
            .clone();

        let new_proxy = self
            .all_proxies
            .get(&peer_proxy)
            .expect("consume_new_proxy: cannot find peer proxy")
            .clone();
        Ok(new_proxy)
    }

    pub fn change_config(
        &mut self,
        cluster_name: String,
        config: HashMap<String, String>,
    ) -> Result<(), MetaStoreError> {
        let cluster_name = ClusterName::try_from(cluster_name.as_str())
            .map_err(|_| MetaStoreError::InvalidClusterName)?;
        let new_epoch = self.bump_global_epoch();
        match self.clusters.get_mut(&cluster_name) {
            None => return Err(MetaStoreError::ClusterNotFound),
            Some(ref mut cluster) => {
                let mut cluster_config = cluster.config.clone();
                for (k, v) in config.iter() {
                    cluster_config.set_field(k, v).map_err(|err| {
                        MetaStoreError::InvalidConfig {
                            key: k.clone(),
                            value: v.clone(),
                            error: err.to_string(),
                        }
                    })?;
                }
                cluster.config = cluster_config;
                cluster.set_epoch(new_epoch);
            }
        }

        Ok(())
    }

    pub fn balance_masters(&mut self, cluster_name: String) -> Result<(), MetaStoreError> {
        let cluster_name = ClusterName::try_from(cluster_name.as_str())
            .map_err(|_| MetaStoreError::InvalidClusterName)?;
        let new_epoch = self.bump_global_epoch();

        let failed_proxies = &self.failed_proxies;
        let failures = &self.failures;

        let failed_proxy_exists = |addresses: &[String; CHUNK_PARTS]| -> bool {
            for address in addresses.iter() {
                if failed_proxies.contains(address) || failures.contains_key(address) {
                    return true;
                }
            }
            false
        };

        match self.clusters.get_mut(&cluster_name) {
            None => return Err(MetaStoreError::ClusterNotFound),
            Some(ref mut cluster) => {
                for chunk in cluster.chunks.iter_mut() {
                    if failed_proxy_exists(&chunk.proxy_addresses) {
                        continue;
                    }
                    chunk.role_position = ChunkRolePosition::Normal;
                }
                cluster.set_epoch(new_epoch);
            }
        }

        Ok(())
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
            for chunk in cluster.chunks.iter_mut() {
                for migrating_slots in chunk.migrating_slots.iter_mut() {
                    for slots in migrating_slots.iter_mut() {
                        slots.meta.epoch = new_epoch;
                    }
                }
            }
        }
        Ok(())
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
    use crate::common::config::CompressionStrategy;

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
                    // no limit
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
    fn test_flatten_machines() {
        let host_num = 6;
        let proxy_per_host = 1;
        let migration_limit = 2;
        let added_node_num = 4;

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
}
