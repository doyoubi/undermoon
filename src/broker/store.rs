use crate::common::cluster::{
    Cluster, MigrationMeta, MigrationTaskMeta, Node, PeerProxy, Proxy, Range, RangeList, ReplMeta,
    ReplPeer, SlotRange, SlotRangeTag,
};
use crate::common::cluster::{DBName, Role};
use crate::common::config::ClusterConfig;
use crate::common::utils::SLOT_NUM;
use chrono::{DateTime, NaiveDateTime, Utc};
use itertools::Itertools;
use std::cmp;
use std::collections::{HashMap, HashSet};
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
}

type ProxySlot = String;

#[derive(Clone, Deserialize, Serialize)]
enum ChunkRolePosition {
    Normal,
    FirstChunkMaster,
    SecondChunkMaster,
}

#[derive(Clone, Deserialize, Serialize)]
struct MigrationSlotRangeStore {
    range_list: RangeList,
    is_migrating: bool, // migrating or importing
    meta: MigrationMetaStore,
}

impl MigrationSlotRangeStore {
    fn to_slot_range(&self, epoch: u64, chunks: &[ChunkStore]) -> SlotRange {
        let src_chunk = chunks.get(self.meta.src_chunk_index).expect("get_cluster");
        let src_proxy_address = src_chunk
            .proxy_addresses
            .get(self.meta.src_chunk_part)
            .expect("get_cluster")
            .clone();
        let src_node_address = src_chunk
            .node_addresses
            .get(self.meta.src_chunk_index * 2 + self.meta.src_chunk_part)
            .expect("get_cluster")
            .clone();
        let dst_chunk = chunks.get(self.meta.dst_chunk_index).expect("get_cluster");
        let dst_proxy_address = dst_chunk
            .proxy_addresses
            .get(self.meta.dst_chunk_part)
            .expect("get_cluster")
            .clone();
        let dst_node_address = dst_chunk
            .node_addresses
            .get(self.meta.dst_chunk_index * 2 + self.meta.dst_chunk_part)
            .expect("get_cluster")
            .clone();
        let meta = MigrationMeta {
            epoch,
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
}

#[derive(Clone, Deserialize, Serialize)]
struct MigrationMetaStore {
    src_chunk_index: usize,
    src_chunk_part: usize,
    dst_chunk_index: usize,
    dst_chunk_part: usize,
}

#[derive(Clone, Deserialize, Serialize)]
struct ChunkStore {
    role_position: ChunkRolePosition,
    stable_slots: Option<[SlotRange; CHUNK_PARTS]>,
    migrating_slots: [Vec<MigrationSlotRangeStore>; CHUNK_PARTS],
    proxy_addresses: [String; CHUNK_PARTS],
    node_addresses: [String; CHUNK_NODE_NUM],
}

#[derive(Clone, Deserialize, Serialize)]
struct ClusterStore {
    name: DBName,
    chunks: Vec<ChunkStore>,
    config: ClusterConfig,
}

struct MigrationSlots {
    ranges: Vec<Range>,
    meta: MigrationMetaStore,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct MetaStore {
    global_epoch: u64,
    cluster: Option<ClusterStore>,
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
            global_epoch: 0,
            cluster: None,
            all_proxies: HashMap::new(),
            failed_proxies: HashSet::new(),
            failures: HashMap::new(),
        }
    }
}

impl MetaStore {
    pub fn bump_global_epoch(&mut self) -> u64 {
        self.global_epoch += 1;
        self.global_epoch
    }

    pub fn get_hosts(&self) -> Vec<String> {
        self.all_proxies.keys().cloned().collect()
    }

    pub fn get_host_by_address(&self, address: &str) -> Option<Proxy> {
        let all_nodes = &self.all_proxies;
        let cluster_opt = self.get_cluster();

        let node_resource = all_nodes.get(address)?;

        let cluster = match cluster_opt {
            Some(cluster) => cluster,
            None => {
                return Some(Proxy::new(
                    address.to_string(),
                    self.global_epoch,
                    vec![],
                    node_resource.node_addresses.to_vec(),
                    vec![],
                    HashMap::new(),
                ));
            }
        };

        let cluster_name = cluster.get_name().clone();
        let epoch = self.global_epoch;
        let nodes = cluster
            .get_nodes()
            .iter()
            .filter(|node| node.get_proxy_address() == address)
            .cloned()
            .collect();
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
        let proxy = Proxy::new(
            address.to_string(),
            epoch,
            nodes,
            Vec::new(),
            peers,
            HashMap::new(),
        );
        Some(proxy)
    }

    pub fn get_cluster_names(&self) -> Vec<DBName> {
        match &self.cluster {
            Some(cluster_store) => vec![cluster_store.name.clone()],
            None => vec![],
        }
    }

    pub fn get_cluster_by_name(&self, db_name: &str) -> Option<Cluster> {
        let db_name = DBName::from(&db_name).ok()?;
        let cluster_store = self.cluster.as_ref()?;
        if cluster_store.name != db_name {
            return None;
        }

        self.get_cluster()
    }

    fn get_cluster(&self) -> Option<Cluster> {
        let epoch = self.global_epoch;
        let cluster_store = self.cluster.as_ref()?;
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
                        .node_addresses
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
                        if let Some(stable_slots) = chunk.stable_slots.clone() {
                            first_slots.push(stable_slots[0].clone());
                        }
                        slots.append(&mut first_slots);
                        let slot_ranges: Vec<_> = chunk.migrating_slots[0]
                            .iter()
                            .map(|slot_range_store| {
                                slot_range_store.to_slot_range(epoch, &cluster_store.chunks)
                            })
                            .collect();
                        slots.extend(slot_ranges);
                    }
                    if i == second_slot_index {
                        let mut second_slots = vec![];
                        if let Some(stable_slots) = chunk.stable_slots.clone() {
                            second_slots.push(stable_slots[1].clone());
                        }
                        slots.append(&mut second_slots);
                        let slot_ranges: Vec<_> = chunk.migrating_slots[1]
                            .iter()
                            .map(|slot_range_store| {
                                slot_range_store.to_slot_range(epoch, &cluster_store.chunks)
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
                        3 | _ => 0,
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

        let cluster = Cluster::new(
            cluster_store.name.clone(),
            self.global_epoch,
            nodes,
            cluster_store.config.clone(),
        );
        Some(cluster)
    }

    pub fn add_failure(&mut self, address: String, reporter_id: String) {
        let now = Utc::now();
        self.bump_global_epoch();
        self.failures
            .entry(address)
            .or_insert_with(HashMap::new)
            .insert(reporter_id, now.timestamp());
    }

    pub fn get_failures(&mut self, falure_ttl: chrono::Duration) -> Vec<String> {
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
        self.failures.keys().cloned().collect()
    }

    pub fn add_hosts(
        &mut self,
        proxy_address: String,
        nodes: [String; NODES_PER_PROXY],
    ) -> Result<(), MetaStoreError> {
        self.bump_global_epoch();

        let res = if self.all_proxies.contains_key(&proxy_address) {
            Err(MetaStoreError::AlreadyExisted)
        } else {
            Ok(())
        };

        self.all_proxies
            .entry(proxy_address.clone())
            .or_insert_with(|| ProxyResource {
                proxy_address: proxy_address.clone(),
                node_addresses: nodes,
            });

        self.failed_proxies.remove(&proxy_address);

        res
    }

    // TODO: add node_num
    pub fn add_cluster(&mut self, db_name: String) -> Result<(), MetaStoreError> {
        let db_name = DBName::from(&db_name).map_err(|_| MetaStoreError::InvalidClusterName)?;
        if self.cluster.is_some() {
            return Err(MetaStoreError::OnlySupportOneCluster);
        }

        let node_num = NonZeroUsize::new(4).expect("TODO: remove this");

        let proxy_resource_arr = self.consume_proxy(node_num)?;
        let chunk_stores = Self::proxy_resource_to_chunk_store(proxy_resource_arr);

        let cluster_store = ClusterStore {
            name: db_name,
            chunks: chunk_stores,
            config: ClusterConfig::default(),
        };

        self.cluster = Some(cluster_store);
        self.bump_global_epoch();
        Ok(())
    }

    fn proxy_resource_to_chunk_store(
        proxy_resource_arr: Vec<[ProxyResource; CHUNK_HALF_NODE_NUM]>,
    ) -> Vec<ChunkStore> {
        let master_num = proxy_resource_arr.len() * 2;
        let range_per_node = (SLOT_NUM + master_num - 1) / master_num;
        let mut chunk_stores = vec![];
        for (i, chunk) in proxy_resource_arr.into_iter().enumerate() {
            let a = 2 * i;
            let b = a + 1;

            let create_slots = |index| SlotRange {
                range_list: RangeList::from_single_range(Range(
                    index * range_per_node,
                    cmp::min((index + 1) * range_per_node - 1, SLOT_NUM - 1),
                )),
                tag: SlotRangeTag::None,
            };

            let first_proxy = chunk[0].clone();
            let second_proxy = chunk[1].clone();
            let chunk_store = ChunkStore {
                role_position: ChunkRolePosition::Normal,
                stable_slots: Some([create_slots(a), create_slots(b)]),
                migrating_slots: [vec![], vec![]],
                proxy_addresses: [
                    first_proxy.proxy_address.clone(),
                    second_proxy.proxy_address.clone(),
                ],
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

    pub fn remove_cluster(&mut self, db_name: String) -> Result<(), MetaStoreError> {
        let db_name = DBName::from(&db_name).map_err(|_| MetaStoreError::InvalidClusterName)?;

        match &self.cluster {
            None => return Err(MetaStoreError::ClusterNotFound),
            Some(cluster) if cluster.name != db_name => {
                return Err(MetaStoreError::ClusterNotFound);
            }
            _ => (),
        }
        self.cluster = None;

        self.bump_global_epoch();
        Ok(())
    }

    pub fn auto_add_nodes(
        &mut self,
        db_name: String,
        num: Option<usize>,
    ) -> Result<Vec<Node>, MetaStoreError> {
        let db_name = DBName::from(&db_name).map_err(|_| MetaStoreError::InvalidClusterName)?;

        let existing_node_num = match self.cluster.as_ref() {
            None => return Err(MetaStoreError::ClusterNotFound),
            Some(cluster) => {
                if cluster.name != db_name {
                    return Err(MetaStoreError::ClusterNotFound);
                }
                cluster.chunks.len() * 2
            }
        };

        let num = match num {
            None => existing_node_num,
            Some(num) => num,
        };

        if num % 4 != 0 {
            return Err(MetaStoreError::InvalidNodeNum);
        }
        let proxy_num = NonZeroUsize::new(num / 2).ok_or_else(|| MetaStoreError::InvalidNodeNum)?;

        let proxy_resource_arr = self.consume_proxy(proxy_num)?;
        let mut chunks = Self::proxy_resource_to_chunk_store(proxy_resource_arr);

        match self.cluster {
            None => return Err(MetaStoreError::ClusterNotFound),
            Some(ref mut cluster) => {
                cluster.chunks.append(&mut chunks);
            }
        }

        let cluster = self.get_cluster().expect("auto_add_nodes");
        let nodes = cluster.get_nodes();
        let new_nodes = nodes
            .get((nodes.len() - num)..)
            .expect("auto_add_nodes: get nodes")
            .to_vec();

        self.bump_global_epoch();
        Ok(new_nodes)
    }

    pub fn remove_proxy(&mut self, proxy_address: String) -> Result<(), MetaStoreError> {
        if let Some(cluster) = self.get_cluster() {
            if cluster
                .get_nodes()
                .iter()
                .any(|node| node.get_proxy_address() == proxy_address)
            {
                return Err(MetaStoreError::InUse);
            }
        }

        self.all_proxies.remove(&proxy_address);
        self.failed_proxies.remove(&proxy_address);
        self.failures.remove(&proxy_address);
        self.bump_global_epoch();
        Ok(())
    }

    pub fn migrate_slots(&mut self, db_name: String) -> Result<(), MetaStoreError> {
        let db_name = DBName::from(&db_name).map_err(|_| MetaStoreError::InvalidClusterName)?;

        {
            let cluster = match self.cluster.as_mut() {
                None => return Err(MetaStoreError::ClusterNotFound),
                Some(cluster) => {
                    if db_name != cluster.name {
                        return Err(MetaStoreError::ClusterNotFound);
                    }
                    cluster
                }
            };

            let running_migration = cluster
                .chunks
                .iter()
                .any(|chunk| !chunk.migrating_slots.is_empty());
            if running_migration {
                return Err(MetaStoreError::MigrationRunning);
            }

            let migration_slots = Self::remove_slots_from_src(cluster);
            Self::assign_dst_slots(cluster, migration_slots);
        }

        self.bump_global_epoch();

        Ok(())
    }

    fn remove_slots_from_src(cluster: &mut ClusterStore) -> Vec<MigrationSlots> {
        let dst_chunk_num = cluster
            .chunks
            .iter()
            .filter(|chunk| chunk.stable_slots.is_none())
            .count();
        let dst_master_num = dst_chunk_num * 2;
        let master_num = cluster.chunks.len();
        let average = (SLOT_NUM + master_num - 1) / master_num;
        let remainder = SLOT_NUM - average * master_num;

        let mut curr_dst_master_index = 0;
        let mut migration_slots = vec![];
        let mut curr_dst_slots = vec![];
        let mut curr_slots_num = 0;

        for (src_chunk_index, src_chunk) in cluster.chunks.iter_mut().enumerate() {
            if let Some(stable_slots) = &mut src_chunk.stable_slots {
                for (src_chunk_part, slot_range) in stable_slots.iter_mut().enumerate() {
                    let r = if remainder.checked_sub(1).is_some() {
                        1
                    } else {
                        0
                    };

                    while curr_dst_master_index != dst_master_num || curr_slots_num < average {
                        if slot_range.get_range_list().get_slots_num() <= average + r {
                            break;
                        }
                        let need_num = average - curr_slots_num;
                        if let Some(num) = slot_range
                            .get_range_list()
                            .get_ranges()
                            .last()
                            .map(|r| r.end() - r.start())
                        {
                            if need_num >= num {
                                if let Some(range) =
                                    slot_range.get_mut_range_list().get_mut_ranges().pop()
                                {
                                    curr_dst_slots.push(range);
                                    curr_slots_num += num;
                                }
                            } else if let Some(range) =
                                slot_range.get_mut_range_list().get_mut_ranges().last_mut()
                            {
                                let end = range.end();
                                let start = end - need_num + 1;
                                *range.end_mut() -= need_num;
                                curr_dst_slots.push(Range(start, end));
                                curr_slots_num += need_num;
                            }
                            // reset current state
                            if curr_slots_num >= average {
                                curr_dst_master_index += 1;
                                migration_slots.push(MigrationSlots {
                                    meta: MigrationMetaStore {
                                        src_chunk_index,
                                        src_chunk_part,
                                        dst_chunk_index: curr_dst_master_index / 2,
                                        dst_chunk_part: curr_dst_master_index % 2,
                                    },
                                    ranges: curr_dst_slots.drain(..).collect(),
                                });
                                curr_slots_num = 0;
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
                    .get_mut(meta.src_chunk_part)
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

    pub fn commit_migration(&mut self, _task: MigrationTaskMeta) -> Result<(), MetaStoreError> {
        // TODO: implement it
        Err(MetaStoreError::NotSupported)
    }

    fn get_free_proxies(&self) -> Vec<String> {
        let failed_proxies = self.failed_proxies.clone();
        let failures = self.failures.clone();
        let occupied_proxies = self
            .cluster
            .as_ref()
            .map(|cluster| {
                cluster
                    .chunks
                    .iter()
                    .map(|chunk| chunk.proxy_addresses.to_vec())
                    .flatten()
                    .collect::<HashSet<String>>()
            })
            .unwrap_or_else(HashSet::new);

        let mut free_proxies = vec![];
        for proxy_resource in self.all_proxies.values() {
            let proxy_address = &proxy_resource.proxy_address;
            if failed_proxies.contains(proxy_address) {
                continue;
            }
            if failures.contains_key(proxy_address) {
                continue;
            }
            if occupied_proxies.contains(proxy_address) {
                continue;
            }
            free_proxies.push(proxy_address.clone());
        }
        free_proxies
    }

    fn build_link_table(&self) -> HashMap<String, HashMap<String, usize>> {
        let mut link_table: HashMap<String, HashMap<String, usize>> = HashMap::new();
        for proxy_resource in self.all_proxies.values() {
            let first = proxy_resource.proxy_address.clone();
            for proxy_resource in self.all_proxies.values() {
                let second = proxy_resource.proxy_address.clone();
                if first == second {
                    continue;
                }
                link_table
                    .entry(first.clone())
                    .or_insert_with(HashMap::new)
                    .entry(second)
                    .or_insert(0);
            }
        }

        if let Some(cluster) = self.cluster.as_ref() {
            for chunk in cluster.chunks.iter() {
                let first = chunk.proxy_addresses[0].clone();
                let second = chunk.proxy_addresses[1].clone();
                let linked_num = link_table
                    .entry(first)
                    .or_insert_with(HashMap::new)
                    .entry(second)
                    .or_insert(0);
                *linked_num += 1;
            }
        }
        link_table
    }

    fn consume_proxy(
        &self,
        proxy_num: NonZeroUsize,
    ) -> Result<Vec<[ProxyResource; CHUNK_HALF_NODE_NUM]>, MetaStoreError> {
        // host => proxies
        let mut host_proxies: HashMap<String, Vec<ProxySlot>> = HashMap::new();
        for proxy_address in self.get_free_proxies().into_iter() {
            let host = proxy_address
                .split(':')
                .next()
                .expect("consume_proxy: get host from address")
                .to_string();
            host_proxies
                .entry(host)
                .or_insert_with(Vec::new)
                .push(proxy_address);
        }

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

        while free_proxy_num > expected_num.get() {
            let max_proxy_num = host_proxies
                .values()
                .map(|proxies| proxies.len())
                .max()
                .unwrap_or(0);
            for proxies in host_proxies.values_mut() {
                if proxies.len() == max_proxy_num {
                    proxies.pop();
                    free_proxy_num -= 1;
                    break;
                }
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

        let mut new_proxies = vec![];
        while new_proxies.len() * 2 < expected_num.get() {
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
                    .min_by_key(|(_, count)| **count)
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

            new_proxies.push([first_address, second_address]);
        }

        Ok(new_proxies)
    }

    pub fn replace_failed_proxy(
        &mut self,
        failed_proxy_address: String,
    ) -> Result<Proxy, MetaStoreError> {
        if !self.all_proxies.contains_key(&failed_proxy_address) {
            return Err(MetaStoreError::HostNotFound);
        }

        let not_in_use = self
            .get_cluster()
            .and_then(|cluster| {
                cluster
                    .get_nodes()
                    .iter()
                    .find(|node| node.get_proxy_address() == failed_proxy_address)
                    .map(|_| ())
            })
            .is_none();
        if not_in_use {
            self.failed_proxies.insert(failed_proxy_address);
            return Err(MetaStoreError::NotInUse);
        }

        self.takeover_master(failed_proxy_address.clone())?;

        let proxy_resource = self.consume_new_proxy(failed_proxy_address.clone())?;
        {
            let cluster = self
                .cluster
                .as_mut()
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
        }

        self.failed_proxies.insert(failed_proxy_address);
        self.bump_global_epoch();
        Ok(self
            .get_host_by_address(&proxy_resource.proxy_address)
            .expect("replace_failed_proxy"))
    }

    fn takeover_master(&mut self, failed_proxy_address: String) -> Result<(), MetaStoreError> {
        self.bump_global_epoch();

        let cluster = self
            .cluster
            .as_mut()
            .ok_or_else(|| MetaStoreError::ClusterNotFound)?;
        for chunk in cluster.chunks.iter_mut() {
            if chunk.proxy_addresses[0] == failed_proxy_address {
                chunk.role_position = ChunkRolePosition::SecondChunkMaster;
                break;
            } else if chunk.proxy_addresses[1] == failed_proxy_address {
                chunk.role_position = ChunkRolePosition::FirstChunkMaster;
                break;
            }
        }
        Ok(())
    }

    fn consume_new_proxy(
        &mut self,
        failed_proxy_address: String,
    ) -> Result<ProxyResource, MetaStoreError> {
        let free_proxies: HashSet<String> = self.get_free_proxies().into_iter().collect();
        let link_table = self.build_link_table();

        let link_count_table = link_table
            .get(&failed_proxy_address)
            .expect("consume_new_proxy: cannot find failed proxy");
        let peer_proxy = link_count_table
            .iter()
            .filter(|(peer_address, _)| free_proxies.contains(*peer_address))
            .max_by_key(|(_, count)| *count)
            .map(|(peer_address, _)| peer_address)
            .ok_or_else(|| MetaStoreError::NoAvailableResource)?;

        let new_proxy = self
            .all_proxies
            .get(peer_proxy)
            .expect("consume_new_proxy: cannot find peer proxy")
            .clone();
        Ok(new_proxy)
    }
}

#[derive(Debug)]
pub enum MetaStoreError {
    InUse,
    NotInUse,
    NoAvailableResource,
    ResourceNotBalance,
    AlreadyExisted,
    ClusterNotFound,
    HostNotFound,
    InvalidNodeNum,
    InvalidClusterName,
    OnlySupportOneCluster,
    MigrationRunning,
    NotSupported,
}

impl fmt::Display for MetaStoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for MetaStoreError {
    fn cause(&self) -> Option<&dyn Error> {
        None
    }
}
