use super::query::MetaStoreQuery;
use super::store::{
    ChunkRolePosition, ChunkStore, ClusterStore, HostProxy, MetaStore, MetaStoreError,
    ProxyResource, CHUNK_HALF_NODE_NUM, CHUNK_NODE_NUM, CHUNK_PARTS, NODES_PER_PROXY,
};
use crate::common::cluster::{
    Cluster, Node, Proxy, Range, RangeList, ReplMeta, ReplPeer, SlotRange, SlotRangeTag,
};
use crate::common::cluster::{ClusterName, Role};
use crate::common::config::ClusterConfig;
use crate::common::utils::SLOT_NUM;
use chrono::{DateTime, NaiveDateTime, Utc};
use itertools::Itertools;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::num::NonZeroUsize;

pub struct MetaStoreUpdate<'a> {
    store: &'a mut MetaStore,
}

impl<'a> MetaStoreUpdate<'a> {
    pub fn new(store: &'a mut MetaStore) -> Self {
        Self { store }
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
        self.store.bump_global_epoch();
        self.store
            .failures
            .entry(address)
            .or_insert_with(HashMap::new)
            .insert(reporter_id, now.timestamp());
    }

    pub fn get_failures(
        &mut self,
        failure_ttl: chrono::Duration,
        failure_quorum: u64,
    ) -> Vec<String> {
        let now = Utc::now();
        for reporter_map in self.store.failures.values_mut() {
            reporter_map.retain(|_, report_time| {
                let report_datetime =
                    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(*report_time, 0), Utc);
                now - report_datetime < failure_ttl
            });
        }
        self.store
            .failures
            .retain(|_, proxy_failure_map| !proxy_failure_map.is_empty());

        let all_proxies = &self.store.all_proxies;
        self.store
            .failures
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
        proxy_index: Option<usize>,
    ) -> Result<(), MetaStoreError> {
        if proxy_address.split(':').count() != 2 {
            return Err(MetaStoreError::InvalidProxyAddress);
        }

        let host = match (host, proxy_address.split(':').next()) {
            (Some(h), _) => h,
            (None, Some(h)) => h.to_string(),
            (None, None) => return Err(MetaStoreError::InvalidProxyAddress),
        };

        let index = match (self.store.enable_ordered_proxy, proxy_index) {
            (false, _) => 0,
            (true, Some(index)) => index,
            (true, None) => return Err(MetaStoreError::MissingIndex),
        };

        self.store.bump_global_epoch();

        let exists = self.store.all_proxies.contains_key(&proxy_address);

        self.store
            .all_proxies
            .entry(proxy_address.clone())
            .or_insert_with(|| ProxyResource {
                proxy_address: proxy_address.clone(),
                node_addresses: nodes,
                host,
                index,
                cluster: None,
            });

        self.store.failed_proxies.remove(&proxy_address);
        self.store.failures.remove(&proxy_address);

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
        if self.store.enable_ordered_proxy && !self.store.clusters.is_empty() {
            return Err(MetaStoreError::OneClusterAlreadyExisted);
        }

        let cluster_name = ClusterName::try_from(cluster_name.as_str())
            .map_err(|_| MetaStoreError::InvalidClusterName)?;
        if self.store.clusters.contains_key(&cluster_name) {
            return Err(MetaStoreError::AlreadyExisted);
        }

        if node_num % 4 != 0 {
            return Err(MetaStoreError::InvalidNodeNum);
        }
        let proxy_num =
            NonZeroUsize::new(node_num / 2).ok_or_else(|| MetaStoreError::InvalidNodeNum)?;

        let proxy_resource_arr = if self.store.enable_ordered_proxy {
            self.generate_free_chunks_for_ordered_proxy_index(proxy_num, 0)?
        } else {
            self.generate_free_chunks(proxy_num)?
        };
        let chunk_stores = Self::proxy_resource_to_chunk_store(proxy_resource_arr, true);

        let epoch = self.store.bump_global_epoch();

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
                    .store
                    .all_proxies
                    .get_mut(proxy_address)
                    .expect("add_cluster: failed to get back proxy");
                proxy.cluster = Some(cluster_name.clone());
            }
        }

        self.store.clusters.insert(cluster_name, cluster_store);
        Ok(())
    }

    // This function should preserve the order of the chunks in `proxy_resource_arr`.
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

        let cluster_store = match self.store.clusters.remove(&cluster_name) {
            None => return Err(MetaStoreError::ClusterNotFound),
            Some(cluster_store) => cluster_store,
        };

        // Set proxies free.
        for chunk in cluster_store.chunks.iter() {
            for proxy_address in chunk.proxy_addresses.iter() {
                if let Some(proxy) = self.store.all_proxies.get_mut(proxy_address) {
                    proxy.cluster = None;
                }
            }
        }

        self.store.bump_global_epoch();
        Ok(())
    }

    pub fn auto_scale_up_nodes(
        &mut self,
        cluster_name: String,
        expected_num: usize,
    ) -> Result<Vec<Node>, MetaStoreError> {
        let name = ClusterName::try_from(cluster_name.as_str())
            .map_err(|_| MetaStoreError::InvalidClusterName)?;

        let existing_node_num = match self.store.clusters.get(&name) {
            None => return Err(MetaStoreError::ClusterNotFound),
            Some(cluster) => cluster.chunks.len() * 4,
        };

        let added_num = match expected_num.checked_sub(existing_node_num) {
            None | Some(0) => return Err(MetaStoreError::NodeNumAlreadyEnough),
            Some(added_num) => added_num,
        };

        self.auto_add_nodes(cluster_name, added_num)
    }

    pub fn auto_add_nodes(
        &mut self,
        cluster_name: String,
        num: usize,
    ) -> Result<Vec<Node>, MetaStoreError> {
        let cluster_name = ClusterName::try_from(cluster_name.as_str())
            .map_err(|_| MetaStoreError::InvalidClusterName)?;

        let existing_proxy_num = match self.store.clusters.get(&cluster_name) {
            None => return Err(MetaStoreError::ClusterNotFound),
            Some(cluster) => {
                if cluster
                    .chunks
                    .iter()
                    .any(|chunk| chunk.migrating_slots.iter().any(|slots| !slots.is_empty()))
                {
                    return Err(MetaStoreError::MigrationRunning);
                }
                cluster.chunks.len() * CHUNK_PARTS
            }
        };

        if num % 4 != 0 {
            return Err(MetaStoreError::InvalidNodeNum);
        }
        let proxy_num = NonZeroUsize::new(num / 2).ok_or_else(|| MetaStoreError::InvalidNodeNum)?;

        let proxy_resource_arr = if self.store.enable_ordered_proxy {
            self.generate_free_chunks_for_ordered_proxy_index(proxy_num, existing_proxy_num)?
        } else {
            self.generate_free_chunks(proxy_num)?
        };
        let mut chunks = Self::proxy_resource_to_chunk_store(proxy_resource_arr, false);

        let new_epoch = self.store.bump_global_epoch();

        let cluster = match self.store.clusters.get_mut(&cluster_name) {
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
                .store
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

    pub fn auto_delete_free_nodes_if_exists(
        &mut self,
        cluster_name: String,
    ) -> Result<(), MetaStoreError> {
        match self.auto_delete_free_nodes(cluster_name) {
            Ok(()) => Ok(()),
            Err(err) => match err {
                MetaStoreError::MigrationRunning | MetaStoreError::FreeNodeNotFound => Ok(()),
                other_err => Err(other_err),
            },
        }
    }

    pub fn auto_delete_free_nodes(&mut self, cluster_name: String) -> Result<(), MetaStoreError> {
        let cluster_name = ClusterName::try_from(cluster_name.as_str())
            .map_err(|_| MetaStoreError::InvalidClusterName)?;
        // Will bump epoch later on success.
        let new_epoch = self.store.get_global_epoch() + 1;

        let removed_chunks = match self.store.clusters.get_mut(&cluster_name) {
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
                if let Some(proxy) = self.store.all_proxies.get_mut(proxy_address) {
                    proxy.cluster = None;
                }
            }
        }

        self.store.bump_global_epoch();
        Ok(())
    }

    pub fn remove_proxy(&mut self, proxy_address: String) -> Result<(), MetaStoreError> {
        match self.store.all_proxies.get(&proxy_address) {
            None => return Err(MetaStoreError::ProxyNotFound),
            Some(proxy) => {
                if proxy.cluster.is_some() {
                    return Err(MetaStoreError::InUse);
                }
            }
        }

        self.store.all_proxies.remove(&proxy_address);
        self.store.failed_proxies.remove(&proxy_address);
        self.store.failures.remove(&proxy_address);
        self.store.bump_global_epoch();
        Ok(())
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
                    self.store
                        .all_proxies
                        .get(&a)
                        .expect("consume_proxy: get proxy resource")
                        .clone(),
                    self.store
                        .all_proxies
                        .get(&b)
                        .expect("consume_proxy: get proxy resource")
                        .clone(),
                ]
            })
            .collect();
        Ok(new_proxies)
    }

    fn generate_free_chunks_for_ordered_proxy_index(
        &self,
        proxy_num: NonZeroUsize,
        first_index: usize,
    ) -> Result<Vec<[ProxyResource; CHUNK_HALF_NODE_NUM]>, MetaStoreError> {
        let mut host_proxies = MetaStoreQuery::new(&self.store).get_free_proxy_resource();
        if host_proxies.len() < proxy_num.get() {
            return Err(MetaStoreError::NoAvailableResource);
        }

        host_proxies.sort_by_key(|proxy_resource| proxy_resource.index);
        host_proxies.truncate(proxy_num.get());

        let mut indices = host_proxies
            .iter()
            .map(|proxy_resource| proxy_resource.index);
        if let Some(first) = indices.next() {
            if first != first_index {
                return Err(MetaStoreError::ProxyResourceOutOfOrder);
            }
            let mut last_index = first;
            for i in indices {
                if last_index + 1 != i {
                    return Err(MetaStoreError::ProxyResourceOutOfOrder);
                }
                last_index = i;
            }
        }

        let mut proxy_resources = vec![];
        for mut chunk in host_proxies
            .into_iter()
            .chunks(CHUNK_HALF_NODE_NUM)
            .into_iter()
        {
            let first = chunk.next().ok_or_else(|| {
                error!("Invalid state. Cannot get first host proxy.");
                MetaStoreError::InvalidNodeNum
            })?;
            let second = chunk.next().ok_or_else(|| {
                error!("Invalid state. Cannot get second host proxy.");
                MetaStoreError::InvalidNodeNum
            })?;

            proxy_resources.push([first, second]);
        }

        Ok(proxy_resources)
    }

    fn generate_free_host_proxies(&self) -> HashMap<String, Vec<String>> {
        // host => proxies
        let mut host_proxies: HashMap<String, Vec<String>> = HashMap::new();
        for host_proxy in MetaStoreQuery::new(&self.store)
            .get_free_proxies()
            .into_iter()
        {
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

    fn allocate_chunk(
        mut host_proxies: HashMap<String, Vec<String>>,
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

    fn remove_redundant_chunks(
        mut host_proxies: HashMap<String, Vec<String>>,
        expected_num: NonZeroUsize,
    ) -> Result<HashMap<String, Vec<String>>, MetaStoreError> {
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
            .store
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
            .map(|(peer_host, _)| peer_host)
            .ok_or_else(|| MetaStoreError::NoAvailableResource)?;

        let peer_proxy = MetaStoreQuery::new(&self.store)
            .get_free_proxies()
            .iter()
            .find(|host_proxy| peer_host == &host_proxy.host)
            .expect("consume_new_proxy: get peer address")
            .proxy_address
            .clone();

        let new_proxy = self
            .store
            .all_proxies
            .get(&peer_proxy)
            .expect("consume_new_proxy: cannot find peer proxy")
            .clone();
        Ok(new_proxy)
    }

    fn build_link_table(&self) -> HashMap<String, HashMap<String, usize>> {
        // Remove the fully occupied hosts or there will be severe performance problems.
        let free_hosts: HashSet<String> = self
            .store
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
        for proxy_resource in self.store.all_proxies.values() {
            let first_host = proxy_resource.host.clone();

            for proxy_resource in self.store.all_proxies.values() {
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

        for cluster in self.store.clusters.values() {
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

    pub fn replace_failed_proxy(
        &mut self,
        failed_proxy_address: String,
        migration_limit: u64,
    ) -> Result<Option<Proxy>, MetaStoreError> {
        let cluster_name = match self.store.all_proxies.get(&failed_proxy_address) {
            None => return Err(MetaStoreError::ProxyNotFound),
            Some(proxy) => proxy.cluster.clone(),
        };

        let cluster_name = match cluster_name {
            None => {
                self.store.failures.remove(&failed_proxy_address);
                self.store.failed_proxies.insert(failed_proxy_address);
                return Ok(None);
            }
            Some(cluster_name) => cluster_name,
        };

        self.takeover_master(&cluster_name, failed_proxy_address.clone())?;

        // If enable_ordered_proxy is true, we won't replace the proxy.
        if self.store.enable_ordered_proxy {
            self.store.bump_global_epoch();
            return Ok(None);
        }

        self.store
            .failed_proxies
            .insert(failed_proxy_address.clone());

        let proxy_resource = self.generate_new_free_proxy(failed_proxy_address.clone())?;
        let new_epoch = self.store.bump_global_epoch();
        {
            let cluster = self
                .store
                .clusters
                .get_mut(&cluster_name)
                .expect("replace_failed_proxy: get cluster");
            for chunk in cluster.chunks.iter_mut() {
                if chunk.proxy_addresses[0] == failed_proxy_address {
                    chunk.hosts[0] = proxy_resource.host.clone();
                    chunk.proxy_addresses[0] = proxy_resource.proxy_address.clone();
                    chunk.node_addresses[0] = proxy_resource.node_addresses[0].clone();
                    chunk.node_addresses[1] = proxy_resource.node_addresses[1].clone();
                    break;
                } else if chunk.proxy_addresses[1] == failed_proxy_address {
                    chunk.hosts[1] = proxy_resource.host.clone();
                    chunk.proxy_addresses[1] = proxy_resource.proxy_address.clone();
                    chunk.node_addresses[2] = proxy_resource.node_addresses[0].clone();
                    chunk.node_addresses[3] = proxy_resource.node_addresses[1].clone();
                    break;
                }
            }
            cluster.set_epoch(new_epoch);
        }

        // Set this proxy free
        if let Some(proxy) = self.store.all_proxies.get_mut(&failed_proxy_address) {
            proxy.cluster = None;
        }
        // Tag the new proxy as occupied
        if let Some(proxy) = self
            .store
            .all_proxies
            .get_mut(&proxy_resource.proxy_address)
        {
            proxy.cluster = Some(cluster_name);
        }

        let proxy = MetaStoreQuery::new(self.store)
            .get_proxy_by_address(&proxy_resource.proxy_address, migration_limit)
            .expect("replace_failed_proxy");
        Ok(Some(proxy))
    }

    fn takeover_master(
        &mut self,
        cluster_name: &ClusterName,
        failed_proxy_address: String,
    ) -> Result<(), MetaStoreError> {
        let new_epoch = self.store.bump_global_epoch();

        let cluster = self
            .store
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

    fn second_host_cmp(
        host1: &str,
        count1: usize,
        host2: &str,
        count2: usize,
        free_host_proxies: &HashMap<String, Vec<String>>,
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

    pub fn balance_masters(&mut self, cluster_name: String) -> Result<(), MetaStoreError> {
        let cluster_name = ClusterName::try_from(cluster_name.as_str())
            .map_err(|_| MetaStoreError::InvalidClusterName)?;
        let new_epoch = self.store.get_global_epoch() + 1;

        let failed_proxies = &self.store.failed_proxies;
        let failures = &self.store.failures;

        let failed_proxy_exists = |addresses: &[String; CHUNK_PARTS]| -> bool {
            for address in addresses.iter() {
                if failed_proxies.contains(address) || failures.contains_key(address) {
                    return true;
                }
            }
            false
        };

        match self.store.clusters.get_mut(&cluster_name) {
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

        self.store.bump_global_epoch();
        Ok(())
    }

    pub fn change_config(
        &mut self,
        cluster_name: String,
        config: HashMap<String, String>,
    ) -> Result<(), MetaStoreError> {
        let cluster_name = ClusterName::try_from(cluster_name.as_str())
            .map_err(|_| MetaStoreError::InvalidClusterName)?;
        // Will bump epoch later on success.
        let new_epoch = self.store.get_global_epoch() + 1;
        match self.store.clusters.get_mut(&cluster_name) {
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

        self.store.bump_global_epoch();
        Ok(())
    }
}
