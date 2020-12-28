use super::store::{
    ChunkRolePosition, ClusterInfo, ClusterStore, HostProxy, MetaStore, CHUNK_HALF_NODE_NUM,
    CHUNK_NODE_NUM,
};
use crate::broker::store::ProxyResource;
use crate::broker::utils::InvalidStateError;
use crate::common::cluster::{Cluster, Node, PeerProxy, Proxy, ReplMeta, ReplPeer};
use crate::common::cluster::{ClusterName, Role};
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;

pub struct MetaStoreQuery<'a> {
    store: &'a MetaStore,
}

impl<'a> MetaStoreQuery<'a> {
    pub fn new(store: &'a MetaStore) -> Self {
        Self { store }
    }

    pub fn get_proxies(&self) -> Vec<String> {
        self.store.all_proxies.keys().cloned().collect()
    }

    pub fn get_proxies_with_pagination(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Vec<String> {
        let offset = offset.unwrap_or(0);
        let it = self.store.all_proxies.keys().skip(offset);
        match limit {
            None => it.cloned().collect(),
            Some(limit) => it.take(limit).cloned().collect(),
        }
    }

    fn get_cluster_store(
        clusters: &HashMap<ClusterName, ClusterStore>,
        cluster_name: &ClusterName,
        migration_limit: u64,
    ) -> Result<Option<ClusterStore>, InvalidStateError> {
        clusters
            .get(cluster_name)
            .map(|c| c.limit_migration(migration_limit))
            .transpose()
    }

    pub fn get_proxy_by_address(
        &self,
        address: &str,
        migration_limit: u64,
    ) -> Result<Option<Proxy>, InvalidStateError> {
        let all_proxies = &self.store.all_proxies;
        let clusters = &self.store.clusters;

        let proxy_resource = match all_proxies.get(address) {
            Some(pr) => pr,
            None => return Ok(None),
        };
        let cluster_opt = proxy_resource
            .cluster
            .as_ref()
            .map(|name| Self::get_cluster_store(clusters, name, migration_limit))
            .transpose()?
            .flatten();

        let cluster = match cluster_opt {
            Some(cluster_store) => Self::cluster_store_to_cluster(&cluster_store)?,
            None => {
                return Ok(Some(Proxy::new(
                    address.to_string(),
                    self.store.global_epoch,
                    vec![],
                    proxy_resource.node_addresses.to_vec(),
                    vec![],
                    HashMap::new(),
                )));
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

        let mut cluster_config = HashMap::new();
        cluster_config.insert(cluster_name, cluster.get_config());

        let proxy = Proxy::new(
            address.to_string(),
            epoch,
            nodes,
            free_nodes,
            peers,
            cluster_config,
        );
        Ok(Some(proxy))
    }

    pub fn get_cluster_names(&self) -> Vec<ClusterName> {
        self.store.clusters.keys().cloned().collect()
    }

    pub fn get_cluster_names_with_pagination(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Vec<ClusterName> {
        let offset = offset.unwrap_or(0);
        let it = self.store.clusters.keys().skip(offset);
        match limit {
            None => it.cloned().collect(),
            Some(limit) => it.take(limit).cloned().collect(),
        }
    }

    pub fn get_cluster_by_name(
        &self,
        cluster_name: &str,
        migration_limit: u64,
    ) -> Result<Option<Cluster>, InvalidStateError> {
        let cluster_name = match ClusterName::try_from(cluster_name).ok() {
            Some(cluster_name) => cluster_name,
            None => return Ok(None),
        };

        let cluster_store =
            match Self::get_cluster_store(&self.store.clusters, &cluster_name, migration_limit)? {
                Some(cluster_store) => cluster_store,
                None => return Ok(None),
            };
        Self::cluster_store_to_cluster(&cluster_store).map(Some)
    }

    pub fn get_cluster_info_by_name(
        &self,
        cluster_name: &str,
        migration_limit: u64,
    ) -> Result<Option<ClusterInfo>, InvalidStateError> {
        let cluster_name = match ClusterName::try_from(cluster_name) {
            Ok(cluster_name) => cluster_name,
            _ => return Ok(None),
        };

        let cluster_store_opt =
            Self::get_cluster_store(&self.store.clusters, &cluster_name, migration_limit)?;
        Ok(cluster_store_opt.map(|cluster_store| cluster_store.get_info()))
    }

    pub fn cluster_store_to_cluster(
        cluster_store: &ClusterStore,
    ) -> Result<Cluster, InvalidStateError> {
        let cluster_name = cluster_store.name.clone();

        let mut nodes = vec![];
        for chunk in cluster_store.chunks.iter() {
            for i in 0..CHUNK_NODE_NUM {
                let address = chunk
                    .node_addresses
                    .get(i)
                    .ok_or_else(|| {
                        error!("FATAL MetaStore::get_cluster_by_name: failed to get node");
                        InvalidStateError
                    })?
                    .clone();
                let proxy_address = chunk
                    .proxy_addresses
                    .get(i / 2)
                    .ok_or_else(|| {
                        error!("FATAL MetaStore::get_cluster_by_name: failed to get proxy");
                        InvalidStateError
                    })?
                    .clone();

                // get slots
                let mut slots = vec![];
                let (first_slot_index, second_slot_index) = match chunk.role_position {
                    ChunkRolePosition::Normal => (0, 2),
                    ChunkRolePosition::FirstChunkMaster => (0, 1),
                    ChunkRolePosition::SecondChunkMaster => (3, 2),
                };
                if i == first_slot_index {
                    let mut first_slots = vec![];
                    if let Some(stable_slots) = &chunk.stable_slots[0] {
                        first_slots.push(stable_slots.clone());
                    }
                    slots.append(&mut first_slots);
                    for slot_range_store in chunk.migrating_slots[0].iter() {
                        slots.push(slot_range_store.to_slot_range(&cluster_store.chunks)?);
                    }
                }
                if i == second_slot_index {
                    let mut second_slots = vec![];
                    if let Some(stable_slots) = &chunk.stable_slots[1] {
                        second_slots.push(stable_slots.clone());
                    }
                    slots.append(&mut second_slots);
                    for slot_range_store in chunk.migrating_slots[1].iter() {
                        slots.push(slot_range_store.to_slot_range(&cluster_store.chunks)?);
                    }
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
                        .ok_or_else(|| {
                            error!("FATAL MetaStore::get_cluster_by_name: failed to get peer node");
                            InvalidStateError
                        })?
                        .clone(),
                    proxy_address: chunk
                        .proxy_addresses
                        .get(peer_index / 2)
                        .ok_or_else(|| {
                            error!(
                                "FATAL MetaStore::get_cluster_by_name: failed to get peer proxy"
                            );
                            InvalidStateError
                        })?
                        .clone(),
                };
                let repl = ReplMeta::new(role, vec![peer]);

                let node = Node::new(address, proxy_address, cluster_name.clone(), slots, repl);
                nodes.push(node);
            }
        }

        let cluster = Cluster::new(
            cluster_store.name.clone(),
            cluster_store.epoch,
            nodes,
            cluster_store.config.clone(),
        );
        Ok(cluster)
    }

    pub fn get_free_proxy_resource(&self) -> Vec<ProxyResource> {
        let failed_proxies = self.store.failed_proxies.clone();
        let failures = self.store.failures.clone();

        let mut free_proxies = vec![];
        for proxy_resource in self.store.all_proxies.values() {
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
            free_proxies.push(proxy_resource.clone());
        }
        free_proxies
    }

    pub fn get_free_proxies(&self) -> Vec<HostProxy> {
        self.get_free_proxy_resource()
            .into_iter()
            .map(|proxy_resource| HostProxy {
                host: proxy_resource.host.clone(),
                proxy_address: proxy_resource.proxy_address,
            })
            .collect()
    }

    pub fn check_metadata(&self) -> bool {
        let mut data_correct = true;

        for (cluster_name, cluster) in self.store.clusters.iter() {
            let mut proxy_address_set = HashSet::new();
            for chunk in cluster.chunks.iter() {
                for (i, proxy_address) in chunk.proxy_addresses.iter().enumerate() {
                    match self.store.all_proxies.get(proxy_address) {
                        None => {
                            error!("cannot find {} in all_proxies", proxy_address);
                            data_correct = false;
                        }
                        Some(proxy_resource) => {
                            if &proxy_resource.proxy_address != proxy_address {
                                error!(
                                    "not correspondent proxy address {} != {}",
                                    proxy_resource.proxy_address, proxy_address
                                );
                                data_correct = false;
                            }
                            if proxy_resource.cluster != Some(cluster_name.clone()) {
                                error!(
                                    "incorrect cluster name for {} {:?} != {}",
                                    proxy_address, proxy_resource.cluster, cluster_name
                                );
                                data_correct = false;
                            }
                            if proxy_address_set.contains(proxy_address) {
                                error!(
                                    "duplicate proxy address {} in cluster {}",
                                    proxy_address, cluster_name
                                );
                                data_correct = false;
                            }
                            proxy_address_set.insert(proxy_address.clone());
                            let (host, node_addresses) = if i == 0 {
                                (
                                    chunk.hosts[0].clone(),
                                    [
                                        chunk.node_addresses[0].clone(),
                                        chunk.node_addresses[1].clone(),
                                    ],
                                )
                            } else {
                                (
                                    chunk.hosts[1].clone(),
                                    [
                                        chunk.node_addresses[2].clone(),
                                        chunk.node_addresses[3].clone(),
                                    ],
                                )
                            };
                            if host != proxy_resource.host {
                                error!(
                                    "invalid host for {} {:?} != {:?}",
                                    proxy_address, host, proxy_resource.host
                                );
                                data_correct = false;
                            }
                            if node_addresses != proxy_resource.node_addresses {
                                error!(
                                    "invalid node_addresses for {} {:?} != {:?}",
                                    proxy_address, node_addresses, proxy_resource.node_addresses
                                );
                                data_correct = false;
                            }
                        }
                    }
                }
            }
        }

        for (proxy_address, proxy_resource) in self.store.all_proxies.iter() {
            let cluster_name = match proxy_resource.cluster.as_ref() {
                None => continue,
                Some(cluster_name) => cluster_name,
            };
            match self.store.clusters.get(cluster_name) {
                None => {
                    error!("cannot find cluster {} {}", proxy_address, cluster_name);
                    data_correct = false;
                }
                Some(cluster) => {
                    let chunk = cluster
                        .chunks
                        .iter()
                        .find(|chunk| chunk.proxy_addresses.contains(proxy_address));
                    if chunk.is_none() {
                        error!(
                            "cannot find chunk in cluster {} {}",
                            proxy_address, cluster_name
                        );
                        data_correct = false;
                    }
                }
            }
        }

        data_correct
    }
}
