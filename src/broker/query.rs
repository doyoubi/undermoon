use super::store::{
    ChunkRolePosition, ClusterStore, HostProxy, MetaStore, CHUNK_HALF_NODE_NUM, CHUNK_NODE_NUM,
};
use crate::common::cluster::{Cluster, Node, PeerProxy, Proxy, ReplMeta, ReplPeer};
use crate::common::cluster::{ClusterName, Role};
use itertools::Itertools;
use std::collections::HashMap;
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
    ) -> Option<ClusterStore> {
        clusters
            .get(cluster_name)
            .map(|c| c.limit_migration(migration_limit))
    }

    pub fn get_proxy_by_address(&self, address: &str, migration_limit: u64) -> Option<Proxy> {
        let all_proxies = &self.store.all_proxies;
        let clusters = &self.store.clusters;

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
                    self.store.global_epoch,
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

    pub fn get_cluster_by_name(&self, cluster_name: &str, migration_limit: u64) -> Option<Cluster> {
        let cluster_name = ClusterName::try_from(cluster_name).ok()?;

        let cluster_store =
            Self::get_cluster_store(&self.store.clusters, &cluster_name, migration_limit)?;
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

    pub fn get_free_proxies(&self) -> Vec<HostProxy> {
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
            free_proxies.push(HostProxy {
                host: proxy_resource.host.clone(),
                proxy_address: proxy_address.clone(),
            });
        }
        free_proxies
    }
}
