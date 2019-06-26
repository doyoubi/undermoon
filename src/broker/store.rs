use ::common::cluster::{
    Cluster, Host, MigrationTaskMeta, Node, ReplMeta, ReplPeer, SlotRange, SlotRangeTag,
};
use ::common::utils::SLOT_NUM;
use chrono::{DateTime, NaiveDateTime, Utc};
use common::cluster::{MigrationMeta, Role};
use itertools::Itertools;
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone)]
pub struct NodeSlot {
    pub proxy_address: String,
    pub node_address: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NodeResource {
    pub node_addresses: HashSet<String>,
    pub cluster_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MigrationType {
    All,
    Half,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct MetaStore {
    clusters: HashMap<String, Cluster>,
    // proxy_address => nodes and cluster_name
    all_nodes: HashMap<String, NodeResource>,
    failures: HashMap<String, i64>,
}

impl Default for MetaStore {
    fn default() -> Self {
        Self {
            clusters: HashMap::new(),
            all_nodes: HashMap::new(),
            failures: HashMap::new(),
        }
    }
}

macro_rules! try_state {
    ($expression:expr) => {{
        match $expression {
            Ok(v) => (v),
            Err(err) => {
                error!("Invalid state: {}", line!());
                return Err(err);
            }
        }
    }};
}

impl MetaStore {
    pub fn get_hosts(&self) -> Vec<String> {
        self.clusters
            .iter()
            .map(|(_, cluster)| {
                cluster
                    .get_nodes()
                    .iter()
                    .map(|node| node.get_proxy_address().clone())
            })
            .flatten()
            .collect::<HashSet<String>>()
            .into_iter()
            .collect()
    }

    pub fn get_host_by_address(&self, address: &str) -> Option<Host> {
        let all_nodes = &self.all_nodes;
        let clusters = &self.clusters;
        all_nodes
            .get(address)
            .and_then(|node_resource| node_resource.cluster_name.clone())
            .and_then(|cluster_name| clusters.get(&cluster_name))
            .map(|cluster| {
                let epoch = cluster.get_epoch();
                let nodes = cluster
                    .get_nodes()
                    .iter()
                    .filter(|node| node.get_proxy_address() == address)
                    .cloned()
                    .collect();
                Host::new(address.to_string(), epoch, nodes)
            })
    }

    pub fn get_cluster_names(&self) -> Vec<String> {
        self.clusters.keys().cloned().collect()
    }

    pub fn get_cluster_by_name(&self, name: &str) -> Option<Cluster> {
        self.clusters.get(name).cloned()
    }

    pub fn add_failure(&mut self, address: String, _reporter_id: String) {
        let now = Utc::now();
        self.failures.insert(address, now.timestamp());
    }

    pub fn get_failures(&mut self, falure_ttl: chrono::Duration) -> Vec<String> {
        let now = Utc::now();
        self.failures.retain(|_, report_time| {
            let report_datetime =
                DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(*report_time, 0), Utc);
            now - report_datetime < falure_ttl
        });
        self.failures.keys().cloned().collect()
    }

    pub fn add_hosts(
        &mut self,
        proxy_address: String,
        nodes: Vec<String>,
    ) -> Result<(), MetaStoreError> {
        let node_resource = self
            .all_nodes
            .entry(proxy_address.clone())
            .or_insert_with(|| NodeResource {
                node_addresses: HashSet::new(),
                cluster_name: None,
            });

        if node_resource.cluster_name.is_some() {
            return Err(MetaStoreError::InUse);
        }

        for node in nodes.into_iter() {
            node_resource.node_addresses.insert(node);
        }
        Ok(())
    }

    pub fn add_cluster(&mut self, cluster_name: String) -> Result<(), MetaStoreError> {
        if self.clusters.contains_key(&cluster_name) {
            return Err(MetaStoreError::AlreadyExisted);
        }

        let node_slots = self.consume_node_slot(&cluster_name)?;

        let node_num = node_slots.len();
        let range_per_node = (SLOT_NUM + node_num - 1) / node_num;
        let mut nodes = vec![];
        for (i, node_slot) in node_slots.into_iter().enumerate() {
            let NodeSlot {
                proxy_address,
                node_address,
            } = node_slot;
            let slots = SlotRange {
                start: i * range_per_node,
                end: cmp::min((i + 1) * range_per_node - 1, SLOT_NUM - 1),
                tag: SlotRangeTag::None,
            };
            let repl = ReplMeta::new(Role::Master, vec![]);
            let node = Node::new(
                node_address,
                proxy_address.clone(),
                cluster_name.clone(),
                vec![slots],
                repl,
            );
            nodes.push(node);
        }

        let cluster = Cluster::new(cluster_name.clone(), 0, nodes);
        self.clusters.insert(cluster_name, cluster);
        Ok(())
    }

    pub fn remove_cluster(&mut self, cluster_name: String) -> Result<(), MetaStoreError> {
        let cluster = self
            .clusters
            .remove(&cluster_name)
            .ok_or(MetaStoreError::ClusterNotFound)?;

        for node in cluster.into_nodes().iter() {
            match self.all_nodes.get_mut(node.get_proxy_address()) {
                Some(node_resource) => node_resource.cluster_name = None,
                None => error!(
                    "Invalid meta: cannot find {} {} in all_hosts",
                    cluster_name,
                    node.get_proxy_address()
                ),
            }
        }
        Ok(())
    }

    pub fn auto_add_nodes(&mut self, cluster_name: String) -> Result<Vec<Node>, MetaStoreError> {
        if self.clusters.get(&cluster_name).is_none() {
            return Err(MetaStoreError::ClusterNotFound);
        }

        let node_slots = self.consume_node_slot(&cluster_name)?;
        let cluster = self
            .clusters
            .get_mut(&cluster_name)
            .ok_or_else(|| MetaStoreError::ClusterNotFound)?;
        let mut nodes = vec![];
        for node_slot in node_slots.into_iter() {
            let node = try_state!(Self::add_node(cluster, node_slot));
            nodes.push(node);
        }
        Ok(nodes)
    }

    pub fn add_node(cluster: &mut Cluster, node_slot: NodeSlot) -> Result<Node, MetaStoreError> {
        let NodeSlot {
            proxy_address,
            node_address,
        } = node_slot;

        let repl = ReplMeta::new(Role::Master, vec![]);
        let new_node = Node::new(
            node_address.clone(),
            proxy_address.clone(),
            cluster.get_name().clone(),
            vec![],
            repl,
        );

        cluster.add_node(new_node.clone());
        cluster.bump_epoch();

        Ok(new_node)
    }

    pub fn remove_proxy_from_cluster(
        &mut self,
        cluster_name: String,
        proxy_address: String,
    ) -> Result<(), MetaStoreError> {
        self.remove_proxy_from_cluster_helper(&cluster_name, &proxy_address, false)
    }

    pub fn remove_proxy_from_cluster_helper(
        &mut self,
        cluster_name: &str,
        proxy_address: &str,
        force: bool,
    ) -> Result<(), MetaStoreError> {
        let cluster = self
            .clusters
            .get_mut(cluster_name)
            .ok_or(MetaStoreError::ClusterNotFound)?;

        if !force {
            let still_serving = cluster.get_nodes().iter().any(|node| {
                node.get_proxy_address() == proxy_address && !node.get_slots().is_empty()
            });
            if still_serving {
                return Err(MetaStoreError::SlotNotEmpty);
            }
        }

        let node_addresses = cluster
            .get_nodes()
            .iter()
            .filter(|node| node.get_proxy_address() == proxy_address)
            .map(|node| node.get_address().clone())
            .collect::<Vec<String>>();
        for node_address in node_addresses.iter() {
            cluster
                .remove_node(node_address)
                .ok_or_else(|| MetaStoreError::NodeNotFound)?;
        }

        cluster.bump_epoch();

        try_state!(Self::set_node_free(&mut self.all_nodes, &proxy_address,));
        Ok(())
    }

    pub fn remove_proxy(&mut self, proxy_address: String) -> Result<(), MetaStoreError> {
        self.all_nodes
            .get_mut(&proxy_address)
            .ok_or_else(|| MetaStoreError::HostResourceNotFound)
            .and_then(|node_resource| {
                if node_resource.cluster_name.is_some() {
                    return Err(MetaStoreError::InUse);
                }
                Ok(())
            })?;
        self.all_nodes
            .remove(&proxy_address)
            .map(|_| ())
            .ok_or_else(|| MetaStoreError::NodeResourceNotFound)
    }

    fn set_node_free(
        all_nodes: &mut HashMap<String, NodeResource>,
        proxy_address: &str,
    ) -> Result<(), MetaStoreError> {
        let node_resource = all_nodes
            .get_mut(proxy_address)
            .ok_or_else(|| MetaStoreError::HostResourceNotFound)?;
        node_resource.cluster_name = None;
        Ok(())
    }

    pub fn migrate_slots(
        &mut self,
        cluster_name: String,
        src_node_address: String,
        dst_node_address: String,
        migration_type: MigrationType,
    ) -> Result<(), MetaStoreError> {
        let cluster = self
            .clusters
            .get_mut(&cluster_name)
            .ok_or(MetaStoreError::ClusterNotFound)?;

        let (slot_ranges, src_proxy_address) = cluster
            .get_node(&src_node_address)
            .ok_or(MetaStoreError::NodeNotFound)
            .and_then(
                |node| match (node.get_role(), node.get_slots().is_empty()) {
                    (Role::Master, false) => {
                        Ok((node.get_slots().clone(), node.get_proxy_address().clone()))
                    }
                    (Role::Replica, _) => Err(MetaStoreError::InvalidRole),
                    (_, true) => Err(MetaStoreError::SlotEmpty),
                },
            )?;
        let dst_proxy_address = cluster
            .get_node(&dst_node_address)
            .ok_or(MetaStoreError::NodeNotFound)
            .and_then(|node| {
                if node.get_role() == Role::Master {
                    Ok(node.get_proxy_address().clone())
                } else {
                    Err(MetaStoreError::InvalidRole)
                }
            })?;

        if src_proxy_address == dst_proxy_address {
            return Err(MetaStoreError::SameHost);
        }

        cluster.bump_epoch();

        let migration_meta = MigrationMeta {
            epoch: cluster.get_epoch(),
            src_proxy_address,
            src_node_address: src_node_address.clone(),
            dst_proxy_address,
            dst_node_address: dst_node_address.clone(),
        };

        let (src_slot_ranges, dst_slot_ranges) = match migration_type {
            MigrationType::All => {
                let (mut migrating_slot_ranges, mut free_slot_ranges) =
                    Self::move_slot_ranges(slot_ranges);
                let mut src_new_migrating_slots = free_slot_ranges.clone();
                for slot_range in src_new_migrating_slots.iter_mut() {
                    slot_range.tag = SlotRangeTag::Migrating(migration_meta.clone());
                }
                for slot_range in free_slot_ranges.iter_mut() {
                    slot_range.tag = SlotRangeTag::Importing(migration_meta.clone());
                }
                migrating_slot_ranges.extend_from_slice(&src_new_migrating_slots);
                (migrating_slot_ranges, free_slot_ranges)
            }
            MigrationType::Half => {
                let (mut src_slot_ranges, mut dst_slot_ranges, migrating_slot_ranges) =
                    Self::split_slot_ranges(slot_ranges);

                let mut src_new_migrating_slots = dst_slot_ranges.clone();
                for slot_range in src_new_migrating_slots.iter_mut() {
                    slot_range.tag = SlotRangeTag::Migrating(migration_meta.clone());
                }
                for slot_range in dst_slot_ranges.iter_mut() {
                    slot_range.tag = SlotRangeTag::Importing(migration_meta.clone());
                }
                src_slot_ranges.extend_from_slice(&src_new_migrating_slots);
                src_slot_ranges.extend_from_slice(&migrating_slot_ranges);
                (src_slot_ranges, dst_slot_ranges)
            }
        };

        {
            let src_node = try_state!(cluster
                .get_mut_node(&src_node_address)
                .ok_or_else(|| MetaStoreError::NodeNotFound));
            *src_node.get_mut_slots() = src_slot_ranges;
        }
        {
            let dst_node = try_state!(cluster
                .get_mut_node(&dst_node_address)
                .ok_or_else(|| MetaStoreError::NodeNotFound));
            *dst_node.get_mut_slots() = dst_slot_ranges;
        }

        Ok(())
    }

    pub fn commit_migration(&mut self, task: MigrationTaskMeta) -> Result<(), MetaStoreError> {
        debug!("MetaStore::commit_migration {:?}", task);
        let MigrationTaskMeta {
            db_name,
            slot_range,
        } = task;

        let cluster = self
            .clusters
            .get_mut(&db_name)
            .ok_or(MetaStoreError::ClusterNotFound)?;

        let migration_meta = slot_range
            .tag
            .get_migration_meta()
            .cloned()
            .ok_or_else(|| MetaStoreError::InvalidRequest)?;

        {
            let src = cluster
                .get_node(&migration_meta.src_node_address)
                .ok_or(MetaStoreError::NodeNotFound)?;
            src.get_slots()
                .iter()
                .find(|sr| sr.meta_eq(&slot_range))
                .ok_or(MetaStoreError::SlotRangeNotFound)?;
        }
        {
            let dst = cluster
                .get_node(&migration_meta.dst_node_address)
                .ok_or(MetaStoreError::NodeNotFound)?;
            dst.get_slots()
                .iter()
                .find(|sr| sr.meta_eq(&slot_range))
                .ok_or(MetaStoreError::SlotRangeNotFound)?;
        }

        {
            let src = try_state!(cluster
                .get_mut_node(&migration_meta.src_node_address)
                .ok_or(MetaStoreError::NodeNotFound));
            src.get_mut_slots().retain(|sr| !sr.meta_eq(&slot_range));
        }
        {
            let dst = try_state!(cluster
                .get_mut_node(&migration_meta.dst_node_address)
                .ok_or(MetaStoreError::NodeNotFound));
            {
                let dst_slot_range = try_state!(dst
                    .get_mut_slots()
                    .iter_mut()
                    .find(|sr| sr.meta_eq(&slot_range))
                    .ok_or(MetaStoreError::SlotRangeNotFound));
                dst_slot_range.tag = SlotRangeTag::None;
            }
        }

        cluster.bump_epoch();

        Ok(())
    }

    pub fn stop_migrations(
        &mut self,
        cluster_name: String,
        src_node_address: String,
        dst_node_address: String,
    ) -> Result<(), MetaStoreError> {
        let cluster = self
            .clusters
            .get_mut(&cluster_name)
            .ok_or(MetaStoreError::ClusterNotFound)?;

        cluster
            .get_mut_node(&src_node_address)
            .ok_or(MetaStoreError::NodeNotFound)
            .and_then(
                |node| match (node.get_role(), node.get_slots().is_empty()) {
                    (Role::Master, false) => {
                        for slot_range in node.get_mut_slots().iter_mut() {
                            match slot_range.tag {
                                SlotRangeTag::Migrating(ref meta)
                                    if meta.dst_node_address == dst_node_address => {}
                                _ => continue,
                            }
                            slot_range.tag = SlotRangeTag::None;
                        }
                        Ok(())
                    }
                    (Role::Replica, _) => Err(MetaStoreError::InvalidRole),
                    (_, true) => Err(MetaStoreError::SlotEmpty),
                },
            )?;

        cluster
            .get_mut_node(&dst_node_address)
            .ok_or(MetaStoreError::NodeNotFound)
            .and_then(
                |node| match (node.get_role(), node.get_slots().is_empty()) {
                    (Role::Master, false) => {
                        let slot_ranges = node
                            .get_slots()
                            .iter()
                            .filter(|slot_range| match slot_range.tag {
                                SlotRangeTag::Importing(ref meta) => {
                                    meta.src_node_address != src_node_address
                                }
                                _ => true,
                            })
                            .cloned()
                            .collect();
                        *node.get_mut_slots() = slot_ranges;
                        Ok(())
                    }
                    (Role::Replica, _) => Err(MetaStoreError::InvalidRole),
                    (_, true) => Err(MetaStoreError::SlotEmpty),
                },
            )?;

        cluster.bump_epoch();

        Ok(())
    }

    pub fn assign_replica(
        &mut self,
        cluster_name: String,
        master_node_address: String,
        replica_node_address: String,
    ) -> Result<(), MetaStoreError> {
        let cluster = self
            .clusters
            .get_mut(&cluster_name)
            .ok_or(MetaStoreError::ClusterNotFound)?;

        let master_proxy_address = cluster
            .get_node(&master_node_address)
            .ok_or(MetaStoreError::NodeNotFound)
            .and_then(|node| {
                if node.get_role() == Role::Master {
                    Ok(node)
                } else {
                    Err(MetaStoreError::InvalidRole)
                }
            })
            .map(|node| node.get_proxy_address().clone())?;

        let replica_proxy_address = cluster
            .get_node(&replica_node_address)
            .ok_or(MetaStoreError::NodeNotFound)
            .and_then(|node| {
                if node.get_role() != Role::Master {
                    return Err(MetaStoreError::InvalidRole);
                }
                if !node.get_slots().is_empty() {
                    return Err(MetaStoreError::SlotNotEmpty);
                }
                if !node.get_repl_meta().get_peers().is_empty() {
                    return Err(MetaStoreError::PeerNotEmpty);
                }
                Ok(node.get_proxy_address().clone())
            })?;

        if master_proxy_address == replica_proxy_address {
            return Err(MetaStoreError::SameHost);
        }

        {
            let master = try_state!(cluster
                .get_mut_node(&master_node_address)
                .ok_or(MetaStoreError::NodeNotFound));
            master.get_mut_repl().add_peer(ReplPeer {
                node_address: replica_node_address.clone(),
                proxy_address: replica_proxy_address.clone(),
            });
        }

        {
            let replica = try_state!(cluster
                .get_mut_node(&replica_node_address)
                .ok_or_else(|| MetaStoreError::NodeNotFound));
            replica.get_mut_repl().add_peer(ReplPeer {
                node_address: master_node_address.clone(),
                proxy_address: master_proxy_address.clone(),
            });
            replica.get_mut_repl().set_role(Role::Replica);
        }

        cluster.bump_epoch();
        Ok(())
    }

    pub fn validate(&self) -> Result<(), InconsistentError> {
        self.validate_all_nodes()?;
        self.validate_cluster_nodes()?;
        self.validate_peers()?;
        self.validate_slots()?;
        Ok(())
    }

    pub fn validate_all_nodes(&self) -> Result<(), InconsistentError> {
        let all_nodes = &self.all_nodes;
        let clusters = &self.clusters;
        for (proxy_address, node_resource) in all_nodes.iter() {
            let cluster_name = match &node_resource.cluster_name {
                Some(cluster_name) => cluster_name,
                None => continue,
            };
            let cluster =
                clusters
                    .get(cluster_name)
                    .ok_or_else(|| InconsistentError::ClusterNotFound {
                        cluster_name: cluster_name.clone(),
                    })?;
            for node_address in node_resource.node_addresses.iter() {
                let node = cluster.get_node(node_address).ok_or_else(|| {
                    InconsistentError::NodeNotFoundInCluster {
                        proxy_address: proxy_address.clone(),
                        node_address: node_address.clone(),
                    }
                })?;
                if node.get_proxy_address() != proxy_address {
                    return Err(InconsistentError::InvalidProxyAddress {
                        node_address: node_address.clone(),
                        expected_proxy_address: proxy_address.clone(),
                        unexpected_proxy_address: node.get_proxy_address().clone(),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn validate_cluster_nodes(&self) -> Result<(), InconsistentError> {
        let all_nodes = &self.all_nodes;
        let clusters = &self.clusters;
        for (cluster_name, cluster) in clusters.iter() {
            if cluster_name != cluster.get_name() {
                return Err(InconsistentError::InvalidClusterName {
                    expected_name: cluster_name.clone(),
                    unexpected_name: cluster.get_name().clone(),
                });
            }
            for node in cluster.get_nodes().iter() {
                if node.get_cluster_name() != cluster_name {
                    return Err(InconsistentError::InvalidClusterName {
                        expected_name: cluster_name.clone(),
                        unexpected_name: node.get_cluster_name().clone(),
                    });
                }
                let proxy_address = node.get_proxy_address();
                let node_address = node.get_address();
                let node_resource = all_nodes.get(proxy_address).ok_or_else(|| {
                    InconsistentError::NodeNotFoundInAllNodes {
                        proxy_address: proxy_address.clone(),
                        node_address: node_address.clone(),
                    }
                })?;
                node_resource
                    .node_addresses
                    .iter()
                    .find(|node| *node == node_address)
                    .ok_or_else(|| InconsistentError::NodeNotFoundInAllNodes {
                        proxy_address: proxy_address.clone(),
                        node_address: node_address.clone(),
                    })?;
            }
        }
        Ok(())
    }

    pub fn validate_peers(&self) -> Result<(), InconsistentError> {
        for cluster in self.clusters.values() {
            for node in cluster.get_nodes().iter() {
                let peers = node.get_repl_meta().get_peers();
                for peer in peers.iter() {
                    let peer_node = cluster.get_node(&peer.node_address).ok_or_else(|| {
                        InconsistentError::PeerNotFound {
                            proxy_address: peer.proxy_address.clone(),
                            node_address: peer.node_address.clone(),
                        }
                    })?;
                    if *peer_node.get_proxy_address() != peer.proxy_address {
                        return Err(InconsistentError::InvalidProxyAddress {
                            node_address: peer.node_address.clone(),
                            expected_proxy_address: peer.proxy_address.clone(),
                            unexpected_proxy_address: peer_node.get_proxy_address().clone(),
                        });
                    }
                    if node.get_role() == peer_node.get_role() {
                        return Err(InconsistentError::SameRole {
                            node_address: node.get_address().clone(),
                            node_address_peer: peer_node.get_address().clone(),
                        });
                    }
                }
                if node.get_role() == Role::Replica && !node.get_slots().is_empty() {
                    return Err(InconsistentError::ReplicaHasSlots {
                        proxy_address: node.get_proxy_address().clone(),
                        node_address: node.get_address().clone(),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn validate_slots(&self) -> Result<(), InconsistentError> {
        for cluster in self.clusters.values() {
            for node in cluster.get_nodes().iter() {
                let slots = node.get_slots();
                for slot_range in slots.iter() {
                    match &slot_range.tag {
                        SlotRangeTag::None => continue,
                        SlotRangeTag::Migrating(meta) => {
                            let peer_node =
                                cluster.get_node(&meta.dst_node_address).ok_or_else(|| {
                                    InconsistentError::MigrationPeerNotFound {
                                        node_address: meta.dst_node_address.clone(),
                                        proxy_address: meta.dst_proxy_address.clone(),
                                    }
                                })?;
                            if *peer_node.get_proxy_address() != meta.dst_proxy_address {
                                return Err(InconsistentError::MigrationPeerNotFound {
                                    node_address: meta.dst_node_address.clone(),
                                    proxy_address: meta.dst_proxy_address.clone(),
                                });
                            }
                        }
                        SlotRangeTag::Importing(meta) => {
                            let peer_node =
                                cluster.get_node(&meta.src_node_address).ok_or_else(|| {
                                    InconsistentError::MigrationPeerNotFound {
                                        node_address: meta.src_node_address.clone(),
                                        proxy_address: meta.src_proxy_address.clone(),
                                    }
                                })?;
                            if *peer_node.get_proxy_address() != meta.src_proxy_address {
                                return Err(InconsistentError::MigrationPeerNotFound {
                                    node_address: meta.src_node_address.clone(),
                                    proxy_address: meta.src_proxy_address.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn consume_node_slot(&mut self, cluster_name: &str) -> Result<Vec<NodeSlot>, MetaStoreError> {
        let failures = self.failures.clone();

        let (proxy_address, node_resource) = self
            .all_nodes
            .iter_mut()
            .filter(|(proxy_address, node_resource)| {
                !failures.contains_key(*proxy_address) && node_resource.cluster_name.is_none()
            })
            .fold1(
                |(max_proxy_address, max_node_resource), (proxy_address, node_resource)| {
                    let max_free_num = max_node_resource.node_addresses.len();
                    let curr_free_num = node_resource.node_addresses.len();
                    if curr_free_num > max_free_num {
                        (proxy_address, node_resource)
                    } else {
                        (max_proxy_address, max_node_resource)
                    }
                },
            )
            .ok_or_else(|| MetaStoreError::NoAvailableResource)?;

        node_resource.cluster_name = Some(cluster_name.to_string());

        Ok(node_resource
            .node_addresses
            .iter()
            .map(|node_address| NodeSlot {
                proxy_address: proxy_address.clone(),
                node_address: node_address.clone(),
            })
            .collect())
    }

    fn move_slot_ranges(slot_ranges: Vec<SlotRange>) -> (Vec<SlotRange>, Vec<SlotRange>) {
        let migrating_slot_ranges = slot_ranges
            .iter()
            .filter(|slot_range| slot_range.tag != SlotRangeTag::None)
            .cloned()
            .collect();
        let free_slot_ranges = slot_ranges
            .into_iter()
            .filter(|slot_range| slot_range.tag == SlotRangeTag::None)
            .collect();
        (migrating_slot_ranges, free_slot_ranges)
    }

    fn split_slot_ranges(
        slot_ranges: Vec<SlotRange>,
    ) -> (Vec<SlotRange>, Vec<SlotRange>, Vec<SlotRange>) {
        let migrating_slot_ranges: Vec<SlotRange> = slot_ranges
            .iter()
            .filter(|slot_range| slot_range.tag != SlotRangeTag::None)
            .cloned()
            .collect();

        let free_slot_ranges: Vec<SlotRange> = slot_ranges
            .into_iter()
            .filter(|slot_range| slot_range.tag == SlotRangeTag::None)
            .collect();

        let slot_num: usize = free_slot_ranges
            .iter()
            .map(|slot_range| slot_range.end - slot_range.start + 1)
            .sum();
        let half_slot_sum = slot_num / 2;
        let mut src_slot_ranges = vec![];
        let mut dst_slot_ranges = vec![];

        let mut src_slot_num = 0;
        for slot_range in free_slot_ranges.into_iter() {
            if src_slot_num >= half_slot_sum {
                dst_slot_ranges.push(slot_range);
                continue;
            }

            let num = slot_range.end - slot_range.start + 1;
            if src_slot_num + num <= half_slot_sum {
                src_slot_num += num;
                src_slot_ranges.push(slot_range);
            } else {
                let src_num = half_slot_sum - src_slot_num;
                // assert num > src_num > 0
                src_slot_ranges.push(SlotRange {
                    start: slot_range.start,
                    end: slot_range.start + src_num - 1,
                    tag: SlotRangeTag::None,
                });
                // assert start + src_num < start + num = end
                dst_slot_ranges.push(SlotRange {
                    start: slot_range.start + src_num,
                    end: slot_range.end,
                    tag: SlotRangeTag::None,
                })
            }
        }
        (src_slot_ranges, dst_slot_ranges, migrating_slot_ranges)
    }

    pub fn replace_failed_proxy(
        &mut self,
        failed_proxy_address: String,
    ) -> Result<Host, MetaStoreError> {
        let (cluster_name, node_addresses) = {
            let node_resource = self
                .all_nodes
                .get(&failed_proxy_address)
                .ok_or(MetaStoreError::HostNotFound)?;
            (
                node_resource
                    .cluster_name
                    .clone()
                    .ok_or_else(|| MetaStoreError::NotInUse)?,
                node_resource
                    .node_addresses
                    .iter()
                    .cloned()
                    .collect::<Vec<String>>(),
            )
        };

        for node_address in node_addresses.iter() {
            match self.takeover_master(&cluster_name, &node_address) {
                Err(MetaStoreError::NotMaster) => (),
                Err(MetaStoreError::NoPeer) => (),
                Err(unexpected_errors) => {
                    error!(
                        "unexpected errors when replacing failed nodes {:?}",
                        unexpected_errors
                    );
                    return Err(unexpected_errors);
                }
                Ok(_) => continue,
            }
        }

        let node_slots = self.consume_new_proxy(&cluster_name)?;
        if node_slots.len() != node_addresses.len() {
            if let Some(node_resource) = self.all_nodes.get_mut(&failed_proxy_address) {
                node_resource.cluster_name = None;
            }
            let cluster = try_state!(self
                .clusters
                .get_mut(&cluster_name)
                .ok_or_else(|| MetaStoreError::ClusterNotFound));
            for node_slot in node_slots.iter() {
                try_state!(cluster
                    .remove_node(&node_slot.node_address)
                    .ok_or_else(|| MetaStoreError::NodeNotFound));
            }
            return Err(MetaStoreError::InvalidNodeNum);
        }

        let new_proxy_address = node_slots
            .iter()
            .next()
            .map(|node_slot| node_slot.proxy_address.clone())
            .ok_or_else(|| MetaStoreError::InvalidNodeNum)?;

        for (node_address, node_slot) in node_addresses.iter().zip(node_slots.into_iter()) {
            try_state!(self.replace_node(&cluster_name, node_address, node_slot));
        }

        try_state!(self.remove_proxy_from_cluster_helper(
            &cluster_name,
            &failed_proxy_address,
            false
        ));
        let host = try_state!(self
            .get_host_by_address(&new_proxy_address)
            .ok_or_else(|| MetaStoreError::HostNotFound));
        Ok(host)
    }

    fn takeover_master(
        &mut self,
        cluster_name: &str,
        node_address: &str,
    ) -> Result<Node, MetaStoreError> {
        let cluster = self
            .clusters
            .get_mut(cluster_name)
            .ok_or(MetaStoreError::ClusterNotFound)?;

        let mut master_node = cluster
            .get_node(node_address)
            .ok_or_else(|| MetaStoreError::NodeNotFound)?
            .clone();

        if master_node.get_repl_meta().get_role() == Role::Replica {
            return Err(MetaStoreError::NotMaster);
        }

        info!("start to takeover {:?}", master_node);
        let peer = master_node
            .get_repl_meta()
            .get_peers()
            .iter()
            .next()
            .cloned()
            .ok_or_else(|| MetaStoreError::NoPeer)?;
        let peer_node_address = peer.node_address.clone();
        let mut replica_node = try_state!(cluster
            .get_node(&peer.node_address)
            .cloned()
            .ok_or_else(|| MetaStoreError::NodeNotFound));

        cluster.bump_epoch();

        try_state!(Self::change_migration_peer(
            cluster,
            &master_node,
            &peer.node_address,
            &peer.proxy_address,
        ));

        master_node.get_mut_repl().set_role(Role::Replica);
        replica_node.get_mut_repl().set_role(Role::Master);
        let slot_ranges = Self::gen_new_slots(
            master_node.get_slots().clone(),
            replica_node.get_address(),
            replica_node.get_proxy_address(),
            cluster.get_epoch(),
        );
        *replica_node.get_mut_slots() = slot_ranges;
        *master_node.get_mut_slots() = vec![];

        try_state!(Self::update_peer(cluster, &master_node, &replica_node));

        *try_state!(cluster
            .get_mut_node(&node_address)
            .ok_or_else(|| MetaStoreError::NodeNotFound)) = master_node.clone();
        *try_state!(cluster
            .get_mut_node(&peer_node_address)
            .ok_or_else(|| MetaStoreError::NodeNotFound)) = replica_node.clone();

        Ok(replica_node)
    }

    fn change_migration_peer(
        cluster: &mut Cluster,
        master_node: &Node,
        new_node_address: &str,
        new_proxy_address: &str,
    ) -> Result<(), MetaStoreError> {
        let epoch = cluster.get_epoch();
        let slot_ranges = master_node.get_slots();
        for slot_range in slot_ranges.iter() {
            let start = slot_range.start;
            let end = slot_range.end;
            match &slot_range.tag {
                SlotRangeTag::None => continue,
                SlotRangeTag::Migrating(meta) => {
                    let mut new_meta = meta.clone();
                    new_meta.src_node_address = new_node_address.to_string();
                    new_meta.src_proxy_address = new_proxy_address.to_string();
                    new_meta.epoch = epoch;
                    let peer_node = try_state!(cluster
                        .get_mut_node(&meta.dst_node_address)
                        .ok_or_else(|| MetaStoreError::NodeNotFound));
                    let sr = try_state!(peer_node
                        .get_mut_slots()
                        .iter_mut()
                        .find(|sr| sr.start == start && sr.end == end)
                        .ok_or_else(|| MetaStoreError::SlotRangeNotFound));
                    *try_state!(sr
                        .tag
                        .get_mut_migration_meta()
                        .ok_or_else(|| MetaStoreError::NotMigrating)) = new_meta.clone();
                }
                SlotRangeTag::Importing(meta) => {
                    let mut new_meta = meta.clone();
                    new_meta.dst_node_address = new_node_address.to_string();
                    new_meta.dst_proxy_address = new_proxy_address.to_string();
                    new_meta.epoch = epoch;
                    let peer_node = try_state!(cluster
                        .get_mut_node(&meta.src_node_address)
                        .ok_or_else(|| MetaStoreError::NodeNotFound));
                    let sr = try_state!(peer_node
                        .get_mut_slots()
                        .iter_mut()
                        .find(|sr| sr.start == start && sr.end == end)
                        .ok_or_else(|| MetaStoreError::SlotRangeNotFound));
                    *try_state!(sr
                        .tag
                        .get_mut_migration_meta()
                        .ok_or_else(|| MetaStoreError::NotMigrating)) = new_meta.clone();
                }
            };
        }
        Ok(())
    }

    fn update_peer(
        cluster: &mut Cluster,
        old_node: &Node,
        new_node: &Node,
    ) -> Result<(), MetaStoreError> {
        let old_peer = ReplPeer {
            node_address: old_node.get_address().clone(),
            proxy_address: old_node.get_proxy_address().clone(),
        };
        let new_peer = ReplPeer {
            node_address: new_node.get_address().clone(),
            proxy_address: new_node.get_proxy_address().clone(),
        };
        for repl in old_node.get_repl_meta().get_peers().iter() {
            if repl.node_address == new_peer.node_address {
                continue;
            }
            let mut peer = try_state!(cluster
                .get_mut_node(&repl.node_address)
                .ok_or_else(|| MetaStoreError::NodeNotFound));
            try_state!(peer
                .get_mut_repl()
                .remove_peer(&old_peer)
                .ok_or_else(|| MetaStoreError::NodeNotFound));
            peer.get_mut_repl().add_peer(new_peer.clone());
        }
        Ok(())
    }

    fn gen_new_slots(
        slot_ranges: Vec<SlotRange>,
        new_node_address: &str,
        new_proxy_address: &str,
        new_cluster_epoch: u64,
    ) -> Vec<SlotRange> {
        slot_ranges
            .into_iter()
            .map(|mut slot_range| {
                match slot_range.tag {
                    SlotRangeTag::None => (),
                    SlotRangeTag::Migrating(ref mut meta) => {
                        meta.src_node_address = new_node_address.to_string();
                        meta.src_proxy_address = new_proxy_address.to_string();
                        meta.epoch = new_cluster_epoch;
                    }
                    SlotRangeTag::Importing(ref mut meta) => {
                        meta.dst_node_address = new_node_address.to_string();
                        meta.dst_proxy_address = new_proxy_address.to_string();
                        meta.epoch = new_cluster_epoch;
                    }
                }
                slot_range
            })
            .collect()
    }

    fn replace_node(
        &mut self,
        cluster_name: &str,
        node_address: &str,
        node_slot: NodeSlot,
    ) -> Result<Node, MetaStoreError> {
        let new_node = {
            let node = {
                let cluster = self
                    .clusters
                    .get(cluster_name)
                    .ok_or_else(|| MetaStoreError::ClusterNotFound)?;
                cluster
                    .get_node(node_address)
                    .cloned()
                    .ok_or_else(|| MetaStoreError::NodeNotFound)?
            };

            let NodeSlot {
                proxy_address,
                node_address,
            } = node_slot;

            let cluster = try_state!(self
                .clusters
                .get_mut(node.get_cluster_name())
                .ok_or(MetaStoreError::ClusterNotFound));

            cluster.bump_epoch();

            info!(
                "start to replace {:?} with {} {}",
                node, proxy_address, node_address
            );

            let new_slot_ranges = Self::gen_new_slots(
                node.get_slots().clone(),
                &node_address,
                &proxy_address,
                cluster.get_epoch(),
            );

            try_state!(cluster
                .remove_node(&node_address)
                .ok_or_else(|| MetaStoreError::NodeNotFound));

            let new_node = Node::new(
                node_address,
                proxy_address.clone(),
                node.get_cluster_name().clone(),
                new_slot_ranges,
                node.get_repl_meta().clone(),
            );

            if node.get_role() == Role::Master {
                info!("Replace a master");
                try_state!(Self::change_migration_peer(
                    cluster,
                    &node,
                    new_node.get_address(),
                    new_node.get_proxy_address(),
                ));
            }

            cluster.add_node(new_node.clone());
            try_state!(Self::update_peer(cluster, &node, &new_node));

            new_node
        };

        Ok(new_node)
    }

    fn consume_new_proxy(&mut self, cluster_name: &str) -> Result<Vec<NodeSlot>, MetaStoreError> {
        let nodes = self.auto_add_nodes(cluster_name.to_string())?;
        Ok(nodes
            .into_iter()
            .map(|node| NodeSlot {
                node_address: node.get_address().clone(),
                proxy_address: node.get_proxy_address().clone(),
            })
            .collect())
    }
}

#[derive(Debug)]
pub enum InconsistentError {
    ClusterNotFound {
        cluster_name: String,
    },
    NodeNotFoundInCluster {
        proxy_address: String,
        node_address: String,
    },
    InvalidProxyAddress {
        node_address: String,
        expected_proxy_address: String,
        unexpected_proxy_address: String,
    },
    InvalidClusterName {
        expected_name: String,
        unexpected_name: String,
    },
    NodeNotFoundInAllNodes {
        proxy_address: String,
        node_address: String,
    },
    PeerNotFound {
        proxy_address: String,
        node_address: String,
    },
    SameRole {
        node_address: String,
        node_address_peer: String,
    },
    ReplicaHasSlots {
        proxy_address: String,
        node_address: String,
    },
    MigrationPeerNotFound {
        proxy_address: String,
        node_address: String,
    },
}

impl fmt::Display for InconsistentError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for InconsistentError {
    fn description(&self) -> &str {
        match self {
            InconsistentError::ClusterNotFound { .. } => "CLUSTER_NOT_FOUND",
            InconsistentError::NodeNotFoundInCluster { .. } => "NODE_NOT_FOUND_IN_CLUSTER",
            InconsistentError::InvalidProxyAddress { .. } => "INVALID_PROXY_ADDRESS",
            InconsistentError::InvalidClusterName { .. } => "INVALID_CLUSTER_NAME",
            InconsistentError::NodeNotFoundInAllNodes { .. } => "NODE_NOT_FOUND_IN_ALL_NODES",
            InconsistentError::PeerNotFound { .. } => "PEER_NOT_FOUND",
            InconsistentError::SameRole { .. } => "SAME_ROLE",
            InconsistentError::ReplicaHasSlots { .. } => "REPLICA_HAS_SLOTS",
            InconsistentError::MigrationPeerNotFound { .. } => "MIGRATION_PEER_NOT_FOUND",
        }
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}

#[derive(Debug)]
pub enum MetaStoreError {
    InUse,
    NotInUse,
    NoAvailableResource,
    InvalidState,
    InvalidRole,
    SlotNotEmpty,
    SlotEmpty,
    PeerNotEmpty,
    SameHost,
    AlreadyExisted,
    InvalidRequest,
    SlotRangeNotFound,
    ClusterNotFound,
    NodeNotFound,
    HostNotFound,
    HostResourceNotFound,
    NodeResourceNotFound,
    NotMaster,
    NoPeer,
    NotMigrating,
    MismatchEpoch,
    InvalidNodeNum,
}

impl fmt::Display for MetaStoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for MetaStoreError {
    fn description(&self) -> &str {
        match self {
            MetaStoreError::InUse => "IN_USE",
            MetaStoreError::NotInUse => "NOT_IN_USE",
            MetaStoreError::NoAvailableResource => "NO_AVAILABLE_RESOURCE",
            MetaStoreError::InvalidState => "INVALID_STATE",
            MetaStoreError::InvalidRole => "INVALID_ROLE",
            MetaStoreError::SlotNotEmpty => "SLOT_NOT_EMPTY",
            MetaStoreError::SlotEmpty => "SLOT_EMPTY",
            MetaStoreError::PeerNotEmpty => "PEER_NOT_EMPTY",
            MetaStoreError::SameHost => "SAME_HOST",
            MetaStoreError::AlreadyExisted => "ALREADY_EXISTED",
            MetaStoreError::InvalidRequest => "INVALID_REQUEST",
            MetaStoreError::SlotRangeNotFound => "SLOT_RANGE_NOT_FOUND",
            MetaStoreError::ClusterNotFound => "CLUSTER_NOT_FOUND",
            MetaStoreError::NodeNotFound => "NODE_NOT_FOUND",
            MetaStoreError::HostNotFound => "HOST_NOT_FOUND",
            MetaStoreError::HostResourceNotFound => "HOST_RESOURCE_NOT_FOUND",
            MetaStoreError::NodeResourceNotFound => "NODE_RESOURCE_NOT_FOUND",
            MetaStoreError::NotMaster => "NOT_MASTER",
            MetaStoreError::NoPeer => "NOT_PEER",
            MetaStoreError::NotMigrating => "NOT_MIGRATING",
            MetaStoreError::MismatchEpoch => "MISMATCH_EPOCH",
            MetaStoreError::InvalidNodeNum => "INVALID_NODE_NUM",
        }
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}
