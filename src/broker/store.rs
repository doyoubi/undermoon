use ::common::cluster::{
    Cluster, Host, MigrationTaskMeta, Node, ReplMeta, ReplPeer, SlotRange, SlotRangeTag,
};
use ::common::utils::SLOT_NUM;
use chrono::{DateTime, NaiveDateTime, Utc};
use common::cluster::{MigrationMeta, Role};
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone)]
pub struct NodeSlot {
    pub proxy_address: String,
    pub node_address: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MigrationType {
    All,
    Half,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct MetaStore {
    clusters: HashMap<String, Cluster>,
    hosts: HashMap<String, Host>,
    // proxy_address => node_address => free
    all_nodes: HashMap<String, HashMap<String, bool>>,
    failures: HashMap<String, i64>,
}

impl Default for MetaStore {
    fn default() -> Self {
        Self {
            clusters: HashMap::new(),
            hosts: HashMap::new(),
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
        self.hosts.keys().cloned().collect()
    }

    pub fn get_host_by_address(&self, address: &str) -> Option<Host> {
        self.hosts.get(address).cloned()
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
        let node_map = self
            .all_nodes
            .entry(proxy_address.clone())
            .or_insert_with(HashMap::new);
        for node in nodes.into_iter() {
            node_map.entry(node).or_insert(true);
        }
        Ok(())
    }

    pub fn add_cluster(&mut self, cluster_name: String) -> Result<(), MetaStoreError> {
        if self.clusters.contains_key(&cluster_name) {
            return Err(MetaStoreError::AlreadyExisted);
        }

        let NodeSlot {
            proxy_address,
            node_address,
        } = self.consume_node_slot()?;

        let slots = SlotRange {
            start: 0,
            end: SLOT_NUM - 1,
            tag: SlotRangeTag::None,
        };
        let repl = ReplMeta::new(Role::Master, vec![]);
        let init_node = Node::new(
            node_address,
            proxy_address.clone(),
            cluster_name.clone(),
            vec![slots],
            repl,
        );

        let cluster = Cluster::new(cluster_name.clone(), 0, vec![init_node.clone()]);
        let host = self
            .hosts
            .entry(proxy_address.clone())
            .or_insert_with(|| Host::new(proxy_address.clone(), 0, vec![]));
        host.add_node(init_node);
        host.bump_epoch();

        self.clusters.insert(cluster_name, cluster);
        Ok(())
    }

    pub fn remove_cluster(&mut self, cluster_name: String) -> Result<(), MetaStoreError> {
        let cluster = self
            .clusters
            .remove(&cluster_name)
            .ok_or(MetaStoreError::ClusterNotFound)?;

        for node in cluster.into_nodes().iter() {
            match self
                .all_nodes
                .get_mut(node.get_proxy_address())
                .and_then(|nodes| nodes.get_mut(node.get_address()))
            {
                Some(free) => *free = true,
                None => error!(
                    "Invalid meta: cannot find {} {} in all_hosts",
                    cluster_name,
                    node.get_proxy_address()
                ),
            }
            let found = self
                .hosts
                .get_mut(node.get_proxy_address())
                .and_then(|host| {
                    host.bump_epoch();
                    host.remove_node(node.get_address())
                })
                .is_some();
            if !found {
                error!(
                    "Invalid meta: cannot find {} {} in hosts",
                    cluster_name,
                    node.get_proxy_address()
                );
            }
        }
        Ok(())
    }

    pub fn auto_add_node(&mut self, cluster_name: String) -> Result<Node, MetaStoreError> {
        if self.clusters.get(&cluster_name).is_none() {
            return Err(MetaStoreError::ClusterNotFound);
        }

        let node_slot = self.consume_node_slot()?;
        let node = try_state!(self.add_node(cluster_name, node_slot));
        Ok(node)
    }

    pub fn add_node(
        &mut self,
        cluster_name: String,
        node_slot: NodeSlot,
    ) -> Result<Node, MetaStoreError> {
        if self.clusters.get(&cluster_name).is_none() {
            return Err(MetaStoreError::ClusterNotFound);
        }

        let NodeSlot {
            proxy_address,
            node_address,
        } = node_slot;

        {
            let free = self
                .all_nodes
                .get_mut(&proxy_address)
                .ok_or_else(|| MetaStoreError::HostResourceNotFound)?
                .get_mut(&node_address)
                .ok_or_else(|| MetaStoreError::NodeResourceNotFound)?;
            *free = false;
        }

        let repl = ReplMeta::new(Role::Master, vec![]);
        let new_node = Node::new(
            node_address.clone(),
            proxy_address.clone(),
            cluster_name.clone(),
            vec![],
            repl,
        );

        let cluster = try_state!(self
            .clusters
            .get_mut(&cluster_name)
            .ok_or_else(|| MetaStoreError::ClusterNotFound));
        cluster.add_node(new_node.clone());
        cluster.bump_epoch();

        let host = self
            .hosts
            .entry(proxy_address.clone())
            .or_insert_with(|| Host::new(proxy_address.clone(), 0, vec![]));
        host.add_node(new_node.clone());

        Ok(new_node)
    }

    pub fn remove_node_from_cluster(
        &mut self,
        cluster_name: String,
        node_slot: NodeSlot,
    ) -> Result<(), MetaStoreError> {
        self.remove_node_from_cluster_helper(cluster_name.clone(), node_slot, false)
    }

    pub fn remove_node_from_cluster_helper(
        &mut self,
        cluster_name: String,
        node_slot: NodeSlot,
        force: bool,
    ) -> Result<(), MetaStoreError> {
        let cluster = self
            .clusters
            .get_mut(&cluster_name)
            .ok_or(MetaStoreError::ClusterNotFound)?;

        let NodeSlot {
            proxy_address,
            node_address,
        } = node_slot.clone();

        if !force {
            cluster
                .get_node(&node_address)
                .ok_or_else(|| MetaStoreError::NodeNotFound)
                .and_then(|node| {
                    if node.get_slots().is_empty() {
                        Ok(())
                    } else {
                        Err(MetaStoreError::SlotNotEmpty)
                    }
                })?;
        }

        if cluster.remove_node(&node_address).is_none() {
            return Err(MetaStoreError::NodeNotFound);
        }

        cluster.bump_epoch();

        let empty = {
            let host = try_state!(self
                .hosts
                .get_mut(&proxy_address)
                .ok_or_else(|| MetaStoreError::HostNotFound));
            try_state!(host
                .remove_node(&node_address)
                .ok_or_else(|| MetaStoreError::NodeNotFound));

            host.bump_epoch();

            host.get_nodes().is_empty()
        };

        if empty {
            try_state!(self
                .hosts
                .remove(&proxy_address)
                .ok_or_else(|| MetaStoreError::HostNotFound));
        }

        try_state!(Self::set_node_free(
            &mut self.all_nodes,
            NodeSlot {
                proxy_address,
                node_address,
            },
        ));
        Ok(())
    }

    pub fn remove_node(&mut self, node_slot: NodeSlot) -> Result<(), MetaStoreError> {
        let NodeSlot {
            proxy_address,
            node_address,
        } = node_slot;
        self.all_nodes
            .get_mut(&proxy_address)
            .ok_or_else(|| MetaStoreError::HostResourceNotFound)
            .and_then(|nodes| {
                nodes
                    .get(&node_address)
                    .ok_or_else(|| MetaStoreError::NodeResourceNotFound)
                    .and_then(|free| {
                        if *free {
                            Ok(())
                        } else {
                            Err(MetaStoreError::InUse)
                        }
                    })?;
                try_state!(nodes
                    .remove(&node_address)
                    .map(|_| ())
                    .ok_or_else(|| MetaStoreError::NodeResourceNotFound));
                Ok(())
            })
    }

    fn set_node_free(
        all_nodes: &mut HashMap<String, HashMap<String, bool>>,
        node_slot: NodeSlot,
    ) -> Result<(), MetaStoreError> {
        let NodeSlot {
            proxy_address,
            node_address,
        } = node_slot;
        let nodes = all_nodes
            .get_mut(&proxy_address)
            .ok_or_else(|| MetaStoreError::HostResourceNotFound)?;
        let free = nodes
            .get_mut(&node_address)
            .ok_or_else(|| MetaStoreError::NodeResourceNotFound)?;
        if *free {
            return Err(MetaStoreError::NotInUse);
        }
        *free = true;
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
                .ok_or(MetaStoreError::NodeNotFound));
            *src_node.get_mut_slots() = src_slot_ranges;
            try_state!(Self::update_host_node(&mut self.hosts, src_node.clone()));
        }
        {
            let dst_node = try_state!(cluster.get_mut_node(&dst_node_address).ok_or_else(|| {
                error!("Invalid state: cannot find dst node");
                MetaStoreError::NodeNotFound
            }));
            *dst_node.get_mut_slots() = dst_slot_ranges;
            try_state!(Self::update_host_node(&mut self.hosts, dst_node.clone()));
        }

        Ok(())
    }

    pub fn commit_migration(&mut self, task: MigrationTaskMeta) -> Result<(), MetaStoreError> {
        debug!("MetaStore::commit_migration {:?}", task);
        let MigrationTaskMeta {
            db_name,
            slot_range,
        } = task;

        {
            let cluster = self
                .clusters
                .get_mut(&db_name)
                .ok_or(MetaStoreError::ClusterNotFound)?;

            let migration_meta = match slot_range.tag {
                SlotRangeTag::None => return Err(MetaStoreError::InvalidRequest),
                SlotRangeTag::Migrating(ref meta) => meta.clone(),
                SlotRangeTag::Importing(ref meta) => meta.clone(),
            };

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
                try_state!(Self::update_host_node(&mut self.hosts, src.clone()));
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
                try_state!(Self::update_host_node(&mut self.hosts, dst.clone()));
            }

            cluster.bump_epoch();
        }

        try_state!(self.bump_all_hosts(&db_name));
        Ok(())
    }

    pub fn stop_migrations(
        &mut self,
        cluster_name: String,
        src_node_address: String,
        dst_node_address: String,
    ) -> Result<(), MetaStoreError> {
        {
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

            {
                let src_node = try_state!(cluster
                    .get_node(&src_node_address)
                    .ok_or(MetaStoreError::NodeNotFound));
                try_state!(Self::update_host_node(&mut self.hosts, src_node.clone()));
            }
            {
                let dst_node = try_state!(cluster.get_node(&dst_node_address).ok_or_else(|| {
                    error!("Invalid state: cannot find dst node");
                    MetaStoreError::NodeNotFound
                }));
                try_state!(Self::update_host_node(&mut self.hosts, dst_node.clone()));
            }
        }

        try_state!(self.bump_all_hosts(&cluster_name));
        Ok(())
    }

    fn update_host_node(
        hosts: &mut HashMap<String, Host>,
        node: Node,
    ) -> Result<(), MetaStoreError> {
        let host = hosts
            .get_mut(node.get_proxy_address())
            .ok_or_else(|| MetaStoreError::HostNotFound)?;
        host.remove_node(node.get_address())
            .ok_or_else(|| MetaStoreError::HostNotFound)?;
        host.add_node(node);
        host.bump_epoch();
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
            try_state!(Self::update_host_node(&mut self.hosts, master.clone()));
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
            try_state!(Self::update_host_node(&mut self.hosts, replica.clone()));
        }

        cluster.bump_epoch();
        Ok(())
    }

    // TODO: implement validation
    //    pub fn validate(&self) {}

    fn consume_node_slot(&mut self) -> Result<NodeSlot, MetaStoreError> {
        let (proxy_address, nodes) = self
            .all_nodes
            .iter_mut()
            .fold1(|(max_proxy_address, max_nodes), (proxy_address, nodes)| {
                // the bool(free) conversion is guaranteed to be 1 or 0 in Rust
                let max_free_num: usize = max_nodes.iter().map(|(_, free)| *free as usize).sum();
                let curr_free_num: usize = nodes.iter().map(|(_, free)| *free as usize).sum();
                if curr_free_num > max_free_num {
                    (proxy_address, nodes)
                } else {
                    (max_proxy_address, max_nodes)
                }
            })
            .ok_or_else(|| MetaStoreError::NoAvailableResource)?;

        let (node_address, free) = nodes
            .iter_mut()
            .find(|(_, free)| **free)
            .ok_or_else(|| MetaStoreError::NoAvailableResource)?;
        *free = false;
        Ok(NodeSlot {
            proxy_address: proxy_address.clone(),
            node_address: node_address.clone(),
        })
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

    pub fn replace_failed_node(
        &mut self,
        curr_cluster_epoch: u64,
        node: Node,
    ) -> Result<Node, MetaStoreError> {
        {
            let cluster = self
                .clusters
                .get(node.get_cluster_name())
                .ok_or(MetaStoreError::ClusterNotFound)?;
            if cluster.get_epoch() != curr_cluster_epoch {
                return Err(MetaStoreError::MismatchEpoch);
            }
        }

        let old_node_address = node.get_address().clone();
        match self.takeover_master(node.clone()) {
            Err(MetaStoreError::NotMaster) => (),
            Err(MetaStoreError::NoPeer) => (),
            others => return others,
        }
        let new_node = self.replace_node(node)?;
        if self.failures.remove(&old_node_address).is_some() {
            info!("Remove failure {}", old_node_address);
        }
        Ok(new_node)
    }

    fn takeover_master(&mut self, master: Node) -> Result<Node, MetaStoreError> {
        let replica_node = {
            let cluster = self
                .clusters
                .get_mut(master.get_cluster_name())
                .ok_or(MetaStoreError::ClusterNotFound)?;

            let mut master_node = cluster
                .get_node(master.get_address())
                .ok_or_else(|| MetaStoreError::NodeNotFound)?
                .clone();

            if master_node.get_repl_meta().get_role() == Role::Replica {
                return Err(MetaStoreError::NotMaster);
            }

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
            );
            *replica_node.get_mut_slots() = slot_ranges;
            *master_node.get_mut_slots() = vec![];

            try_state!(Self::update_peer(
                cluster,
                &mut self.hosts,
                &master_node,
                &replica_node
            ));

            *try_state!(cluster
                .get_mut_node(master.get_address())
                .ok_or_else(|| MetaStoreError::NodeNotFound)) = master_node.clone();
            *try_state!(cluster
                .get_mut_node(&peer_node_address)
                .ok_or_else(|| MetaStoreError::NodeNotFound)) = replica_node.clone();

            try_state!(Self::update_host_node(&mut self.hosts, master_node.clone()));
            try_state!(Self::update_host_node(
                &mut self.hosts,
                replica_node.clone()
            ));
            replica_node
        };

        try_state!(self.bump_all_hosts(master.get_cluster_name()));

        Ok(replica_node)
    }

    fn change_migration_peer(
        cluster: &mut Cluster,
        master_node: &Node,
        new_node_address: &str,
        new_proxy_address: &str,
    ) -> Result<(), MetaStoreError> {
        let epoch = cluster.get_epoch();
        let mut slot_ranges = master_node.get_slots().clone();
        for slot_range in slot_ranges.iter_mut() {
            let start = slot_range.start;
            let end = slot_range.end;
            match &mut slot_range.tag {
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
                    *meta = new_meta;
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
                    *meta = new_meta;
                }
            };
        }
        Ok(())
    }

    fn update_peer(
        cluster: &mut Cluster,
        hosts: &mut HashMap<String, Host>,
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
            try_state!(Self::update_host_node(hosts, peer.clone()));
        }
        Ok(())
    }

    fn gen_new_slots(
        slot_ranges: Vec<SlotRange>,
        new_node_address: &str,
        new_proxy_address: &str,
    ) -> Vec<SlotRange> {
        slot_ranges
            .into_iter()
            .map(|mut slot_range| {
                match slot_range.tag {
                    SlotRangeTag::None => (),
                    SlotRangeTag::Migrating(ref mut meta) => {
                        meta.src_node_address = new_node_address.to_string();
                        meta.src_proxy_address = new_proxy_address.to_string();
                    }
                    SlotRangeTag::Importing(ref mut meta) => {
                        meta.dst_node_address = new_node_address.to_string();
                        meta.dst_proxy_address = new_proxy_address.to_string();
                    }
                }
                slot_range
            })
            .collect()
    }

    fn replace_node(&mut self, node: Node) -> Result<Node, MetaStoreError> {
        let new_node = {
            if self.clusters.get(node.get_cluster_name()).is_none() {
                return Err(MetaStoreError::ClusterNotFound);
            }

            let NodeSlot {
                proxy_address,
                node_address,
            } = self.consume_node_slot()?;

            let cluster = try_state!(self
                .clusters
                .get_mut(node.get_cluster_name())
                .ok_or(MetaStoreError::ClusterNotFound));

            let new_slot_ranges =
                Self::gen_new_slots(node.get_slots().clone(), &node_address, &proxy_address);

            let new_node = Node::new(
                node_address,
                proxy_address.clone(),
                node.get_cluster_name().clone(),
                new_slot_ranges,
                node.get_repl_meta().clone(),
            );

            if new_node.get_role() == Role::Master {
                try_state!(Self::change_migration_peer(
                    cluster,
                    &node,
                    new_node.get_address(),
                    new_node.get_proxy_address(),
                ));
            }

            try_state!(Self::update_peer(
                cluster,
                &mut self.hosts,
                &node,
                &new_node
            ));

            cluster.add_node(new_node.clone());
            cluster.bump_epoch();

            new_node
        };

        let cluster_name = node.get_cluster_name().clone();
        let old_slot = NodeSlot {
            proxy_address: node.get_proxy_address().clone(),
            node_address: node.get_address().clone(),
        };

        {
            let host = self
                .hosts
                .entry(new_node.get_proxy_address().clone())
                .or_insert_with(|| Host::new(new_node.get_proxy_address().clone(), 0, vec![]));
            host.add_node(new_node.clone());
            host.bump_epoch();
        }

        try_state!(self.remove_node_from_cluster_helper(cluster_name.clone(), old_slot, true));
        try_state!(self.bump_all_hosts(&cluster_name));

        Ok(new_node)
    }

    fn bump_all_hosts(&mut self, cluster_name: &str) -> Result<(), MetaStoreError> {
        let cluster = self
            .clusters
            .get(cluster_name)
            .ok_or(MetaStoreError::ClusterNotFound)?;
        let proxy_addresses = cluster
            .get_nodes()
            .iter()
            .map(Node::get_proxy_address)
            .cloned()
            .collect::<HashSet<String>>();
        for proxy_address in proxy_addresses.iter() {
            self.hosts
                .get_mut(proxy_address)
                .ok_or_else(|| MetaStoreError::HostNotFound)?
                .bump_epoch();
        }
        Ok(())
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
        }
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}
