use ::common::cluster::{Cluster, Host, Node, ReplMeta, ReplPeer, SlotRange, SlotRangeTag};
use ::common::utils::SLOT_NUM;
use common::cluster::{MigrationMeta, Role};
use std::collections::HashMap;
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
}

impl Default for MetaStore {
    fn default() -> Self {
        Self {
            clusters: HashMap::new(),
            hosts: HashMap::new(),
            all_nodes: HashMap::new(),
        }
    }
}

impl MetaStore {
    pub fn get_hosts(&self) -> Vec<String> {
        self.hosts.keys().cloned().collect()
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
            .ok_or(MetaStoreError::NotFound)?;

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
            return Err(MetaStoreError::NotFound);
        }

        let node_slot = self.consume_node_slot()?;
        self.add_node(cluster_name, node_slot)
    }

    pub fn add_node(
        &mut self,
        cluster_name: String,
        node_slot: NodeSlot,
    ) -> Result<Node, MetaStoreError> {
        if self.clusters.get(&cluster_name).is_none() {
            return Err(MetaStoreError::NotFound);
        }

        let NodeSlot {
            proxy_address,
            node_address,
        } = node_slot;

        let repl = ReplMeta::new(Role::Master, vec![]);
        let new_node = Node::new(
            node_address.clone(),
            proxy_address.clone(),
            cluster_name.clone(),
            vec![],
            repl,
        );

        let cluster = match self.clusters.get_mut(&cluster_name) {
            Some(cluster) => cluster,
            None => {
                error!("Invalid state: cannot find cluster {}", cluster_name);
                return Err(MetaStoreError::NotFound);
            }
        };
        cluster.add_node(new_node.clone());
        cluster.bump_epoch();

        let host = self
            .hosts
            .entry(proxy_address.clone())
            .or_insert_with(|| Host::new(proxy_address.clone(), 0, vec![]));
        host.add_node(new_node.clone());
        host.bump_epoch();
        Ok(new_node)
    }

    pub fn remove_node_from_cluster(
        &mut self,
        cluster_name: String,
        node_slot: NodeSlot,
    ) -> Result<(), MetaStoreError> {
        let cluster = self
            .clusters
            .get_mut(&cluster_name)
            .ok_or(MetaStoreError::NotFound)?;

        let NodeSlot {
            proxy_address,
            node_address,
        } = node_slot.clone();

        if cluster.remove_node(&node_address).is_none() {
            return Err(MetaStoreError::NotFound);
        }
        let host = self.hosts.get_mut(&proxy_address).ok_or_else(|| {
            error!("Invalid state: cannot find proxy {}", proxy_address);
            MetaStoreError::NotFound
        })?;
        if host.remove_node(&node_address).is_none() {
            error!(
                "Invalid state: cannot find {} {:?} in hosts",
                cluster_name, node_slot
            );
        }

        cluster.bump_epoch();
        host.bump_epoch();

        Self::set_node_free(
            &mut self.all_nodes,
            NodeSlot {
                proxy_address,
                node_address,
            },
        )
    }

    pub fn remove_node(&mut self, node_slot: NodeSlot) -> Result<(), MetaStoreError> {
        let NodeSlot {
            proxy_address,
            node_address,
        } = node_slot;
        self.all_nodes
            .get_mut(&proxy_address)
            .ok_or_else(|| MetaStoreError::NotFound)
            .and_then(|nodes| {
                nodes
                    .get(&node_address)
                    .ok_or_else(|| MetaStoreError::NotFound)
                    .and_then(|free| {
                        if *free {
                            Ok(())
                        } else {
                            Err(MetaStoreError::InUse)
                        }
                    })?;
                nodes
                    .remove(&node_address)
                    .map(|_| ())
                    .ok_or_else(|| MetaStoreError::NotFound)
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
        let free = all_nodes
            .get_mut(&proxy_address)
            .and_then(|nodes| nodes.get_mut(&node_address))
            .ok_or_else(|| MetaStoreError::NotFound)?;
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
            .ok_or(MetaStoreError::NotFound)?;

        let (slot_ranges, src_proxy_address) = cluster
            .get_node(&src_node_address)
            .ok_or(MetaStoreError::NotFound)
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
            .ok_or(MetaStoreError::NotFound)
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
                let (mut migrating_slot_ranges, mut free_slot_ranges) = Self::move_slot_ranges(slot_ranges);
                let mut src_new_migrating_slots = free_slot_ranges.clone();
                for slot_range in src_new_migrating_slots.iter_mut() {
                    slot_range.tag = SlotRangeTag::Migrating(migration_meta.clone());
                }
                for slot_range in free_slot_ranges.iter_mut() {
                    slot_range.tag = SlotRangeTag::Importing(migration_meta.clone());
                }
                migrating_slot_ranges.extend_from_slice(&src_new_migrating_slots);
                (migrating_slot_ranges, free_slot_ranges)
            },
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
            let src_node = cluster
                .get_mut_node(&src_node_address)
                .ok_or(MetaStoreError::NotFound)?;
            *src_node.get_mut_slots() = src_slot_ranges;
            Self::update_host_node(&mut self.hosts, src_node.clone())?;
        }
        {
            let dst_node = cluster.get_mut_node(&dst_node_address).ok_or_else(|| {
                error!("Invalid state: cannot find dst node");
                MetaStoreError::NotFound
            })?;
            *dst_node.get_mut_slots() = dst_slot_ranges;
            Self::update_host_node(&mut self.hosts, dst_node.clone())?;
        }

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
            .ok_or(MetaStoreError::NotFound)?;

        cluster
            .get_mut_node(&src_node_address)
            .ok_or(MetaStoreError::NotFound)
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
            .ok_or(MetaStoreError::NotFound)
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
            let src_node = cluster
                .get_node(&src_node_address)
                .ok_or(MetaStoreError::NotFound)?;
            Self::update_host_node(&mut self.hosts, src_node.clone())?;
        }
        {
            let dst_node = cluster.get_node(&dst_node_address).ok_or_else(|| {
                error!("Invalid state: cannot find dst node");
                MetaStoreError::NotFound
            })?;
            Self::update_host_node(&mut self.hosts, dst_node.clone())?;
        }
        Ok(())
    }

    fn update_host_node(
        hosts: &mut HashMap<String, Host>,
        node: Node,
    ) -> Result<(), MetaStoreError> {
        let host = hosts.get_mut(node.get_proxy_address()).ok_or_else(|| {
            error!("Invalid state: cannot find host");
            MetaStoreError::NotFound
        })?;
        host.remove_node(node.get_address()).ok_or_else(|| {
            error!("Invalid state: cannot find node");
            MetaStoreError::NotFound
        })?;
        host.add_node(node.clone());
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
            .ok_or(MetaStoreError::NotFound)?;

        let master_proxy_address = cluster
            .get_node(&master_node_address)
            .ok_or(MetaStoreError::NotFound)
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
            .ok_or(MetaStoreError::NotFound)
            .and_then(
                |node| match (node.get_role() == Role::Master, node.get_slots().is_empty()) {
                    (false, _) => Err(MetaStoreError::InvalidRole),
                    (_, false) => Err(MetaStoreError::SlotNotEmpty),
                    (true, true) => Ok(node),
                },
            )
            .map(|node| node.get_proxy_address().clone())?;

        {
            let master = cluster
                .get_mut_node(&master_node_address)
                .ok_or(MetaStoreError::NotFound)?;
            master.get_mut_repl().add_peer(ReplPeer {
                node_address: replica_node_address.clone(),
                proxy_address: replica_proxy_address.clone(),
            });
            Self::update_host_node(&mut self.hosts, master.clone())?;
        }

        {
            let replica = cluster.get_mut_node(&replica_node_address).ok_or_else(|| {
                error!("Invalid state: cannot find replica. Data corrupt.");
                MetaStoreError::NotFound
            })?;
            replica.get_mut_repl().add_peer(ReplPeer {
                node_address: master_node_address.clone(),
                proxy_address: master_proxy_address.clone(),
            });
            replica.get_mut_repl().set_role(Role::Replica);
            Self::update_host_node(&mut self.hosts, replica.clone())?;
        }

        cluster.bump_epoch();
        Ok(())
    }

    //    pub fn validate(&self) {}

    fn consume_node_slot(&mut self) -> Result<NodeSlot, MetaStoreError> {
        // TODO: find the least used slot
        for (proxy_address, nodes) in self.all_nodes.iter_mut() {
            for (node_address, free) in nodes.iter_mut() {
                if *free {
                    *free = false;
                    return Ok(NodeSlot {
                        proxy_address: proxy_address.clone(),
                        node_address: node_address.clone(),
                    });
                }
            }
        }
        Err(MetaStoreError::NoAvailableResource)
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
}

#[derive(Debug)]
pub enum MetaStoreError {
    InUse,
    NotInUse,
    NoAvailableResource,
    NotFound,
    InvalidState,
    InvalidRole,
    SlotNotEmpty,
    SlotEmpty,
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
            MetaStoreError::NotFound => "NOT_FOUND",
            MetaStoreError::InvalidState => "INVALID_STATE",
            MetaStoreError::InvalidRole => "INVALID_ROLE",
            MetaStoreError::SlotNotEmpty => "SLOT_NOT_EMPTY",
            MetaStoreError::SlotEmpty => "SLOT_EMPTY",
        }
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}
