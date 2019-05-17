use ::common::cluster::{Cluster, Host, Node, ReplMeta, SlotRange, SlotRangeTag};
use ::common::utils::SLOT_NUM;
use common::cluster::Role;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone)]
pub struct NodeSlot {
    proxy_address: String,
    node_address: String,
}

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
    pub fn add_hosts(&mut self, address: String, nodes: Vec<String>) -> Result<(), MetaStoreError> {
        if self.hosts.contains_key(&address) {
            return Err(MetaStoreError::InUse);
        }

        for node in nodes.into_iter() {
            let nodes = self
                .all_nodes
                .entry(node.clone())
                .or_insert_with(HashMap::new);
            nodes.insert(node, true);
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
            if None
                == self
                    .all_nodes
                    .get_mut(node.get_proxy_address())
                    .and_then(|nodes| nodes.get_mut(node.get_address()))
                    .map(|free| *free = true)
            {
                error!(
                    "Invalid meta: cannot find {} {} in all_hosts",
                    cluster_name,
                    node.get_proxy_address()
                );
            }
            if None
                == self
                    .hosts
                    .get_mut(node.get_proxy_address())
                    .and_then(|host| host.remove_node(node.get_address()))
            {
                error!(
                    "Invalid meta: cannot find {} {} in hosts",
                    cluster_name,
                    node.get_proxy_address()
                );
            }
        }
        Ok(())
    }

    pub fn add_node(&mut self, cluster_name: String) -> Result<Node, MetaStoreError> {
        if self.clusters.get(&cluster_name).is_none() {
            return Err(MetaStoreError::NotFound);
        }

        let NodeSlot {
            proxy_address,
            node_address,
        } = self.consume_node_slot()?;

        let repl = ReplMeta::new(Role::Master, vec![]);
        let new_node = Node::new(
            node_address,
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
        if None == cluster.remove_node(&node_address) {
            error!(
                "Invalid state: cannot find {} {:?} in cluster",
                cluster_name, node_slot
            );
        }
        let host = self
            .hosts
            .get_mut(&proxy_address)
            .ok_or(MetaStoreError::NotFound)?;
        if None == host.remove_node(&node_address) {
            error!(
                "Invalid state: cannot find {} {:?} in hosts",
                cluster_name, node_slot
            );
        }
        Ok(())
    }

    pub fn remove_node(&mut self, node_slot: NodeSlot) -> Result<(), MetaStoreError> {
        let NodeSlot {
            proxy_address,
            node_address,
        } = node_slot;
        let free = self
            .all_nodes
            .get_mut(&proxy_address)
            .and_then(|nodes| nodes.get_mut(&node_address))
            .ok_or_else(|| MetaStoreError::NotFound)?;
        if !(*free) {
            return Err(MetaStoreError::InUse);
        }
        *free = true;
        Ok(())
    }

//    pub fn migrate_slots(&mut self, cluster_name: String, src_node: String, dst_node: String) -> Result<(), MetaStoreError> {}
    //
    //    pub fn assign_replica() {}
    //
    //    pub fn validate(&self) {}

    fn consume_node_slot(&mut self) -> Result<NodeSlot, MetaStoreError> {
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
}

#[derive(Debug)]
pub enum MetaStoreError {
    InUse,
    NoAvailableResource,
    NotFound,
    InvalidState,
}

impl fmt::Display for MetaStoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for MetaStoreError {
    fn description(&self) -> &str {
        "meta store error"
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}
