use std::collections::HashMap;
use std::sync::Arc;
use arc_swap::ArcSwap;


pub type Address = String;

pub struct Node {
    address: Address,
    cluster_name: String,
}

impl Node {
    pub fn get_address(&self) -> &Address { &self.address }
    pub fn get_cluster_name(&self) -> &String { &self.cluster_name }
}

pub struct Cluster {
    name: Address,
    epoch: u64,
    nodes: Vec<Node>,
}

impl Cluster {
    pub fn get_name(&self) -> &String { &self.name }
    pub fn get_nodes(&self) -> &Vec<Node> { &self.nodes }
    pub fn get_epoch(&self) -> u64 { self.epoch }
}

pub struct Host {
    address: Address,
    epoch: u64,
    nodes: Vec<Node>,
}

impl Host {
    pub fn get_address(&self) -> &Address { &self.address }
    pub fn get_nodes(&self) -> &Vec<Node> { &self.nodes }
    pub fn get_epoch(&self) -> u64 { self.epoch }
}

pub struct CachedFullMetaData {
    full_meta_data: ArcSwap<FullMetaData>,
}

impl CachedFullMetaData {
    fn new() -> Self {
        let meta = FullMetaData::new(HashMap::new(), HashMap::new());
        CachedFullMetaData{
            full_meta_data: ArcSwap::from(Arc::new(meta)),
        }
    }
}

// The data in `clusters` and `hosts` are likely to be inconsistent
// because they are retrieved by different read operations.
// Even the data in `clusters` and `hosts` could also be inconsistent too
// depending on the implementation of the brokers.
pub struct FullMetaData {
    clusters: HashMap<String, Cluster>,
    hosts: HashMap<Address, Host>,
}

impl FullMetaData {
    fn new(clusters: HashMap<String, Cluster>, hosts: HashMap<Address, Host>) -> Self {
        FullMetaData{
            hosts,
            clusters: HashMap::new(),  // TODO: construct clusters from hosts
        }
    }
}
