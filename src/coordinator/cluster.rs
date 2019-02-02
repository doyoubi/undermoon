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
    pub fn get_adddress(&self) -> &Address { &self.address }
    pub fn get_nodes(&self) -> &Vec<Node> { &self.nodes }
    pub fn get_epoch(&self) -> u64 { self.epoch }
}
