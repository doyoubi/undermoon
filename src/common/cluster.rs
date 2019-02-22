#[derive(Debug, Clone, Deserialize)]
pub enum SlotRangeTag {
    Migrating(String),
    None,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SlotRange {
    pub start: usize,
    pub end: usize,
    pub tag: SlotRangeTag,
}

#[derive(Debug, Deserialize)]
pub struct Node {
    address: String,
    cluster_name: String,
    slots: Vec<SlotRange>,
}

impl Node {
    pub fn get_address(&self) -> &String { &self.address }
    pub fn get_cluster_name(&self) -> &String { &self.cluster_name }
}

#[derive(Debug, Deserialize)]
pub struct Cluster {
    name: String,
    epoch: u64,
    nodes: Vec<Node>,
}

impl Cluster {
    pub fn get_name(&self) -> &String { &self.name }
    pub fn get_nodes(&self) -> &Vec<Node> { &self.nodes }
    pub fn get_epoch(&self) -> u64 { self.epoch }
}

#[derive(Debug, Deserialize)]
pub struct Host {
    address: String,
    epoch: u64,
    nodes: Vec<Node>,
}

impl Host {
    pub fn get_address(&self) -> &String { &self.address }
    pub fn get_nodes(&self) -> &Vec<Node> { &self.nodes }
    pub fn get_epoch(&self) -> u64 { self.epoch }
}