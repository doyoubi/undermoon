use serde::{Deserialize, Deserializer};
use serde::de::Error;

#[derive(Debug, Clone)]
pub enum SlotRangeTag {
    Migrating(String),
    None,
}

impl<'de> Deserialize<'de> for SlotRangeTag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where D: Deserializer<'de> {
        let s = String::deserialize(deserializer)?;
        let mut segs = s.split_terminator(' ');
        let flag = match segs.next() {
            None => return Ok(SlotRangeTag::None),
            Some(flag) => flag,
        };
        if flag != "migrating" {
            return Err(D::Error::custom("Invalid flag"))
        }
        let dst = segs.next()
            .ok_or(D::Error::custom("Missing destination address"))?;
        Ok(SlotRangeTag::Migrating(dst.to_string()))
    }
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
    pub fn get_slots(&self) -> &Vec<SlotRange> { &self.slots }
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
    pub fn into_nodes(self) -> Vec<Node> { self.nodes }
}

#[derive(Debug, Deserialize)]
pub struct Host {
    address: String,
    epoch: u64,
    nodes: Vec<Node>,
}

impl Host {
    pub fn new(address: String, epoch: u64, nodes: Vec<Node>) -> Self { Self{ address, epoch, nodes } }
    pub fn get_address(&self) -> &String { &self.address }
    pub fn get_nodes(&self) -> &Vec<Node> { &self.nodes }
    pub fn get_epoch(&self) -> u64 { self.epoch }
    pub fn into_nodes(self) -> Vec<Node> { self.nodes }
}
