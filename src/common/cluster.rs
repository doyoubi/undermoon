use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone)]
pub enum SlotRangeTag {
    Migrating(String),
    Importing(String),
    None,
}

impl Serialize for SlotRangeTag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let tag = match self {
            SlotRangeTag::Migrating(dst) => format!("migrating {}", dst),
            SlotRangeTag::Importing(src) => format!("importing {}", src),
            SlotRangeTag::None => String::new(),
        };
        serializer.serialize_str(&tag)
    }
}

impl<'de> Deserialize<'de> for SlotRangeTag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let mut segs = s.split_terminator(' ');
        let flag = match segs.next() {
            None => return Ok(SlotRangeTag::None),
            Some(flag) => flag.to_lowercase(),
        };
        let peer = segs
            .next()
            .ok_or_else(|| D::Error::custom("Missing peer address"))?;
        if flag == "migrating" {
            Ok(SlotRangeTag::Migrating(peer.to_string()))
        } else if flag == "importing" {
            Ok(SlotRangeTag::Importing(peer.to_string()))
        } else {
            Err(D::Error::custom("Invalid flag"))
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SlotRange {
    pub start: usize,
    pub end: usize,
    pub tag: SlotRangeTag,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct ReplPeer {
    pub node_address: String,
    pub proxy_address: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Role {
    Master,
    Replica,
}

impl Serialize for Role {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Role::Master => serializer.serialize_str("master"),
            Role::Replica => serializer.serialize_str("replica"),
        }
    }
}

impl<'de> Deserialize<'de> for Role {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?.to_uppercase();
        match s.as_str() {
            "MASTER" => Ok(Role::Master),
            "REPLICA" => Ok(Role::Replica),
            _ => Err(D::Error::custom(format!("invalid role {}", s))),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ReplMeta {
    role: Role,
    peers: Vec<ReplPeer>,
}

impl ReplMeta {
    pub fn new(role: Role, peers: Vec<ReplPeer>) -> Self {
        Self { role, peers }
    }
}

// (1) In proxy, all Node instances are masters.
// Replica Node will only be used for replication.
// (2) Coordinator will send the master Node metadata to proxies' database module
// and the replica Node metadata to proxies' replication module.
#[derive(Debug, Deserialize, Serialize)]
pub struct Node {
    address: String,
    proxy_address: String,
    cluster_name: String,
    slots: Vec<SlotRange>,
    repl: ReplMeta,
}

impl Node {
    pub fn new(
        address: String,
        proxy_address: String,
        cluster_name: String,
        slots: Vec<SlotRange>,
        repl: ReplMeta,
    ) -> Self {
        Node {
            address,
            proxy_address,
            cluster_name,
            slots,
            repl,
        }
    }
    pub fn get_address(&self) -> &String {
        &self.address
    }
    pub fn get_proxy_address(&self) -> &String {
        &self.proxy_address
    }
    pub fn get_cluster_name(&self) -> &String {
        &self.cluster_name
    }
    pub fn get_slots(&self) -> &Vec<SlotRange> {
        &self.slots
    }
    pub fn into_slots(self) -> Vec<SlotRange> {
        self.slots
    }
    pub fn get_role(&self) -> Role {
        self.repl.role
    }
}

#[derive(Debug, Deserialize)]
pub struct Cluster {
    name: String,
    epoch: u64,
    nodes: Vec<Node>,
}

impl Cluster {
    pub fn get_name(&self) -> &String {
        &self.name
    }
    pub fn get_nodes(&self) -> &Vec<Node> {
        &self.nodes
    }
    pub fn get_epoch(&self) -> u64 {
        self.epoch
    }
    pub fn into_nodes(self) -> Vec<Node> {
        self.nodes
    }
}

#[derive(Debug, Deserialize)]
pub struct Host {
    address: String,
    epoch: u64,
    nodes: Vec<Node>,
}

impl Host {
    pub fn new(address: String, epoch: u64, nodes: Vec<Node>) -> Self {
        Self {
            address,
            epoch,
            nodes,
        }
    }
    pub fn get_address(&self) -> &String {
        &self.address
    }
    pub fn get_nodes(&self) -> &Vec<Node> {
        &self.nodes
    }
    pub fn get_epoch(&self) -> u64 {
        self.epoch
    }
    pub fn into_nodes(self) -> Vec<Node> {
        self.nodes
    }
}
