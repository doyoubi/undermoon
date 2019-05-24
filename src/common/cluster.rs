use super::utils::{IMPORTING_TAG, MIGRATING_TAG};
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct MigrationMeta {
    pub epoch: u64, // The epoch migration starts
    pub src_proxy_address: String,
    pub src_node_address: String,
    pub dst_proxy_address: String,
    pub dst_node_address: String,
}

impl MigrationMeta {
    pub fn into_strings(self) -> Vec<String> {
        let MigrationMeta {
            epoch,
            src_proxy_address,
            src_node_address,
            dst_proxy_address,
            dst_node_address,
        } = self;
        vec![
            epoch.to_string(),
            src_proxy_address.clone(),
            src_node_address.clone(),
            dst_proxy_address.clone(),
            dst_node_address.clone(),
        ]
    }

    pub fn from_strings<It>(it: &mut It) -> Option<Self>
    where
        It: Iterator<Item = String>,
    {
        let epoch_str = it.next()?;
        Some(Self {
            epoch: epoch_str.parse::<u64>().ok()?,
            src_proxy_address: it.next()?,
            src_node_address: it.next()?,
            dst_proxy_address: it.next()?,
            dst_node_address: it.next()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum SlotRangeTag {
    Migrating(MigrationMeta),
    Importing(MigrationMeta),
    None,
}

impl SlotRangeTag {
    pub fn get_migration_meta(&self) -> Option<&MigrationMeta> {
        match self {
            SlotRangeTag::Migrating(ref meta) => Some(meta),
            SlotRangeTag::Importing(ref meta) => Some(meta),
            SlotRangeTag::None => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct SlotRange {
    pub start: usize,
    pub end: usize,
    pub tag: SlotRangeTag,
}

impl SlotRange {
    pub fn meta_eq(&self, other: &Self) -> bool {
        if self.start != other.start || self.end != other.end {
            return false;
        }

        match (&self.tag, &other.tag) {
            (SlotRangeTag::None, SlotRangeTag::None) => true,
            (SlotRangeTag::Migrating(lhs), SlotRangeTag::Migrating(rhs)) => lhs == rhs,
            (SlotRangeTag::Importing(lhs), SlotRangeTag::Importing(rhs)) => lhs == rhs,
            (SlotRangeTag::Migrating(lhs), SlotRangeTag::Importing(rhs)) => lhs == rhs,
            (SlotRangeTag::Importing(lhs), SlotRangeTag::Migrating(rhs)) => lhs == rhs,
            _ => false,
        }
    }

    pub fn into_strings(self) -> Vec<String> {
        let SlotRange { start, end, tag } = self;
        let mut strs = vec![];
        match tag {
            SlotRangeTag::Migrating(meta) => {
                strs.push(MIGRATING_TAG.to_string());
                strs.push(format!("{}-{}", start, end));
                strs.extend(meta.into_strings());
            }
            SlotRangeTag::Importing(meta) => {
                strs.push(IMPORTING_TAG.to_string());
                strs.push(format!("{}-{}", start, end));
                strs.extend(meta.into_strings());
            }
            SlotRangeTag::None => {
                strs.push(format!("{}-{}", start, end));
            }
        }
        strs
    }

    pub fn from_strings<It>(it: &mut It) -> Option<Self>
    where
        It: Iterator<Item = String>,
    {
        let slot_range = it.next()?;
        let slot_range_tag = slot_range.to_uppercase();

        if slot_range_tag == MIGRATING_TAG {
            let (start, end) = Self::parse_slot_range(it.next()?)?;
            let meta = MigrationMeta::from_strings(it)?;
            Some(SlotRange {
                start,
                end,
                tag: SlotRangeTag::Migrating(meta),
            })
        } else if slot_range_tag == IMPORTING_TAG {
            let (start, end) = Self::parse_slot_range(it.next()?)?;
            let meta = MigrationMeta::from_strings(it)?;
            Some(SlotRange {
                start,
                end,
                tag: SlotRangeTag::Importing(meta),
            })
        } else {
            let (start, end) = Self::parse_slot_range(slot_range)?;
            Some(SlotRange {
                start,
                end,
                tag: SlotRangeTag::None,
            })
        }
    }

    fn parse_slot_range(s: String) -> Option<(usize, usize)> {
        let mut slot_range = s.split('-');
        let start_str = slot_range.next()?;
        let end_str = slot_range.next()?;
        let start = start_str.parse::<usize>().ok()?;
        let end = end_str.parse::<usize>().ok()?;
        Some((start, end))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct MigrationTaskMeta {
    pub db_name: String,
    pub slot_range: SlotRange,
}

impl MigrationTaskMeta {
    pub fn into_strings(self) -> Vec<String> {
        let MigrationTaskMeta {
            db_name,
            slot_range,
        } = self;
        let mut strs = vec![db_name];
        strs.extend(slot_range.into_strings());
        strs
    }
    pub fn from_strings<It>(it: &mut It) -> Option<Self>
    where
        It: Iterator<Item = String>,
    {
        let db_name = it.next()?;
        let slot_range = SlotRange::from_strings(it)?;
        Some(Self {
            db_name,
            slot_range,
        })
    }
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

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct ReplMeta {
    role: Role,
    peers: Vec<ReplPeer>,
}

impl ReplMeta {
    pub fn new(role: Role, peers: Vec<ReplPeer>) -> Self {
        Self { role, peers }
    }

    pub fn get_role(&self) -> Role {
        self.role
    }

    pub fn get_peers(&self) -> &Vec<ReplPeer> {
        &self.peers
    }

    pub fn set_role(&mut self, role: Role) {
        self.role = role
    }

    pub fn add_peer(&mut self, peer: ReplPeer) {
        self.peers.push(peer)
    }
    pub fn remove_peer(&mut self, peer: &ReplPeer) -> Option<ReplPeer> {
        let p = self.peers.iter().find(|p| *p == peer).map(ReplPeer::clone);
        self.peers.retain(|p| p == peer);
        p
    }
}

// (1) In proxy, all Node instances are masters.
// Replica Node will only be used for replication.
// (2) Coordinator will send the master Node metadata to proxies' database module
// and the replica Node metadata to proxies' replication module.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
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
    pub fn get_repl_meta(&self) -> &ReplMeta {
        &self.repl
    }

    pub fn get_mut_slots(&mut self) -> &mut Vec<SlotRange> {
        &mut self.slots
    }
    pub fn get_mut_repl(&mut self) -> &mut ReplMeta {
        &mut self.repl
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Cluster {
    name: String,
    epoch: u64,
    nodes: Vec<Node>,
}

impl Cluster {
    pub fn new(name: String, epoch: u64, nodes: Vec<Node>) -> Self {
        Self { name, epoch, nodes }
    }
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

    pub fn add_node(&mut self, node: Node) {
        self.nodes.push(node);
    }
    pub fn bump_epoch(&mut self) {
        self.epoch += 1;
    }
    pub fn remove_node(&mut self, node_address: &str) -> Option<Node> {
        let node = match self
            .nodes
            .iter()
            .find(|node| node.get_address() == node_address)
        {
            Some(node) => node.clone(),
            None => return None,
        };
        self.nodes.retain(|node| node.get_address() != node_address);
        Some(node)
    }
    pub fn get_node(&mut self, node_address: &str) -> Option<&Node> {
        self.nodes
            .iter()
            .find(|node| node.get_address() == node_address)
    }
    pub fn get_mut_node(&mut self, node_address: &str) -> Option<&mut Node> {
        self.nodes
            .iter_mut()
            .find(|node| node.get_address() == node_address)
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
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

    pub fn add_node(&mut self, node: Node) {
        self.nodes.push(node);
    }
    pub fn bump_epoch(&mut self) {
        self.epoch += 1;
    }
    pub fn remove_node(&mut self, node_address: &str) -> Option<Node> {
        let node = match self
            .nodes
            .iter()
            .find(|node| node.get_address() == node_address)
        {
            Some(node) => node.clone(),
            None => return None,
        };
        self.nodes.retain(|node| node.get_address() != node_address);
        Some(node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_deserialize_slot_range_tag() {
        let importing_str = r#"{"Importing": {
                "epoch": 233,
                "src_proxy_address": "127.0.0.1:7000",
                "src_node_address": "127.0.0.1:6379",
                "dst_proxy_address": "127.0.0.1:7001",
                "dst_node_address": "127.0.0.1:6380"
            }}"#;
        let slot_range: SlotRangeTag =
            serde_json::from_str(importing_str).expect("unexpected string");
        let meta = MigrationMeta {
            epoch: 233,
            src_proxy_address: "127.0.0.1:7000".to_string(),
            src_node_address: "127.0.0.1:6379".to_string(),
            dst_proxy_address: "127.0.0.1:7001".to_string(),
            dst_node_address: "127.0.0.1:6380".to_string(),
        };
        assert_eq!(SlotRangeTag::Importing(meta.clone()), slot_range);

        let migrating_str = r#"{"Migrating": {
                "epoch": 233,
                "src_proxy_address": "127.0.0.1:7000",
                "src_node_address": "127.0.0.1:6379",
                "dst_proxy_address": "127.0.0.1:7001",
                "dst_node_address": "127.0.0.1:6380"
            }}"#;
        let slot_range: SlotRangeTag =
            serde_json::from_str(migrating_str).expect("unexpected string");
        assert_eq!(SlotRangeTag::Migrating(meta), slot_range);

        let none_str = "\"None\"";
        let slot_range: SlotRangeTag = serde_json::from_str(none_str).expect("unexpected string");
        assert_eq!(SlotRangeTag::None, slot_range);
    }

    #[test]
    fn test_deserialize_role() {
        let master_str = "\"master\"";
        let role: Role = serde_json::from_str(master_str).expect("unexpected string");
        assert_eq!(Role::Master, role);

        let replica_str = "\"replica\"";
        let role: Role = serde_json::from_str(replica_str).expect("unexpected string");
        assert_eq!(Role::Replica, role);
    }

    #[test]
    fn test_deserialize_host() {
        let host_str = r#"{
            "address": "server_proxy1:6001",
            "epoch": 1,
            "nodes": [
                {
                    "address": "redis1:7001",
                    "proxy_address": "server_proxy1:6001",
                    "cluster_name": "mydb",
                    "repl": {
                        "role": "master",
                        "peers": [
                            {
                                "node_address": "redis5:7005",
                                "proxy_address": "server_proxy2:6002"
                            }
                        ]
                    },
                    "slots": [{"start": 0, "end": 5461, "tag": "None"}]
                },
                {
                    "address": "redis4:7004",
                    "proxy_address": "server_proxy1:6001",
                    "cluster_name": "mydb",
                    "repl": {
                        "role": "replica",
                        "peers": [
                            {
                                "node_address": "redis3:7003",
                                "proxy_address": "server_proxy3:6003"
                            }
                        ]
                    },
                    "slots": []
                }
            ]
        }"#;
        let host: Host = match serde_json::from_str(host_str) {
            Ok(h) => h,
            Err(e) => {
                println!("### 4 {:?}", e);
                panic!(e);
            }
        };
        let expected_host = Host::new(
            "server_proxy1:6001".to_string(),
            1,
            vec![
                Node::new(
                    "redis1:7001".to_string(),
                    "server_proxy1:6001".to_string(),
                    "mydb".to_string(),
                    vec![SlotRange {
                        start: 0,
                        end: 5461,
                        tag: SlotRangeTag::None,
                    }],
                    ReplMeta::new(
                        Role::Master,
                        vec![ReplPeer {
                            node_address: "redis5:7005".to_string(),
                            proxy_address: "server_proxy2:6002".to_string(),
                        }],
                    ),
                ),
                Node::new(
                    "redis4:7004".to_string(),
                    "server_proxy1:6001".to_string(),
                    "mydb".to_string(),
                    vec![],
                    ReplMeta::new(
                        Role::Replica,
                        vec![ReplPeer {
                            node_address: "redis3:7003".to_string(),
                            proxy_address: "server_proxy3:6003".to_string(),
                        }],
                    ),
                ),
            ],
        );
        assert_eq!(expected_host, host);
    }
}
