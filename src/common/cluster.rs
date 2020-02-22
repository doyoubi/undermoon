use super::utils::{IMPORTING_TAG, MIGRATING_TAG};
use crate::common::config::ClusterConfig;
use arrayvec;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;

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
            src_proxy_address,
            src_node_address,
            dst_proxy_address,
            dst_node_address,
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

    pub fn get_mut_migration_meta(&mut self) -> Option<&mut MigrationMeta> {
        match self {
            SlotRangeTag::Migrating(ref mut meta) => Some(meta),
            SlotRangeTag::Importing(ref mut meta) => Some(meta),
            SlotRangeTag::None => None,
        }
    }

    pub fn is_importing(&self) -> bool {
        match self {
            SlotRangeTag::Importing(_) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Range {
    pub start: usize,
    pub end: usize,
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

    pub fn to_range(&self) -> Range {
        Range {
            start: self.start,
            end: self.end,
        }
    }
}

// To optimize the DBTag::get_db_name, we need to eliminate the heap allocation.
// Thus we make DBName a stack string with limited size.
pub const DB_NAME_MAX_LENGTH: usize = 31;

#[derive(Debug)]
pub struct InvalidDBName;

#[derive(Debug)]
pub struct CapacityError;

type DBNameInner = arrayvec::ArrayString<[u8; DB_NAME_MAX_LENGTH]>;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DBName(DBNameInner);

impl DBName {
    pub fn new() -> Self {
        Self(DBNameInner::new())
    }

    pub fn from(s: &str) -> Result<Self, InvalidDBName> {
        Ok(Self(DBNameInner::from(s).map_err(|_| InvalidDBName)?))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn try_push(&mut self, c: char) -> Result<(), CapacityError> {
        self.0.try_push(c).map_err(|_| CapacityError)
    }
}

impl Default for DBName {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for DBName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0.to_string())
    }
}

impl fmt::Debug for DBName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0.to_string())
    }
}

impl Serialize for DBName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for DBName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        DBName::from(&s)
            .map_err(|err| D::Error::custom(format!("invalid db name {}: {:?}", s, err)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct MigrationTaskMeta {
    pub db_name: DBName,
    pub slot_range: SlotRange,
}

impl MigrationTaskMeta {
    pub fn into_strings(self) -> Vec<String> {
        let MigrationTaskMeta {
            db_name,
            slot_range,
        } = self;
        let mut strs = vec![db_name.to_string()];
        strs.extend(slot_range.into_strings());
        strs
    }
    pub fn from_strings<It>(it: &mut It) -> Option<Self>
    where
        It: Iterator<Item = String>,
    {
        let db_name_str = it.next()?;
        let db_name = DBName::from(&db_name_str).ok()?;
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
        self.peers.retain(|p| p != peer);
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
    cluster_name: DBName,
    slots: Vec<SlotRange>,
    repl: ReplMeta,
}

impl Node {
    pub fn new(
        address: String,
        proxy_address: String,
        cluster_name: DBName,
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
    pub fn get_cluster_name(&self) -> &DBName {
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
    name: DBName,
    epoch: u64,
    nodes: Vec<Node>,
    #[serde(default)]
    config: ClusterConfig,
}

impl Cluster {
    pub fn new(name: DBName, epoch: u64, nodes: Vec<Node>, config: ClusterConfig) -> Self {
        Self {
            name,
            epoch,
            nodes,
            config,
        }
    }
    pub fn get_name(&self) -> &DBName {
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
    pub fn set_epoch(&mut self, epoch: u64) {
        self.epoch = epoch
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
    pub fn get_node(&self, node_address: &str) -> Option<&Node> {
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
pub struct PeerProxy {
    pub proxy_address: String,
    pub cluster_name: DBName,
    pub slots: Vec<SlotRange>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Host {
    address: String,
    epoch: u64,
    nodes: Vec<Node>,
    free_nodes: Vec<String>,
    peers: Vec<PeerProxy>,
    #[serde(default)]
    clusters_config: HashMap<DBName, ClusterConfig>,
}

impl Host {
    pub fn new(
        address: String,
        epoch: u64,
        nodes: Vec<Node>,
        free_nodes: Vec<String>,
        peers: Vec<PeerProxy>,
        clusters_config: HashMap<DBName, ClusterConfig>,
    ) -> Self {
        Self {
            address,
            epoch,
            nodes,
            free_nodes,
            peers,
            clusters_config,
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
    pub fn get_free_nodes(&self) -> &Vec<String> {
        &self.free_nodes
    }

    pub fn add_node(&mut self, node: Node) {
        self.nodes.push(node);
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
    pub fn get_peers(&self) -> &Vec<PeerProxy> {
        &self.peers
    }

    pub fn get_clusters_config(&self) -> &HashMap<DBName, ClusterConfig> {
        &self.clusters_config
    }
}

#[cfg(test)]
mod tests {
    use super::super::config::CompressionStrategy;
    use super::*;
    use serde_json;
    use std::mem::size_of;

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
            ],
            "free_nodes": [],
            "peers": [{
                "proxy_address": "server_proxy2:6002",
                "cluster_name": "mydb",
                "slots": [{"start": 5462, "end": 10000, "tag": "None"}]
            }],
            "clusters_config": {
                "mydb": {
                    "compression_strategy": "set_get_only"
                }
            }
        }"#;
        let host: Host = match serde_json::from_str(host_str) {
            Ok(h) => h,
            Err(e) => {
                panic!(e);
            }
        };

        let mut config = ClusterConfig::default();
        config.compression_strategy = CompressionStrategy::SetGetOnly;
        let mut clusters_config = HashMap::new();
        clusters_config.insert(DBName::from("mydb").unwrap(), config);

        let expected_host = Host::new(
            "server_proxy1:6001".to_string(),
            1,
            vec![
                Node::new(
                    "redis1:7001".to_string(),
                    "server_proxy1:6001".to_string(),
                    DBName::from("mydb").unwrap(),
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
                    DBName::from("mydb").unwrap(),
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
            Vec::new(),
            vec![PeerProxy {
                proxy_address: "server_proxy2:6002".to_string(),
                cluster_name: DBName::from("mydb").unwrap(),
                slots: vec![SlotRange {
                    start: 5462,
                    end: 10000,
                    tag: SlotRangeTag::None,
                }],
            }],
            clusters_config,
        );
        assert_eq!(expected_host, host);
    }

    #[test]
    fn test_db_name_size() {
        assert_eq!(size_of::<DBName>(), DB_NAME_MAX_LENGTH + 1);
        // Align to 32 bytes.
        assert_eq!(size_of::<DBName>(), 32);
        let mut name = DBName::new();
        // DBName should be able to store ASCII with just a byte unlike char.
        for _ in 0..DB_NAME_MAX_LENGTH {
            name.try_push('a').unwrap();
        }
        assert!(name.0.is_full());
        assert_eq!(
            name.as_str(),
            (0..DB_NAME_MAX_LENGTH)
                .into_iter()
                .map(|_| 'a')
                .collect::<String>()
        )
    }
}
