use super::utils::{IMPORTING_TAG, MIGRATING_TAG, SLOT_NUM};
use crate::common::config::ClusterConfig;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::max;
use std::convert::TryFrom;
use std::fmt;
use std::iter::Peekable;
use std::mem::swap;

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

    pub fn is_stable(&self) -> bool {
        matches!(self, SlotRangeTag::None)
    }

    pub fn is_migrating(&self) -> bool {
        matches!(self, SlotRangeTag::Migrating(_))
    }

    pub fn is_importing(&self) -> bool {
        matches!(self, SlotRangeTag::Importing(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct Range(pub usize, pub usize);

impl Range {
    pub fn start(&self) -> usize {
        self.0
    }

    pub fn end(&self) -> usize {
        self.1
    }

    pub fn start_mut(&mut self) -> &mut usize {
        &mut self.0
    }

    pub fn end_mut(&mut self) -> &mut usize {
        &mut self.1
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct RangeList(Vec<Range>);

#[derive(Debug)]
pub struct InvalidRangeListString;

impl TryFrom<&str> for RangeList {
    type Error = InvalidRangeListString;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let mut it = s.split(' ').map(|s| s.to_string());
        Self::parse(&mut it).ok_or(InvalidRangeListString)
    }
}

impl fmt::Display for RangeList {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[")?;
        for (i, range) in self.0.iter().enumerate() {
            if range.start() == range.end() {
                write!(f, "{}", range.start())?;
            } else {
                write!(f, "{}-{}", range.start(), range.end())?;
            }
            if i + 1 != self.0.len() {
                write!(f, ", ")?;
            }
        }
        write!(f, "]")
    }
}

impl RangeList {
    pub fn new(ranges: Vec<Range>) -> Self {
        let mut range_list = Self(ranges);
        range_list.compact();
        range_list
    }

    pub fn from_single_range(mut range: Range) -> Self {
        if range.start() > range.end() {
            swap(&mut range.0, &mut range.1);
        }
        Self(vec![range])
    }

    pub fn merge(range_lists: Vec<RangeList>) -> Self {
        let mut merged_ranges = vec![];
        for RangeList(mut ranges) in range_lists.into_iter() {
            merged_ranges.append(&mut ranges);
        }
        Self::new(merged_ranges)
    }

    pub fn merge_another(&mut self, range_list: &mut RangeList) {
        self.0.append(&mut range_list.0);
        self.compact();
    }

    fn parse<It>(it: &mut It) -> Option<Self>
    where
        It: Iterator<Item = String>,
    {
        let ranges_num = it.next()?.parse::<usize>().ok()?;
        let mut ranges = vec![];
        for _ in 0..ranges_num {
            ranges.push(Self::parse_slot_range(it.next()?)?);
        }
        Some(Self::new(ranges))
    }

    fn parse_slot_range(s: String) -> Option<Range> {
        let mut slot_range = s.split('-');
        let start_str = slot_range.next()?;
        let end_str = slot_range.next()?;
        let start = start_str.parse::<usize>().ok()?;
        let end = end_str.parse::<usize>().ok()?;
        Some(Range(start, end))
    }

    pub fn to_strings(&self) -> Vec<String> {
        let mut strs = vec![self.0.len().to_string()];
        for range in self.0.iter() {
            let Range(start, end) = range;
            strs.push(format!("{}-{}", *start, *end));
        }
        strs
    }

    pub fn compact(&mut self) {
        // Goal: for any i < j, i.start <= i.end < j.start <= j.end
        for range in self.0.iter_mut() {
            if range.start() > range.end() {
                swap(&mut range.0, &mut range.1);
            }
        }
        self.0.sort_by_key(|range| range.start());
        // After sort, for any i < j,
        // i.start <= i.end
        // j.start <= j.end
        // i.start <= j.start
        let mut a = 0;
        let mut b = 1;
        while let Some(e) = self.0.get(b).cloned() {
            {
                let s = self.0.get_mut(a).expect("RangeList::compact");
                if s.end() + 1 >= e.start() {
                    s.1 = max(s.end(), e.end());
                    b += 1;
                    continue;
                }
            }
            *self.0.get_mut(a + 1).expect("RangeList::compact") = e;
            a += 1;
            b += 1;
        }
        self.0.truncate(a + 1);
    }

    pub fn get_ranges(&self) -> &[Range] {
        &self.0
    }

    pub fn get_mut_ranges(&mut self) -> &mut Vec<Range> {
        &mut self.0
    }

    pub fn get_slots_num(&self) -> usize {
        self.get_ranges()
            .iter()
            .map(|range| range.end() - range.start() + 1)
            .sum()
    }
}

#[derive(Clone)]
pub struct RangeMap {
    min_slot: usize,
    exists_map: Vec<bool>,
}

impl From<&RangeList> for RangeMap {
    fn from(range_list: &RangeList) -> Self {
        let min_slot = range_list.get_ranges().first().and_then(|range| {
            if range.start() >= SLOT_NUM {
                None
            } else {
                Some(range.start())
            }
        });
        let max_slot = range_list.get_ranges().last().and_then(|range| {
            if range.end() >= SLOT_NUM {
                None
            } else {
                Some(range.end())
            }
        });
        let (min_slot, map_len) = match (min_slot, max_slot) {
            (Some(min_slot), Some(max_slot)) => (min_slot, max_slot - min_slot + 1),
            _ => (0, 0),
        };

        let mut exists_map = vec![false; map_len];
        for range in range_list.get_ranges().iter() {
            for slot_num in range.start()..=range.end() {
                if let Some(slot) = slot_num
                    .checked_sub(min_slot)
                    .and_then(|inner_index| exists_map.get_mut(inner_index))
                {
                    *slot = true;
                }
            }
        }
        Self {
            min_slot,
            exists_map,
        }
    }
}

impl RangeMap {
    pub fn contains_slot(&self, slot: usize) -> bool {
        slot.checked_sub(self.min_slot)
            .and_then(|inner_index| self.exists_map.get(inner_index).cloned())
            == Some(true)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct SlotRange {
    pub range_list: RangeList,
    pub tag: SlotRangeTag,
}

impl SlotRange {
    pub fn meta_eq(&self, other: &Self) -> bool {
        if self.range_list != other.range_list {
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
        let SlotRange { range_list, tag } = self;
        let mut strs = vec![];
        match tag {
            SlotRangeTag::Migrating(meta) => {
                strs.push(MIGRATING_TAG.to_string());
                strs.extend(range_list.to_strings());
                strs.extend(meta.into_strings());
            }
            SlotRangeTag::Importing(meta) => {
                strs.push(IMPORTING_TAG.to_string());
                strs.extend(range_list.to_strings());
                strs.extend(meta.into_strings());
            }
            SlotRangeTag::None => {
                strs.extend(range_list.to_strings());
            }
        }
        strs
    }

    pub fn from_strings<It>(it: &mut Peekable<It>) -> Option<Self>
    where
        It: Iterator<Item = String>,
    {
        let slot_range = it.peek()?;
        let slot_range_tag = slot_range.to_uppercase();

        if slot_range_tag == MIGRATING_TAG {
            it.next()?; // Consume the tag
            let range_list = RangeList::parse(it)?;
            let meta = MigrationMeta::from_strings(it)?;
            Some(SlotRange {
                range_list,
                tag: SlotRangeTag::Migrating(meta),
            })
        } else if slot_range_tag == IMPORTING_TAG {
            it.next()?; // Consume the tag
            let range_list = RangeList::parse(it)?;
            let meta = MigrationMeta::from_strings(it)?;
            Some(SlotRange {
                range_list,
                tag: SlotRangeTag::Importing(meta),
            })
        } else {
            let range_list = RangeList::parse(it)?;
            Some(SlotRange {
                range_list,
                tag: SlotRangeTag::None,
            })
        }
    }

    pub fn get_range_list(&self) -> &RangeList {
        &self.range_list
    }

    pub fn get_mut_range_list(&mut self) -> &mut RangeList {
        &mut self.range_list
    }

    pub fn to_range_list(&self) -> RangeList {
        self.range_list.clone()
    }
}

// In previous version, to optimize the ClusterTag::get_cluster_name, we need to eliminate the heap allocation.
// Thus we make ClusterName a stack string with limited size.
pub const CLUSTER_NAME_MAX_LENGTH: usize = 31;

// EMPTY_CLUSTER_NAME is used when the proxy is not in any cluster.
lazy_static! {
    pub static ref EMPTY_CLUSTER_NAME: ClusterName = ClusterName::empty();
}

#[derive(Debug)]
pub struct InvalidClusterName;

#[derive(Debug)]
pub struct CapacityError;

type ClusterNameInner = arrayvec::ArrayString<[u8; CLUSTER_NAME_MAX_LENGTH]>;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ClusterName(ClusterNameInner);

impl TryFrom<&str> for ClusterName {
    type Error = InvalidClusterName;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        for c in s.chars() {
            if c.is_ascii_alphanumeric() || c == '@' || c == '-' || c == '_' {
                continue;
            }
            return Err(InvalidClusterName);
        }
        Ok(Self(
            ClusterNameInner::from(s).map_err(|_| InvalidClusterName)?,
        ))
    }
}

impl ClusterName {
    pub fn empty() -> Self {
        Self(ClusterNameInner::new())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        self.0.as_str().to_string().into_bytes()
    }

    pub fn try_push(&mut self, c: char) -> Result<(), CapacityError> {
        self.0.try_push(c).map_err(|_| CapacityError)
    }
}

impl Default for ClusterName {
    fn default() -> Self {
        Self::empty()
    }
}

impl fmt::Display for ClusterName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0.to_string())
    }
}

impl fmt::Debug for ClusterName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0.to_string())
    }
}

impl Serialize for ClusterName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ClusterName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ClusterName::try_from(s.as_str())
            .map_err(|err| D::Error::custom(format!("invalid cluster name {}: {:?}", s, err)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct MigrationTaskMeta {
    pub cluster_name: ClusterName,
    pub slot_range: SlotRange,
}

impl MigrationTaskMeta {
    pub fn into_strings(self) -> Vec<String> {
        let MigrationTaskMeta {
            cluster_name,
            slot_range,
        } = self;
        let mut strs = vec![cluster_name.to_string()];
        strs.extend(slot_range.into_strings());
        strs
    }

    pub fn from_strings<It>(it: &mut Peekable<It>) -> Option<Self>
    where
        It: Iterator<Item = String>,
    {
        let cluster_name_str = it.next()?;
        let cluster_name = ClusterName::try_from(cluster_name_str.as_str()).ok()?;
        let slot_range = SlotRange::from_strings(it)?;
        Some(Self {
            cluster_name,
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

    pub fn get_peers(&self) -> &[ReplPeer] {
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
// (2) Coordinator will send the master Node metadata to proxies' cluster module
// and the replica Node metadata to proxies' replication module.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Node {
    address: String,
    proxy_address: String,
    slots: Vec<SlotRange>,
    repl: ReplMeta,
}

impl Node {
    pub fn new(
        address: String,
        proxy_address: String,
        slots: Vec<SlotRange>,
        repl: ReplMeta,
    ) -> Self {
        Node {
            address,
            proxy_address,
            slots,
            repl,
        }
    }
    pub fn get_address(&self) -> &str {
        &self.address
    }
    pub fn get_proxy_address(&self) -> &str {
        &self.proxy_address
    }
    pub fn get_slots(&self) -> &[SlotRange] {
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
    name: ClusterName,
    epoch: u64,
    nodes: Vec<Node>,
    #[serde(default)]
    config: ClusterConfig,
}

impl Cluster {
    pub fn new(name: ClusterName, epoch: u64, nodes: Vec<Node>, config: ClusterConfig) -> Self {
        Self {
            name,
            epoch,
            nodes,
            config,
        }
    }
    pub fn get_name(&self) -> &ClusterName {
        &self.name
    }
    pub fn get_nodes(&self) -> &[Node] {
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

    pub fn get_config(&self) -> ClusterConfig {
        self.config.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct PeerProxy {
    pub proxy_address: String,
    pub slots: Vec<SlotRange>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Proxy {
    cluster_name: Option<ClusterName>,
    address: String,
    epoch: u64,
    // We can change this to [Node; 2] but we don't
    // because maybe others would like to support more than 2 nodes
    // in a shard.
    nodes: Vec<Node>,
    // TODO: remove `free_nodes`
    free_nodes: Vec<String>,
    peers: Vec<PeerProxy>,
    #[serde(default)]
    cluster_config: Option<ClusterConfig>,
}

impl Proxy {
    pub fn new(
        cluster_name: Option<ClusterName>,
        address: String,
        epoch: u64,
        nodes: Vec<Node>,
        free_nodes: Vec<String>,
        peers: Vec<PeerProxy>,
        cluster_config: Option<ClusterConfig>,
    ) -> Self {
        Self {
            cluster_name,
            address,
            epoch,
            nodes,
            free_nodes,
            peers,
            cluster_config,
        }
    }

    pub fn get_cluster_name(&self) -> Option<&ClusterName> {
        self.cluster_name.as_ref()
    }
    pub fn get_address(&self) -> &str {
        &self.address
    }
    pub fn get_nodes(&self) -> &[Node] {
        &self.nodes
    }
    pub fn get_epoch(&self) -> u64 {
        self.epoch
    }
    pub fn into_nodes(self) -> Vec<Node> {
        self.nodes
    }
    pub fn get_free_nodes(&self) -> &[String] {
        &self.free_nodes
    }

    pub fn get_peers(&self) -> &[PeerProxy] {
        &self.peers
    }

    pub fn get_cluster_config(&self) -> Option<&ClusterConfig> {
        self.cluster_config.as_ref()
    }

    pub fn get_cluster_config_or_default(&self) -> ClusterConfig {
        self.cluster_config
            .clone()
            .unwrap_or_else(ClusterConfig::default)
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
        let slot_range: SlotRangeTag = serde_json::from_str(importing_str).unwrap();
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
        let slot_range: SlotRangeTag = serde_json::from_str(migrating_str).unwrap();
        assert_eq!(SlotRangeTag::Migrating(meta), slot_range);

        let none_str = "\"None\"";
        let slot_range: SlotRangeTag = serde_json::from_str(none_str).unwrap();
        assert_eq!(SlotRangeTag::None, slot_range);
    }

    #[test]
    fn test_deserialize_role() {
        let master_str = "\"master\"";
        let role: Role = serde_json::from_str(master_str).unwrap();
        assert_eq!(Role::Master, role);

        let replica_str = "\"replica\"";
        let role: Role = serde_json::from_str(replica_str).unwrap();
        assert_eq!(Role::Replica, role);
    }

    #[test]
    fn test_deserialize_proxy() {
        let proxy_str = r#"{
            "cluster_name": "mycluster",
            "address": "server_proxy1:6001",
            "epoch": 1,
            "nodes": [
                {
                    "address": "redis1:7001",
                    "proxy_address": "server_proxy1:6001",
                    "cluster_name": "mycluster",
                    "repl": {
                        "role": "master",
                        "peers": [
                            {
                                "node_address": "redis5:7005",
                                "proxy_address": "server_proxy2:6002"
                            }
                        ]
                    },
                    "slots": [{"range_list": [[0, 5461]], "tag": "None"}]
                },
                {
                    "address": "redis4:7004",
                    "proxy_address": "server_proxy1:6001",
                    "cluster_name": "mycluster",
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
                "cluster_name": "mycluster",
                "slots": [{"range_list": [[5462, 10000]], "tag": "None"}]
            }],
            "cluster_config": {
                "compression_strategy": "set_get_only"
            }
        }"#;
        let proxy: Proxy = serde_json::from_str(proxy_str).unwrap();

        let mut config = ClusterConfig::default();
        config.compression_strategy = CompressionStrategy::SetGetOnly;

        let expected_proxy = Proxy::new(
            Some(ClusterName::try_from("mycluster").unwrap()),
            "server_proxy1:6001".to_string(),
            1,
            vec![
                Node::new(
                    "redis1:7001".to_string(),
                    "server_proxy1:6001".to_string(),
                    vec![SlotRange {
                        range_list: RangeList::try_from("1 0-5461").unwrap(),
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
                slots: vec![SlotRange {
                    range_list: RangeList::try_from("1 5462-10000").unwrap(),
                    tag: SlotRangeTag::None,
                }],
            }],
            Some(config),
        );
        assert_eq!(expected_proxy, proxy);
    }

    #[test]
    fn test_cluster_name_size() {
        assert_eq!(size_of::<ClusterName>(), CLUSTER_NAME_MAX_LENGTH + 1);
        // Align to 32 bytes.
        assert_eq!(size_of::<ClusterName>(), 32);
        let mut name = EMPTY_CLUSTER_NAME.clone();
        // ClusterName should be able to store ASCII with just a byte unlike char.
        for _ in 0..CLUSTER_NAME_MAX_LENGTH {
            name.try_push('a').unwrap();
        }
        assert!(name.0.is_full());
        assert_eq!(
            name.as_str(),
            (0..CLUSTER_NAME_MAX_LENGTH)
                .into_iter()
                .map(|_| 'a')
                .collect::<String>()
        )
    }

    #[test]
    fn test_range_list_encoding_and_decoding() {
        let s: Vec<String> = vec!["0", "233-666"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let range_list = RangeList::parse(&mut s.into_iter()).unwrap();
        assert_eq!(range_list.get_ranges().len(), 0);
        let s = range_list.to_strings().join(" ");
        assert_eq!(s, "0");

        let s: Vec<String> = vec!["1", "0-233"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let range_list = RangeList::parse(&mut s.into_iter()).unwrap();
        assert_eq!(range_list.get_ranges().len(), 1);
        assert_eq!(range_list.get_ranges()[0].start(), 0);
        assert_eq!(range_list.get_ranges()[0].end(), 233);
        let s = range_list.to_strings().join(" ");
        assert_eq!(s, "1 0-233");

        let s: Vec<String> = vec!["2", "0-233", "666-999"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let range_list = RangeList::parse(&mut s.into_iter()).unwrap();
        assert_eq!(range_list.get_ranges().len(), 2);
        assert_eq!(range_list.get_ranges()[0].start(), 0);
        assert_eq!(range_list.get_ranges()[0].end(), 233);
        assert_eq!(range_list.get_ranges()[1].start(), 666);
        assert_eq!(range_list.get_ranges()[1].end(), 999);
        let s = range_list.to_strings().join(" ");
        assert_eq!(s, "2 0-233 666-999");
    }

    #[test]
    fn test_range_list_compact() {
        let s: Vec<String> = vec!["2", "666-999", "233-0"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let range_list = RangeList::parse(&mut s.into_iter()).unwrap();
        assert_eq!(range_list.get_ranges().len(), 2);
        assert_eq!(range_list.get_ranges()[0].start(), 0);
        assert_eq!(range_list.get_ranges()[0].end(), 233);
        assert_eq!(range_list.get_ranges()[1].start(), 666);
        assert_eq!(range_list.get_ranges()[1].end(), 999);

        let s: Vec<String> = vec!["2", "666-999", "999-0"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let range_list = RangeList::parse(&mut s.into_iter()).unwrap();
        assert_eq!(range_list.get_ranges().len(), 1);
        assert_eq!(range_list.get_ranges()[0].start(), 0);
        assert_eq!(range_list.get_ranges()[0].end(), 999);

        let s: Vec<String> = vec!["2", "666-7799", "999-0"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let range_list = RangeList::parse(&mut s.into_iter()).unwrap();
        assert_eq!(range_list.get_ranges().len(), 1);
        assert_eq!(range_list.get_ranges()[0].start(), 0);
        assert_eq!(range_list.get_ranges()[0].end(), 7799);

        let s: Vec<String> = vec!["2", "0-7799", "999-0"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let range_list = RangeList::parse(&mut s.into_iter()).unwrap();
        assert_eq!(range_list.get_ranges().len(), 1);
        assert_eq!(range_list.get_ranges()[0].start(), 0);
        assert_eq!(range_list.get_ranges()[0].end(), 7799);

        let s: Vec<String> = vec!["3", "0-1365", "1366-1638", "1639-2047"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let range_list = RangeList::parse(&mut s.into_iter()).unwrap();
        assert_eq!(range_list.get_ranges().len(), 1);
        assert_eq!(range_list.get_ranges()[0].start(), 0);
        assert_eq!(range_list.get_ranges()[0].end(), 2047);

        let s: Vec<String> = vec!["2", "0-997", "999-1000"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let range_list = RangeList::parse(&mut s.into_iter()).unwrap();
        assert_eq!(range_list.get_ranges().len(), 2);
        assert_eq!(range_list.get_ranges()[0].start(), 0);
        assert_eq!(range_list.get_ranges()[0].end(), 997);
        assert_eq!(range_list.get_ranges()[1].start(), 999);
        assert_eq!(range_list.get_ranges()[1].end(), 1000);
    }

    #[test]
    fn test_range_list_contains_slot() {
        let range_list = RangeList::try_from("0 233-666").unwrap();
        let range_map = RangeMap::from(&range_list);
        assert!(!range_map.contains_slot(0));

        let range_list = RangeList::try_from("1 0-233").unwrap();
        let range_map = RangeMap::from(&range_list);
        assert!(range_map.contains_slot(0));
        assert!(range_map.contains_slot(99));
        assert!(range_map.contains_slot(233));

        let range_list = RangeList::try_from("1 99-99").unwrap();
        let range_map = RangeMap::from(&range_list);
        assert!(!range_map.contains_slot(98));
        assert!(range_map.contains_slot(99));
        assert!(!range_map.contains_slot(100));

        let range_list = RangeList::try_from("2 0-233 666-7799").unwrap();
        let range_map = RangeMap::from(&range_list);
        assert!(range_map.contains_slot(0));
        assert!(range_map.contains_slot(99));
        assert!(range_map.contains_slot(233));
        assert!(!range_map.contains_slot(234));

        assert!(!range_map.contains_slot(665));
        assert!(range_map.contains_slot(666));
        assert!(range_map.contains_slot(6699));
        assert!(range_map.contains_slot(7799));
        assert!(!range_map.contains_slot(7800));

        let range_list = RangeList::try_from(format!("1 0-{}", SLOT_NUM - 1).as_str()).unwrap();
        let range_map = RangeMap::from(&range_list);
        assert!(range_map.contains_slot(0));
        assert!(range_map.contains_slot(5299));
        assert!(range_map.contains_slot(SLOT_NUM - 1));
        assert!(!range_map.contains_slot(SLOT_NUM));

        let range_list = RangeList::try_from(format!("1 0-20000000").as_str()).unwrap();
        let range_map = RangeMap::from(&range_list);
        assert!(!range_map.contains_slot(0));
        assert!(!range_map.contains_slot(5299));
        assert!(!range_map.contains_slot(SLOT_NUM - 1));
        assert!(!range_map.contains_slot(SLOT_NUM));
        assert!(!range_map.contains_slot(20000000));

        let range_list = RangeList::try_from(format!("1 20000-30000").as_str()).unwrap();
        let range_map = RangeMap::from(&range_list);
        assert!(!range_map.contains_slot(0));
        assert!(!range_map.contains_slot(5299));
        assert!(!range_map.contains_slot(SLOT_NUM - 1));
        assert!(!range_map.contains_slot(SLOT_NUM));
        assert!(!range_map.contains_slot(20000000));

        let range_list = RangeList::try_from(format!("2 1366-2729 16383-16383").as_str()).unwrap();
        let range_map = RangeMap::from(&range_list);
        assert!(!range_map.contains_slot(0));
        assert!(!range_map.contains_slot(1365));
        assert!(range_map.contains_slot(1366));
        assert!(range_map.contains_slot(2729));
        assert!(!range_map.contains_slot(2730));
        assert!(range_map.contains_slot(SLOT_NUM - 1));
        assert!(!range_map.contains_slot(SLOT_NUM));
        assert!(!range_map.contains_slot(20000000));
    }

    #[test]
    fn test_range_list_deserialize() {
        let range_list_str = r#"[[0, 233]]"#;
        let range_list: RangeList = serde_json::from_str(range_list_str).unwrap();
        assert_eq!(range_list.get_ranges().len(), 1);
        assert_eq!(range_list.get_ranges()[0].start(), 0);
        assert_eq!(range_list.get_ranges()[0].end(), 233);
    }
}
