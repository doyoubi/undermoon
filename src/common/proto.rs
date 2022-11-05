use super::cluster::SlotRange;
use super::utils::{has_flags, CmdParseError};
use crate::common::cluster::ClusterName;
use crate::common::config::ClusterConfig;
use crate::common::utils::extract_host_from_address;
use crate::protocol::{Array, BulkStr, Resp};
use flate2::Compression;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io;
use std::io::Read;
use std::io::Write;
use std::iter::Peekable;
use std::str;

macro_rules! try_parse {
    ($expression:expr) => {{
        match $expression {
            Ok(v) => (v),
            Err(_) => return Err(CmdParseError::InvalidArgs),
        }
    }};
}

macro_rules! try_get {
    ($expression:expr) => {{
        match $expression {
            Some(v) => (v),
            None => return Err(CmdParseError::InvalidArgs),
        }
    }};
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterMapFlags {
    pub force: bool,
    pub compress: bool,
}

impl ClusterMapFlags {
    pub fn to_arg(&self) -> String {
        let mut flags = Vec::new();
        if self.force {
            flags.push("FORCE");
        }
        if self.compress {
            flags.push("COMPRESS");
        }

        if flags.is_empty() {
            "NOFLAG".to_string()
        } else {
            flags.join(",")
        }
    }

    pub fn from_arg(flags_str: &str) -> Self {
        let force = has_flags(flags_str, ',', "FORCE");
        let compress = has_flags(flags_str, ',', "COMPRESS");

        ClusterMapFlags { force, compress }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ProxyClusterMetaData {
    cluster_name: ClusterName,
    local: NodeMap,
    peer: NodeMap,
    cluster_config: ClusterConfigData,
}

impl ProxyClusterMetaData {
    pub fn new(
        cluster_name: ClusterName,
        local: NodeMap,
        peer: NodeMap,
        clusters_config: ClusterConfigData,
    ) -> Self {
        Self {
            cluster_name,
            local,
            peer,
            cluster_config: clusters_config,
        }
    }

    pub fn gen_compressed_data(&self) -> Result<String, MetaCompressError> {
        let s = serde_json::to_string(&self).map_err(|err| {
            error!("failed to encode json for meta: {:?}", err);
            MetaCompressError::Json
        })?;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), Compression::fast());
        encoder
            .write_all(s.as_bytes())
            .map_err(MetaCompressError::Io)?;
        let buf = encoder.finish().map_err(MetaCompressError::Io)?;
        Ok(base64::encode(&buf))
    }

    pub fn from_compressed_data(data: String) -> Result<Self, MetaCompressError> {
        let raw = base64::decode(data).map_err(|err| {
            error!("failed to decode base64 for meta: {:?}", err);
            MetaCompressError::Base64
        })?;
        let r = io::Cursor::new(raw);
        let mut gz = flate2::read::GzDecoder::new(r);
        let mut s = String::new();
        gz.read_to_string(&mut s).map_err(MetaCompressError::Io)?;
        serde_json::from_str(s.as_str()).map_err(|err| {
            error!("failed to decode json for meta: {:?}", err);
            MetaCompressError::Json
        })
    }
}

#[derive(Debug)]
pub enum MetaCompressError {
    Io(io::Error),
    Json,
    Base64,
}

const PEER_PREFIX: &str = "PEER";
const CONFIG_PREFIX: &str = "CONFIG";

pub const SET_CLUSTER_API_VERSION: &str = "v2";

#[derive(Debug, Clone)]
pub struct ProxyClusterMeta {
    version: String,
    epoch: u64,
    flags: ClusterMapFlags,
    cluster_name: ClusterName,
    local: HashMap<String, Vec<SlotRange>>,
    peer: HashMap<String, Vec<SlotRange>>,
    cluster_config: ClusterConfig,
}

impl ProxyClusterMeta {
    pub fn new(
        epoch: u64,
        flags: ClusterMapFlags,
        cluster_name: ClusterName,
        local: HashMap<String, Vec<SlotRange>>,
        peer: HashMap<String, Vec<SlotRange>>,
        cluster_config: ClusterConfig,
    ) -> Self {
        Self {
            version: SET_CLUSTER_API_VERSION.to_string(),
            epoch,
            flags,
            cluster_name,
            local,
            peer,
            cluster_config,
        }
    }

    pub fn get_version(&self) -> &str {
        self.version.as_str()
    }

    pub fn get_epoch(&self) -> u64 {
        self.epoch
    }

    pub fn get_flags(&self) -> ClusterMapFlags {
        self.flags.clone()
    }

    pub fn get_cluster_name(&self) -> &ClusterName {
        &self.cluster_name
    }

    pub fn get_local(&self) -> &HashMap<String, Vec<SlotRange>> {
        &self.local
    }
    pub fn get_peer(&self) -> &HashMap<String, Vec<SlotRange>> {
        &self.peer
    }

    pub fn get_config(&self) -> &ClusterConfig {
        &self.cluster_config
    }

    pub fn gen_data(&self) -> ProxyClusterMetaData {
        ProxyClusterMetaData {
            cluster_name: self.cluster_name.clone(),
            local: NodeMap::new(self.local.clone()),
            peer: NodeMap::new(self.peer.clone()),
            cluster_config: ClusterConfigData(self.cluster_config.clone()),
        }
    }

    pub fn from_resp<T: AsRef<[u8]>>(
        resp: &Resp<T>,
    ) -> Result<(Self, Result<(), ParseExtendedMetaError>), CmdParseError> {
        let arr = match resp {
            Resp::Arr(Array::Arr(ref arr)) => arr,
            _ => return Err(CmdParseError::InvalidArgs),
        };

        // Skip the "UMCTL SETCLUSTER"
        let it = arr.iter().skip(2).flat_map(|resp| match resp {
            Resp::Bulk(BulkStr::Str(safe_str)) => match str::from_utf8(safe_str.as_ref()) {
                Ok(s) => Some(s.to_string()),
                _ => None,
            },
            _ => None,
        });
        let mut it = it.peekable();

        Self::parse(&mut it)
    }

    pub fn parse<It>(
        it: &mut Peekable<It>,
    ) -> Result<(Self, Result<(), ParseExtendedMetaError>), CmdParseError>
    where
        It: Iterator<Item = String>,
    {
        let version = try_get!(it.next());
        if version != SET_CLUSTER_API_VERSION {
            return Err(CmdParseError::InvalidVersion);
        }

        let epoch_str = try_get!(it.next());
        let epoch = try_parse!(epoch_str.parse::<u64>());

        let flags = ClusterMapFlags::from_arg(&try_get!(it.next()));

        if flags.compress {
            let compressed_data = it.next().ok_or_else(|| {
                error!("failed to get compressed data for UMCTL SETCLUSTER");
                CmdParseError::InvalidArgs
            })?;
            let data =
                ProxyClusterMetaData::from_compressed_data(compressed_data).map_err(|err| {
                    error!(
                        "failed to parse compressed data for UMCTL SETCLUSTER: {:?}",
                        err
                    );
                    CmdParseError::InvalidArgs
                })?;
            let ProxyClusterMetaData {
                cluster_name,
                local,
                peer,
                cluster_config,
            } = data;
            return Ok((
                Self {
                    version,
                    epoch,
                    flags,
                    cluster_name,
                    local: local.into_inner(),
                    peer: peer.into_inner(),
                    cluster_config: cluster_config.into_inner(),
                },
                Ok(()),
            ));
        }

        let cluster_name = try_get!(it.next());
        let cluster_name = ClusterName::try_from(cluster_name.as_str())
            .map_err(|_| CmdParseError::InvalidClusterName)?;

        let local = NodeMap::parse(it)?.into_inner();
        let mut peer = NodeMap::new(HashMap::default());
        let mut cluster_config = ClusterConfigData::default();
        let mut extended_meta_result = Ok(());

        while let Some(token) = it.next() {
            match token.to_uppercase().as_str() {
                PEER_PREFIX => peer = NodeMap::parse(it)?,
                CONFIG_PREFIX => match ClusterConfigData::parse(it) {
                    Ok(c) => cluster_config = c,
                    Err(_) => {
                        if local.is_empty() || peer.get_map().is_empty() {
                            return Err(CmdParseError::InvalidArgs);
                        } else {
                            error!("invalid cluster config from UMCTL SETCLUSTER but the local and peer metadata are complete. Ignore this error to protect the core functionality.");
                            extended_meta_result = Err(ParseExtendedMetaError {})
                        }
                    }
                },
                _ => return Err(CmdParseError::InvalidArgs),
            }
        }

        Ok((
            Self {
                version,
                epoch,
                flags,
                cluster_name,
                local,
                peer: peer.into_inner(),
                cluster_config: cluster_config.into_inner(),
            },
            extended_meta_result,
        ))
    }

    pub fn to_args(&self) -> Vec<String> {
        let mut args = vec![
            self.version.clone(),
            self.epoch.to_string(),
            self.flags.to_arg(),
            self.cluster_name.to_string(),
        ];
        let local = NodeMap::new(self.local.clone()).to_args();
        let peer = NodeMap::new(self.peer.clone()).to_args();
        let config = ClusterConfigData::new(self.cluster_config.clone()).to_args();
        args.extend_from_slice(&local);
        if !peer.is_empty() {
            args.push(PEER_PREFIX.to_string());
            args.extend_from_slice(&peer);
        }
        if !config.is_empty() {
            args.push(CONFIG_PREFIX.to_string());
            args.extend_from_slice(&config);
        }
        args
    }

    pub fn to_compressed_args(&self) -> Result<Vec<String>, MetaCompressError> {
        let data = ProxyClusterMetaData::new(
            self.cluster_name.clone(),
            NodeMap::new(self.local.clone()),
            NodeMap::new(self.peer.clone()),
            ClusterConfigData::new(self.cluster_config.clone()),
        )
        .gen_compressed_data()?;
        let args = vec![
            self.version.clone(),
            self.epoch.to_string(),
            self.flags.to_arg(),
            data,
        ];
        Ok(args)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct NodeMap(HashMap<String, Vec<SlotRange>>);

impl NodeMap {
    pub fn new(node_map: HashMap<String, Vec<SlotRange>>) -> Self {
        Self(node_map)
    }

    pub fn into_inner(self) -> HashMap<String, Vec<SlotRange>> {
        self.0
    }

    pub fn get_map(&self) -> &HashMap<String, Vec<SlotRange>> {
        &self.0
    }

    pub fn to_args(&self) -> Vec<String> {
        let mut args = vec![];
        for (node, slot_ranges) in self.0.iter() {
            for slot_range in slot_ranges {
                args.push(node.clone());
                args.extend(slot_range.clone().into_strings());
            }
        }
        args
    }

    fn parse<It>(it: &mut Peekable<It>) -> Result<Self, CmdParseError>
    where
        It: Iterator<Item = String>,
    {
        let mut node_map = HashMap::new();

        // To workaround lifetime problem.
        #[allow(clippy::while_let_loop)]
        loop {
            match it.peek() {
                Some(first_token) => {
                    let prefix = first_token.to_uppercase();
                    if prefix == PEER_PREFIX || prefix == CONFIG_PREFIX {
                        break;
                    }
                }
                None => break,
            }

            let (address, slot_range) = try_parse!(Self::parse_node(it));
            let slots = node_map.entry(address).or_insert_with(Vec::new);
            slots.push(slot_range);
        }

        Ok(Self(node_map))
    }

    fn parse_node<It>(it: &mut Peekable<It>) -> Result<(String, SlotRange), CmdParseError>
    where
        It: Iterator<Item = String>,
    {
        let addr = try_get!(it.next());
        let slot_range = try_parse!(Self::parse_tagged_slot_range(it));
        Ok((addr, slot_range))
    }

    fn parse_tagged_slot_range<It>(it: &mut Peekable<It>) -> Result<SlotRange, CmdParseError>
    where
        It: Iterator<Item = String>,
    {
        SlotRange::from_strings(it).ok_or(CmdParseError::InvalidSlots)
    }

    pub fn check_hosts(&self, announce_host: &str, cluster_name: &ClusterName) -> bool {
        for local_node_address in self.0.keys() {
            let host = match extract_host_from_address(local_node_address.as_str()) {
                Some(host) => host,
                None => {
                    error!("invalid local node address: {}", local_node_address);
                    return false;
                }
            };
            if host != announce_host {
                error!(
                    "not my announce host: {} {} != {}",
                    cluster_name, announce_host, local_node_address
                );
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Default)]
pub struct ClusterConfigData(ClusterConfig);

impl ClusterConfigData {
    pub fn new(config: ClusterConfig) -> Self {
        Self(config)
    }

    pub fn get_config(&self) -> &ClusterConfig {
        &self.0
    }

    pub fn into_inner(self) -> ClusterConfig {
        self.0
    }

    fn parse<It>(it: &mut Peekable<It>) -> Result<Self, CmdParseError>
    where
        It: Iterator<Item = String>,
    {
        let mut config = ClusterConfig::default();

        // To workaround lifetime problem.
        #[allow(clippy::while_let_loop)]
        loop {
            match it.peek() {
                Some(first_token) => {
                    let prefix = first_token.to_uppercase();
                    if prefix == PEER_PREFIX || prefix == CONFIG_PREFIX {
                        break;
                    }
                }
                None => break,
            }

            let field = try_get!(it.next());
            let value = try_get!(it.next());
            if let Err(err) = config.set_field(&field, &value) {
                warn!("failed to set config field {:?}", err);
                return Err(CmdParseError::InvalidConfig);
            }
        }

        Ok(Self(config))
    }

    pub fn to_args(&self) -> Vec<String> {
        let mut args = vec![];
        for (k, v) in self.0.to_str_map().into_iter() {
            args.push(k);
            args.push(v);
        }
        args
    }
}

#[derive(Debug)]
pub struct ParseExtendedMetaError {}

#[cfg(test)]
mod tests {
    use super::super::config::CompressionStrategy;
    use super::*;

    #[test]
    fn test_single_cluster() {
        let args = vec!["127.0.0.1:6379", "1", "0-1000"];
        let mut arguments = args.iter().map(|s| s.to_string()).peekable();
        let r = NodeMap::parse(&mut arguments);
        assert!(r.is_ok());
        let node_map = r.unwrap();
        assert_eq!(node_map.get_map().len(), 1);

        assert_eq!(node_map.to_args(), args);
    }

    #[test]
    fn test_multiple_slots() {
        let args = vec![
            "127.0.0.1:6379",
            "1",
            "0-1000",
            "127.0.0.1:6379",
            "1",
            "1001-2000",
        ];
        let mut arguments = args.iter().map(|s| s.to_string()).peekable();

        let node_map = NodeMap::parse(&mut arguments).unwrap();
        assert_eq!(node_map.get_map().len(), 1);
        assert_eq!(node_map.get_map().get("127.0.0.1:6379").unwrap().len(), 2);

        assert_eq!(node_map.to_args(), args);
    }

    #[test]
    fn test_multiple_nodes() {
        let args = vec![
            "127.0.0.1:7000",
            "1",
            "0-1000",
            "127.0.0.1:7001",
            "1",
            "1001-2000",
        ];
        let mut arguments = args.iter().map(|s| s.to_string()).peekable();
        let node_map = NodeMap::parse(&mut arguments).unwrap();
        assert_eq!(node_map.get_map().len(), 2);
        assert_eq!(node_map.get_map().get("127.0.0.1:7000").unwrap().len(), 1);
        assert_eq!(node_map.get_map().get("127.0.0.1:7001").unwrap().len(), 1);

        let mut expected_args = args.clone();
        let mut actual_args = node_map.to_args();
        expected_args.sort();
        actual_args.sort();
        assert_eq!(actual_args, expected_args);
    }

    #[test]
    fn test_clusters_config() {
        let args = vec![
            "compression_strategy",
            "allow_all",
            "migration_max_blocking_time",
            "66699",
            "migration_max_migration_time",
            "666",
        ];
        let mut it = args.iter().map(|s| s.to_string()).peekable();
        let cluster_config = ClusterConfigData::parse(&mut it).unwrap();
        assert_eq!(
            cluster_config.get_config().compression_strategy,
            CompressionStrategy::AllowAll
        );
        assert_eq!(
            cluster_config
                .get_config()
                .migration_config
                .max_migration_time,
            666
        );
        assert_eq!(
            cluster_config
                .get_config()
                .migration_config
                .max_blocking_time,
            66699
        );

        let mut result_args = cluster_config.to_args();

        let mut full_args = vec![
            "compression_strategy",
            "allow_all",
            "migration_max_migration_time",
            "666",
            "migration_max_blocking_time",
            "66699",
            "migration_scan_interval",
            "500",
            "migration_scan_count",
            "16",
        ];
        result_args.sort();
        full_args.sort();
        assert_eq!(result_args, full_args);
    }

    #[test]
    fn test_to_map() {
        let arguments = vec![
            "127.0.0.1:7000",
            "1",
            "0-1000",
            // another node
            "127.0.0.1:7001",
            "IMPORTING",
            "1",
            "1001-2000",
            "233",
            "127.0.0.2:7001",
            "127.0.0.2:6001",
            "127.0.0.1:7001",
            "127.0.0.1:6002",
            // another node
            "127.0.0.1:7002",
            "MIGRATING",
            "1",
            "0-2000",
            "666",
            "127.0.0.2:7001",
            "127.0.0.2:6001",
            "127.0.0.1:7001",
            "127.0.0.1:6002",
        ];
        let mut it = arguments
            .clone()
            .into_iter()
            .map(|s| s.to_string())
            .peekable();
        let node_map = NodeMap::parse(&mut it).unwrap();

        let mut args = node_map.to_args();
        let mut origin_args: Vec<String> = arguments.into_iter().map(|s| s.to_string()).collect();
        args.sort();
        origin_args.sort();
        assert_eq!(args, origin_args);
    }

    #[test]
    fn test_parse_proxy_cluster_meta() {
        let arguments = vec![
            SET_CLUSTER_API_VERSION,
            "233",
            "FORCE",
            "cluster_name",
            "127.0.0.1:7000",
            "1",
            "0-1000",
            "127.0.0.1:7001",
            "1",
            "1001-2000",
            "PEER",
            "127.0.0.2:7001",
            "1",
            "2001-3000",
            "127.0.0.2:7002",
            "1",
            "3001-4000",
            "CONFIG",
            "compression_strategy",
            "set_get_only",
        ];
        let mut it = arguments
            .clone()
            .into_iter()
            .map(|s| s.to_string())
            .peekable();

        let (cluster_meta, extended_res) = ProxyClusterMeta::parse(&mut it).unwrap();
        assert!(extended_res.is_ok());
        assert_eq!(cluster_meta.epoch, 233);
        assert!(cluster_meta.flags.force);
        let local = cluster_meta.local.clone();
        let peer = cluster_meta.peer.clone();
        let config = cluster_meta.cluster_config.clone();
        assert_eq!(local.len(), 2);
        assert_eq!(
            local.get("127.0.0.1:7000").unwrap()[0]
                .get_range_list()
                .get_ranges()[0]
                .start(),
            0
        );
        assert_eq!(
            local.get("127.0.0.1:7001").unwrap()[0]
                .get_range_list()
                .get_ranges()[0]
                .start(),
            1001
        );
        assert_eq!(peer.len(), 2);
        assert_eq!(
            peer.get("127.0.0.2:7001").unwrap()[0]
                .get_range_list()
                .get_ranges()[0]
                .start(),
            2001
        );
        assert_eq!(
            peer.get("127.0.0.2:7002").unwrap()[0]
                .get_range_list()
                .get_ranges()[0]
                .start(),
            3001
        );
        assert_eq!(config.compression_strategy, CompressionStrategy::SetGetOnly);

        let mut args = cluster_meta.to_args();
        let mut cluster_args: Vec<String> = arguments.into_iter().map(|s| s.to_string()).collect();
        let extended = vec![
            "migration_max_migration_time",
            "10800",
            "migration_max_blocking_time",
            "10000",
            "migration_scan_interval",
            "500",
            "migration_scan_count",
            "16",
        ]
        .into_iter()
        .map(|s| s.to_string());
        cluster_args.extend(extended);
        args.sort();
        cluster_args.sort();
        assert_eq!(args, cluster_args);

        let metadata = cluster_meta.gen_data();
        let d = metadata.gen_compressed_data().unwrap();
        let metadata2 = ProxyClusterMetaData::from_compressed_data(d).unwrap();
        assert_eq!(metadata, metadata2);
    }

    #[test]
    fn test_parse_proxy_cluster_meta_without_peer() {
        let arguments = vec![
            SET_CLUSTER_API_VERSION,
            "233",
            "FORCE",
            "cluster_name",
            "127.0.0.1:7000",
            "1",
            "0-1000",
            "CONFIG",
            "compression_strategy",
            "set_get_only",
        ];
        let mut it = arguments
            .clone()
            .into_iter()
            .map(|s| s.to_string())
            .peekable();

        let (cluster_meta, extended_res) = ProxyClusterMeta::parse(&mut it).unwrap();
        assert!(extended_res.is_ok());
        assert_eq!(cluster_meta.epoch, 233);
        assert!(cluster_meta.flags.force);
        assert_eq!(
            cluster_meta.get_config().compression_strategy,
            CompressionStrategy::SetGetOnly
        );

        let metadata = cluster_meta.gen_data();
        let d = metadata.gen_compressed_data().unwrap();
        let metadata2 = ProxyClusterMetaData::from_compressed_data(d).unwrap();
        assert_eq!(metadata, metadata2);
    }

    #[test]
    fn test_missing_config_cluster() {
        let arguments = vec![
            SET_CLUSTER_API_VERSION,
            "233",
            "FORCE",
            "cluster_name",
            "127.0.0.1:7000",
            "1",
            "0-1000",
            "PEER",
            "127.0.0.2:7001",
            "1",
            "2001-3000",
            "CONFIG",
            // "compression_strategy", missing field name
            "set_get_only",
        ];
        let mut it = arguments
            .clone()
            .into_iter()
            .map(|s| s.to_string())
            .peekable();

        let (cluster_meta, extended_res) = ProxyClusterMeta::parse(&mut it).unwrap();
        assert!(extended_res.is_err());
        assert_eq!(cluster_meta.epoch, 233);
        assert!(cluster_meta.flags.force);

        let metadata = cluster_meta.gen_data();
        let d = metadata.gen_compressed_data().unwrap();
        let metadata2 = ProxyClusterMetaData::from_compressed_data(d).unwrap();
        assert_eq!(metadata, metadata2);
    }

    #[test]
    fn test_invalid_config_field() {
        let arguments = vec![
            SET_CLUSTER_API_VERSION,
            "233",
            "FORCE",
            "cluster_name",
            "127.0.0.1:7000",
            "1",
            "0-1000",
            "PEER",
            "127.0.0.2:7001",
            "1",
            "2001-3000",
            "CONFIG",
            "config_field_that_does_not_exist",
            "invalid_value",
        ];
        let mut it = arguments
            .clone()
            .into_iter()
            .map(|s| s.to_string())
            .peekable();

        let (cluster_meta, extended_res) = ProxyClusterMeta::parse(&mut it).unwrap();
        assert!(extended_res.is_err());
        assert_eq!(cluster_meta.epoch, 233);
        assert!(cluster_meta.flags.force);

        let metadata = cluster_meta.gen_data();
        let d = metadata.gen_compressed_data().unwrap();
        let metadata2 = ProxyClusterMetaData::from_compressed_data(d).unwrap();
        assert_eq!(metadata, metadata2);
    }

    #[test]
    fn test_incomplete_main_meta_with_config_err() {
        let arguments = vec![
            SET_CLUSTER_API_VERSION,
            "233",
            "FORCE",
            "cluster_name",
            "127.0.0.1:7000",
            "1",
            "0-1000",
            "CONFIG",
            "cluster_name",
            "config_field_that_does_not_exist",
            "invalid_value",
        ];
        let mut it = arguments
            .clone()
            .into_iter()
            .map(|s| s.to_string())
            .peekable();

        assert!(ProxyClusterMeta::parse(&mut it).is_err());
    }
}
