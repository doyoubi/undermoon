use super::cluster::SlotRange;
use super::utils::{has_flags, CmdParseError};
use crate::common::cluster::ClusterName;
use crate::common::config::ClusterConfig;
use crate::protocol::{Array, BulkStr, Resp};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::iter::Peekable;
use std::str;

macro_rules! try_parse {
    ($expression:expr) => {{
        match $expression {
            Ok(v) => (v),
            Err(_) => return Err(CmdParseError {}),
        }
    }};
}

macro_rules! try_get {
    ($expression:expr) => {{
        match $expression {
            Some(v) => (v),
            None => return Err(CmdParseError {}),
        }
    }};
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClusterMapFlags {
    pub force: bool,
}

impl ClusterMapFlags {
    pub fn to_arg(&self) -> String {
        if self.force {
            "FORCE".to_string()
        } else {
            "NOFLAG".to_string()
        }
    }

    pub fn from_arg(flags_str: &str) -> Self {
        let force = has_flags(flags_str, ',', "FORCE");
        ClusterMapFlags { force }
    }
}

const PEER_PREFIX: &str = "PEER";
const CONFIG_PREFIX: &str = "CONFIG";

#[derive(Debug, Clone)]
pub struct ProxyClusterMeta {
    epoch: u64,
    flags: ClusterMapFlags,
    local: ProxyClusterMap,
    peer: ProxyClusterMap,
    clusters_config: ClusterConfigMap,
}

impl ProxyClusterMeta {
    pub fn new(
        epoch: u64,
        flags: ClusterMapFlags,
        local: ProxyClusterMap,
        peer: ProxyClusterMap,
        clusters_config: ClusterConfigMap,
    ) -> Self {
        Self {
            epoch,
            flags,
            local,
            peer,
            clusters_config,
        }
    }

    pub fn get_epoch(&self) -> u64 {
        self.epoch
    }

    pub fn get_flags(&self) -> ClusterMapFlags {
        self.flags.clone()
    }

    pub fn get_local(&self) -> &ProxyClusterMap {
        &self.local
    }
    pub fn get_peer(&self) -> &ProxyClusterMap {
        &self.peer
    }

    pub fn get_configs(&self) -> &ClusterConfigMap {
        &self.clusters_config
    }

    pub fn from_resp<T: AsRef<[u8]>>(
        resp: &Resp<T>,
    ) -> Result<(Self, Result<(), ParseExtendedMetaError>), CmdParseError> {
        let arr = match resp {
            Resp::Arr(Array::Arr(ref arr)) => arr,
            _ => return Err(CmdParseError {}),
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
        let epoch_str = try_get!(it.next());
        let epoch = try_parse!(epoch_str.parse::<u64>());

        let flags = ClusterMapFlags::from_arg(&try_get!(it.next()));

        let local = ProxyClusterMap::parse(it)?;
        let mut peer = ProxyClusterMap::new(HashMap::new());
        let mut clusters_config = ClusterConfigMap::default();
        let mut extended_meta_result = Ok(());

        while let Some(token) = it.next() {
            match token.to_uppercase().as_str() {
                PEER_PREFIX => peer = ProxyClusterMap::parse(it)?,
                CONFIG_PREFIX => match ClusterConfigMap::parse(it) {
                    Ok(c) => clusters_config = c,
                    Err(_) => {
                        if local.get_map().is_empty() || peer.get_map().is_empty() {
                            return Err(CmdParseError {});
                        } else {
                            error!("invalid cluster config from UMCTL SETCLUSTER but the local and peer metadata are complete. Ignore this error to protect the core functionality.");
                            extended_meta_result = Err(ParseExtendedMetaError {})
                        }
                    }
                },
                _ => return Err(CmdParseError {}),
            }
        }

        Ok((
            Self {
                epoch,
                flags,
                local,
                peer,
                clusters_config,
            },
            extended_meta_result,
        ))
    }

    pub fn to_args(&self) -> Vec<String> {
        let mut args = vec![self.epoch.to_string(), self.flags.to_arg()];
        let local = self.local.cluster_map_to_args();
        let peer = self.peer.cluster_map_to_args();
        let config = self.clusters_config.to_args();
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
}

#[derive(Debug, Clone)]
pub struct ProxyClusterMap {
    cluster_map: HashMap<ClusterName, HashMap<String, Vec<SlotRange>>>,
}

impl ProxyClusterMap {
    pub fn new(cluster_map: HashMap<ClusterName, HashMap<String, Vec<SlotRange>>>) -> Self {
        Self { cluster_map }
    }

    pub fn get_map(&self) -> &HashMap<ClusterName, HashMap<String, Vec<SlotRange>>> {
        &self.cluster_map
    }

    pub fn cluster_map_to_args(&self) -> Vec<String> {
        let mut args = vec![];
        for (cluster_name, node_map) in &self.cluster_map {
            for (node, slot_ranges) in node_map {
                for slot_range in slot_ranges {
                    args.push(cluster_name.to_string());
                    args.push(node.clone());
                    args.extend(slot_range.clone().into_strings());
                }
            }
        }
        args
    }

    fn parse<It>(it: &mut Peekable<It>) -> Result<Self, CmdParseError>
    where
        It: Iterator<Item = String>,
    {
        let mut cluster_map = HashMap::new();

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

            let (cluster_name, address, slot_range) = try_parse!(Self::parse_cluster(it));
            let cluster = cluster_map.entry(cluster_name).or_insert_with(HashMap::new);
            let slots = cluster.entry(address).or_insert_with(Vec::new);
            slots.push(slot_range);
        }

        Ok(Self { cluster_map })
    }

    fn parse_cluster<It>(
        it: &mut Peekable<It>,
    ) -> Result<(ClusterName, String, SlotRange), CmdParseError>
    where
        It: Iterator<Item = String>,
    {
        let cluster_name = try_get!(it.next());
        let cluster_name =
            ClusterName::try_from(cluster_name.as_str()).map_err(|_| CmdParseError {})?;
        let addr = try_get!(it.next());
        let slot_range = try_parse!(Self::parse_tagged_slot_range(it));
        Ok((cluster_name, addr, slot_range))
    }

    fn parse_tagged_slot_range<It>(it: &mut Peekable<It>) -> Result<SlotRange, CmdParseError>
    where
        It: Iterator<Item = String>,
    {
        SlotRange::from_strings(it).ok_or_else(|| CmdParseError {})
    }
}

#[derive(Debug, Clone)]
pub struct ClusterConfigMap {
    config_map: HashMap<ClusterName, ClusterConfig>,
}

impl Default for ClusterConfigMap {
    fn default() -> Self {
        Self {
            config_map: HashMap::new(),
        }
    }
}

impl ClusterConfigMap {
    pub fn new(config_map: HashMap<ClusterName, ClusterConfig>) -> Self {
        Self { config_map }
    }

    pub fn get_or_default(&self, cluster_name: &ClusterName) -> ClusterConfig {
        self.config_map
            .get(cluster_name)
            .cloned()
            .unwrap_or_else(ClusterConfig::default)
    }

    pub fn get(&self, cluster_name: &ClusterName) -> Option<ClusterConfig> {
        self.config_map.get(cluster_name).cloned()
    }

    pub fn get_map(&self) -> &HashMap<ClusterName, ClusterConfig> {
        &self.config_map
    }

    fn parse<It>(it: &mut Peekable<It>) -> Result<Self, CmdParseError>
    where
        It: Iterator<Item = String>,
    {
        let mut config_map = HashMap::new();

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

            let (cluster_name, field, value) = try_parse!(Self::parse_config(it));
            let cluster_config = config_map
                .entry(cluster_name)
                .or_insert_with(ClusterConfig::default);
            if let Err(err) = cluster_config.set_field(&field, &value) {
                warn!("failed to set config field {:?}", err);
                return Err(CmdParseError {});
            }
        }

        Ok(Self { config_map })
    }

    fn parse_config<It>(it: &mut It) -> Result<(ClusterName, String, String), CmdParseError>
    where
        It: Iterator<Item = String>,
    {
        let cluster_name = try_get!(it.next());
        let cluster_name =
            ClusterName::try_from(cluster_name.as_str()).map_err(|_| CmdParseError {})?;
        let field = try_get!(it.next());
        let value = try_get!(it.next());
        Ok((cluster_name, field, value))
    }

    pub fn to_args(&self) -> Vec<String> {
        let mut args = vec![];
        for (cluster_name, config) in &self.config_map {
            for (k, v) in config.to_str_map().into_iter() {
                args.push(cluster_name.to_string());
                args.push(k);
                args.push(v);
            }
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
        let args = vec!["cluster_name", "127.0.0.1:6379", "1", "0-1000"];
        let mut arguments = args.iter().map(|s| s.to_string()).peekable();
        let r = ProxyClusterMap::parse(&mut arguments);
        assert!(r.is_ok());
        let proxy_cluster_map = r.unwrap();
        assert_eq!(proxy_cluster_map.cluster_map.len(), 1);

        assert_eq!(proxy_cluster_map.cluster_map_to_args(), args);
    }

    #[test]
    fn test_multiple_slots() {
        let args = vec![
            "cluster_name",
            "127.0.0.1:6379",
            "1",
            "0-1000",
            "cluster_name",
            "127.0.0.1:6379",
            "1",
            "1001-2000",
        ];
        let mut arguments = args.iter().map(|s| s.to_string()).peekable();

        let r = ProxyClusterMap::parse(&mut arguments);
        assert!(r.is_ok());
        let proxy_cluster_map = r.unwrap();
        assert_eq!(proxy_cluster_map.cluster_map.len(), 1);
        let cluster_name = ClusterName::try_from("cluster_name").unwrap();
        assert_eq!(
            proxy_cluster_map
                .cluster_map
                .get(&cluster_name)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            proxy_cluster_map
                .cluster_map
                .get(&cluster_name)
                .unwrap()
                .get("127.0.0.1:6379")
                .unwrap()
                .len(),
            2
        );

        assert_eq!(proxy_cluster_map.cluster_map_to_args(), args);
    }

    #[test]
    fn test_multiple_nodes() {
        let args = vec![
            "cluster_name",
            "127.0.0.1:7000",
            "1",
            "0-1000",
            "cluster_name",
            "127.0.0.1:7001",
            "1",
            "1001-2000",
        ];
        let mut arguments = args.iter().map(|s| s.to_string()).peekable();
        let proxy_cluster_map = ProxyClusterMap::parse(&mut arguments).unwrap();
        assert_eq!(proxy_cluster_map.cluster_map.len(), 1);
        let cluster_name = ClusterName::try_from("cluster_name").unwrap();
        assert_eq!(
            proxy_cluster_map
                .cluster_map
                .get(&cluster_name)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            proxy_cluster_map
                .cluster_map
                .get(&cluster_name)
                .unwrap()
                .get("127.0.0.1:7000")
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            proxy_cluster_map
                .cluster_map
                .get(&cluster_name)
                .unwrap()
                .get("127.0.0.1:7001")
                .unwrap()
                .len(),
            1
        );

        let mut expected_args = args.clone();
        let mut actual_args = proxy_cluster_map.cluster_map_to_args();
        expected_args.sort();
        actual_args.sort();
        assert_eq!(actual_args, expected_args);
    }

    #[test]
    fn test_multiple_cluster() {
        let args = vec![
            "cluster_name",
            "127.0.0.1:7000",
            "1",
            "0-1000",
            "cluster_name",
            "127.0.0.1:7001",
            "1",
            "1001-2000",
            "another_cluster",
            "127.0.0.1:7002",
            "1",
            "0-2000",
        ];
        let mut arguments = args.iter().map(|s| s.to_string()).peekable();
        let r = ProxyClusterMap::parse(&mut arguments);
        assert!(r.is_ok());
        let proxy_cluster_map = r.unwrap();
        assert_eq!(proxy_cluster_map.cluster_map.len(), 2);
        let cluster_name = ClusterName::try_from("cluster_name").unwrap();
        assert_eq!(
            proxy_cluster_map
                .cluster_map
                .get(&cluster_name)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            proxy_cluster_map
                .cluster_map
                .get(&cluster_name)
                .unwrap()
                .get("127.0.0.1:7000")
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            proxy_cluster_map
                .cluster_map
                .get(&cluster_name)
                .unwrap()
                .get("127.0.0.1:7001")
                .unwrap()
                .len(),
            1
        );
        let another_cluster = ClusterName::try_from("another_cluster").unwrap();
        assert_eq!(
            proxy_cluster_map
                .cluster_map
                .get(&another_cluster)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            proxy_cluster_map
                .cluster_map
                .get(&another_cluster)
                .unwrap()
                .get("127.0.0.1:7002")
                .unwrap()
                .len(),
            1
        );

        let mut expected_args = args.clone();
        let mut actual_args = proxy_cluster_map.cluster_map_to_args();
        expected_args.sort();
        actual_args.sort();
        assert_eq!(actual_args, expected_args);
    }

    #[test]
    fn test_clusters_config() {
        let args = vec![
            "mycluster",
            "compression_strategy",
            "allow_all",
            "othercluster",
            "migration_max_blocking_time",
            "66699",
            "mycluster",
            "migration_max_migration_time",
            "666",
        ];
        let mut it = args.iter().map(|s| s.to_string()).peekable();
        let clusters_config = ClusterConfigMap::parse(&mut it).unwrap();
        assert_eq!(clusters_config.config_map.len(), 2);
        let mycluster = ClusterName::try_from("mycluster").unwrap();
        assert_eq!(
            clusters_config
                .config_map
                .get(&mycluster)
                .unwrap()
                .compression_strategy,
            CompressionStrategy::AllowAll
        );
        assert_eq!(
            clusters_config
                .config_map
                .get(&mycluster)
                .unwrap()
                .migration_config
                .max_migration_time,
            666
        );
        assert_eq!(
            clusters_config
                .config_map
                .get(&ClusterName::try_from("othercluster").unwrap())
                .unwrap()
                .migration_config
                .max_blocking_time,
            66699
        );

        let mut result_args = clusters_config.to_args();

        let mut full_args = vec![
            "mycluster",
            "compression_strategy",
            "allow_all",
            "mycluster",
            "migration_max_migration_time",
            "666",
            "mycluster",
            "migration_max_blocking_time",
            "10000",
            "mycluster",
            "migration_scan_interval",
            "500",
            "mycluster",
            "migration_scan_count",
            "16",
            "othercluster",
            "compression_strategy",
            "disabled",
            "othercluster",
            "migration_max_migration_time",
            "10800",
            "othercluster",
            "migration_max_blocking_time",
            "66699",
            "othercluster",
            "migration_scan_interval",
            "500",
            "othercluster",
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
            "cluster_name",
            "127.0.0.1:7000",
            "1",
            "0-1000",
            "cluster_name",
            "127.0.0.1:7001",
            "IMPORTING",
            "1",
            "1001-2000",
            "233",
            "127.0.0.2:7001",
            "127.0.0.2:6001",
            "127.0.0.1:7001",
            "127.0.0.1:6002",
            "another_cluster",
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
        let r = ProxyClusterMap::parse(&mut it);
        let proxy_cluster_map = r.unwrap();

        let cluster_map = ProxyClusterMap::new(proxy_cluster_map.cluster_map);
        let mut args = cluster_map.cluster_map_to_args();
        let mut cluster_args: Vec<String> = arguments.into_iter().map(|s| s.to_string()).collect();
        args.sort();
        cluster_args.sort();
        assert_eq!(args, cluster_args);
    }

    #[test]
    fn test_parse_proxy_cluster_meta() {
        let arguments = vec![
            "233",
            "FORCE",
            "cluster_name",
            "127.0.0.1:7000",
            "1",
            "0-1000",
            "cluster_name",
            "127.0.0.1:7001",
            "1",
            "1001-2000",
            "PEER",
            "cluster_name",
            "127.0.0.2:7001",
            "1",
            "2001-3000",
            "cluster_name",
            "127.0.0.2:7002",
            "1",
            "3001-4000",
            "CONFIG",
            "cluster_name",
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
        let local = cluster_meta.local.get_map();
        let peer = cluster_meta.peer.get_map();
        let config = cluster_meta.clusters_config.get_map();
        assert_eq!(local.len(), 1);
        let cluster_name = ClusterName::try_from("cluster_name").unwrap();
        assert_eq!(local.get(&cluster_name).unwrap().len(), 2);
        assert_eq!(
            local
                .get(&cluster_name)
                .unwrap()
                .get("127.0.0.1:7000")
                .unwrap()[0]
                .get_range_list()
                .get_ranges()[0]
                .start(),
            0
        );
        assert_eq!(
            local
                .get(&cluster_name)
                .unwrap()
                .get("127.0.0.1:7001")
                .unwrap()[0]
                .get_range_list()
                .get_ranges()[0]
                .start(),
            1001
        );
        assert_eq!(peer.len(), 1);
        assert_eq!(peer.get(&cluster_name).unwrap().len(), 2);
        assert_eq!(
            peer.get(&cluster_name)
                .unwrap()
                .get("127.0.0.2:7001")
                .unwrap()[0]
                .get_range_list()
                .get_ranges()[0]
                .start(),
            2001
        );
        assert_eq!(
            peer.get(&cluster_name)
                .unwrap()
                .get("127.0.0.2:7002")
                .unwrap()[0]
                .get_range_list()
                .get_ranges()[0]
                .start(),
            3001
        );
        assert_eq!(config.len(), 1);
        assert_eq!(
            config.get(&cluster_name).unwrap().compression_strategy,
            CompressionStrategy::SetGetOnly
        );

        let mut args = cluster_meta.to_args();
        let mut cluster_args: Vec<String> = arguments.into_iter().map(|s| s.to_string()).collect();
        let extended = vec![
            "cluster_name",
            "migration_max_migration_time",
            "10800",
            "cluster_name",
            "migration_max_blocking_time",
            "10000",
            "cluster_name",
            "migration_scan_interval",
            "500",
            "cluster_name",
            "migration_scan_count",
            "16",
        ]
        .into_iter()
        .map(|s| s.to_string());
        cluster_args.extend(extended);
        args.sort();
        cluster_args.sort();
        assert_eq!(args, cluster_args);
    }

    #[test]
    fn test_parse_proxy_cluster_meta_without_peer() {
        let arguments = vec![
            "233",
            "FORCE",
            "cluster_name",
            "127.0.0.1:7000",
            "1",
            "0-1000",
            "CONFIG",
            "cluster_name",
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
            cluster_meta
                .get_configs()
                .get_or_default(&ClusterName::try_from("cluster_name").unwrap())
                .compression_strategy,
            CompressionStrategy::SetGetOnly
        );
    }

    #[test]
    fn test_missing_config_cluster() {
        let arguments = vec![
            "233",
            "FORCE",
            "cluster_name",
            "127.0.0.1:7000",
            "1",
            "0-1000",
            "PEER",
            "cluster_name",
            "127.0.0.2:7001",
            "1",
            "2001-3000",
            "CONFIG",
            // "cluster_name", missing cluster_name
            "compression_strategy",
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
    }

    #[test]
    fn test_invalid_config_field() {
        let arguments = vec![
            "233",
            "FORCE",
            "cluster_name",
            "127.0.0.1:7000",
            "1",
            "0-1000",
            "PEER",
            "cluster_name",
            "127.0.0.2:7001",
            "1",
            "2001-3000",
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

        let (cluster_meta, extended_res) = ProxyClusterMeta::parse(&mut it).unwrap();
        assert!(extended_res.is_err());
        assert_eq!(cluster_meta.epoch, 233);
        assert!(cluster_meta.flags.force);
    }

    #[test]
    fn test_incomplete_main_meta_with_config_err() {
        let arguments = vec![
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
