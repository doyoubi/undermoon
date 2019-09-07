use super::cluster::{SlotRange, SlotRangeTag};
use super::utils::{has_flags, CmdParseError};
use ::common::config::ClusterConfig;
use protocol::{Array, BulkStr, Resp};
use std::collections::HashMap;
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
pub struct DBMapFlags {
    pub force: bool,
}

impl DBMapFlags {
    pub fn to_arg(&self) -> String {
        if self.force {
            "FORCE".to_string()
        } else {
            "NOFLAG".to_string()
        }
    }

    pub fn from_arg(flags_str: &str) -> Self {
        let force = has_flags(flags_str, ',', "FORCE");
        DBMapFlags { force }
    }
}

const PEER_PREFIX: &str = "PEER";
const CONFIG_PREFIX: &str = "CONFIG";

#[derive(Debug, Clone)]
pub struct ProxyDBMeta {
    epoch: u64,
    flags: DBMapFlags,
    local: HostDBMap,
    peer: HostDBMap,
    clusters_config: ClusterConfigMap,
}

impl ProxyDBMeta {
    pub fn new(
        epoch: u64,
        flags: DBMapFlags,
        local: HostDBMap,
        peer: HostDBMap,
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

    pub fn get_flags(&self) -> DBMapFlags {
        self.flags.clone()
    }

    pub fn get_local(&self) -> &HostDBMap {
        &self.local
    }
    pub fn get_peer(&self) -> &HostDBMap {
        &self.peer
    }

    pub fn get_configs(&self) -> &ClusterConfigMap {
        &self.clusters_config
    }

    pub fn from_resp(resp: &Resp) -> Result<Self, CmdParseError> {
        let arr = match resp {
            Resp::Arr(Array::Arr(ref arr)) => arr,
            _ => return Err(CmdParseError {}),
        };

        // Skip the "UMCTL SETDB"
        let it = arr.iter().skip(2).flat_map(|resp| match resp {
            Resp::Bulk(BulkStr::Str(safe_str)) => match str::from_utf8(safe_str) {
                Ok(s) => Some(s.to_string()),
                _ => None,
            },
            _ => None,
        });
        let mut it = it.peekable();

        Self::parse(&mut it)
    }

    pub fn parse<It>(it: &mut Peekable<It>) -> Result<Self, CmdParseError>
    where
        It: Iterator<Item = String>,
    {
        let epoch_str = try_get!(it.next());
        let epoch = try_parse!(epoch_str.parse::<u64>());

        let flags = DBMapFlags::from_arg(&try_get!(it.next()));

        let local = HostDBMap::parse(it)?;
        let mut peer = HostDBMap::new(HashMap::new());
        let mut clusters_config = ClusterConfigMap::default();
        while let Some(token) = it.next() {
            match token.to_uppercase().as_str() {
                PEER_PREFIX => peer = HostDBMap::parse(it)?,
                CONFIG_PREFIX => clusters_config = ClusterConfigMap::parse(it)?,
                _ => return Err(CmdParseError {}),
            }
        }

        Ok(Self {
            epoch,
            flags,
            local,
            peer,
            clusters_config,
        })
    }

    pub fn to_args(&self) -> Vec<String> {
        let mut args = vec![self.epoch.to_string(), self.flags.to_arg()];
        let local = self.local.db_map_to_args();
        let peer = self.peer.db_map_to_args();
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
pub struct HostDBMap {
    db_map: HashMap<String, HashMap<String, Vec<SlotRange>>>,
}

impl HostDBMap {
    pub fn new(db_map: HashMap<String, HashMap<String, Vec<SlotRange>>>) -> Self {
        Self { db_map }
    }

    pub fn get_map(&self) -> &HashMap<String, HashMap<String, Vec<SlotRange>>> {
        &self.db_map
    }

    pub fn db_map_to_args(&self) -> Vec<String> {
        let mut args = vec![];
        for (db_name, node_map) in &self.db_map {
            for (node, slot_ranges) in node_map {
                for slot_range in slot_ranges {
                    args.push(db_name.clone());
                    args.push(node.clone());
                    match &slot_range.tag {
                        SlotRangeTag::Migrating(ref meta) => {
                            args.push("migrating".to_string());
                            args.push(format!("{}-{}", slot_range.start, slot_range.end));
                            args.push(meta.epoch.to_string());
                            args.push(meta.src_proxy_address.clone());
                            args.push(meta.src_node_address.clone());
                            args.push(meta.dst_proxy_address.clone());
                            args.push(meta.dst_node_address.clone());
                        }
                        SlotRangeTag::Importing(ref meta) => {
                            args.push("importing".to_string());
                            args.push(format!("{}-{}", slot_range.start, slot_range.end));
                            args.push(meta.epoch.to_string());
                            args.push(meta.src_proxy_address.clone());
                            args.push(meta.src_node_address.clone());
                            args.push(meta.dst_proxy_address.clone());
                            args.push(meta.dst_node_address.clone());
                        }
                        SlotRangeTag::None => {
                            args.push(format!("{}-{}", slot_range.start, slot_range.end));
                        }
                    };
                }
            }
        }
        args
    }

    fn parse<It>(it: &mut Peekable<It>) -> Result<Self, CmdParseError>
    where
        It: Iterator<Item = String>,
    {
        let mut db_map = HashMap::new();

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

            let (dbname, address, slot_range) = try_parse!(Self::parse_db(it));
            let db = db_map.entry(dbname).or_insert_with(HashMap::new);
            let slots = db.entry(address).or_insert_with(Vec::new);
            slots.push(slot_range);
        }

        Ok(Self { db_map })
    }

    fn parse_db<It>(it: &mut It) -> Result<(String, String, SlotRange), CmdParseError>
    where
        It: Iterator<Item = String>,
    {
        let dbname = try_get!(it.next());
        let addr = try_get!(it.next());
        let slot_range = try_parse!(Self::parse_tagged_slot_range(it));
        Ok((dbname, addr, slot_range))
    }

    fn parse_tagged_slot_range<It>(it: &mut It) -> Result<SlotRange, CmdParseError>
    where
        It: Iterator<Item = String>,
    {
        SlotRange::from_strings(it).ok_or_else(|| CmdParseError {})
    }
}

#[derive(Debug, Clone)]
pub struct ClusterConfigMap {
    config_map: HashMap<String, ClusterConfig>,
}

impl Default for ClusterConfigMap {
    fn default() -> Self {
        Self {
            config_map: HashMap::new(),
        }
    }
}

impl ClusterConfigMap {
    pub fn new(config_map: HashMap<String, ClusterConfig>) -> Self {
        Self { config_map }
    }

    pub fn get(&self, dbname: &str) -> ClusterConfig {
        self.config_map
            .get(dbname)
            .cloned()
            .unwrap_or_else(ClusterConfig::default)
    }

    pub fn get_map(&self) -> &HashMap<String, ClusterConfig> {
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

            let (dbname, field, value) = try_parse!(Self::parse_config(it));
            let cluster_config = config_map
                .entry(dbname)
                .or_insert_with(ClusterConfig::default);
            if let Err(err) = cluster_config.set_field(&field, &value) {
                warn!("failed to set config field {:?}", err);
            }
        }

        Ok(Self { config_map })
    }

    fn parse_config<It>(it: &mut It) -> Result<(String, String, String), CmdParseError>
    where
        It: Iterator<Item = String>,
    {
        let dbname = try_get!(it.next());
        let field = try_get!(it.next());
        let value = try_get!(it.next());
        Ok((dbname, field, value))
    }

    pub fn to_args(&self) -> Vec<String> {
        let mut args = vec![];
        for (db_name, config) in &self.config_map {
            for (k, v) in config.to_str_map().into_iter() {
                args.push(db_name.clone());
                args.push(k);
                args.push(v);
            }
        }
        args
    }
}

#[cfg(test)]
mod tests {
    use super::super::config::CompressionStrategy;
    use super::*;

    #[test]
    fn test_single_db() {
        let mut arguments = vec!["dbname", "127.0.0.1:6379", "0-1000"]
            .into_iter()
            .map(|s| s.to_string())
            .peekable();
        let r = HostDBMap::parse(&mut arguments);
        assert!(r.is_ok());
        let host_db_map = r.expect("test_single_db");
        assert_eq!(host_db_map.db_map.len(), 1);
    }

    #[test]
    fn test_multiple_slots() {
        let mut arguments = vec![
            "dbname",
            "127.0.0.1:6379",
            "0-1000",
            "dbname",
            "127.0.0.1:6379",
            "1001-2000",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .peekable();

        let r = HostDBMap::parse(&mut arguments);
        assert!(r.is_ok());
        let host_db_map = r.expect("test_multiple_slots");
        assert_eq!(host_db_map.db_map.len(), 1);
        assert_eq!(
            host_db_map
                .db_map
                .get("dbname")
                .expect("test_multiple_slots")
                .len(),
            1
        );
        assert_eq!(
            host_db_map
                .db_map
                .get("dbname")
                .expect("test_multiple_slots")
                .get("127.0.0.1:6379")
                .expect("test_multiple_slots")
                .len(),
            2
        );
    }

    #[test]
    fn test_multiple_nodes() {
        let mut arguments = vec![
            "dbname",
            "127.0.0.1:7000",
            "0-1000",
            "dbname",
            "127.0.0.1:7001",
            "1001-2000",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .peekable();
        let r = HostDBMap::parse(&mut arguments);
        assert!(r.is_ok());
        let host_db_map = r.expect("test_multiple_nodes");
        assert_eq!(host_db_map.db_map.len(), 1);
        assert_eq!(
            host_db_map
                .db_map
                .get("dbname")
                .expect("test_multiple_nodes")
                .len(),
            2
        );
        assert_eq!(
            host_db_map
                .db_map
                .get("dbname")
                .expect("test_multiple_nodes")
                .get("127.0.0.1:7000")
                .expect("test_multiple_nodes")
                .len(),
            1
        );
        assert_eq!(
            host_db_map
                .db_map
                .get("dbname")
                .expect("test_multiple_nodes")
                .get("127.0.0.1:7001")
                .expect("test_multiple_nodes")
                .len(),
            1
        );
    }

    #[test]
    fn test_multiple_db() {
        let mut arguments = vec![
            "dbname",
            "127.0.0.1:7000",
            "0-1000",
            "dbname",
            "127.0.0.1:7001",
            "1001-2000",
            "another_db",
            "127.0.0.1:7002",
            "0-2000",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .peekable();
        let r = HostDBMap::parse(&mut arguments);
        assert!(r.is_ok());
        let host_db_map = r.expect("test_multiple_db");
        assert_eq!(host_db_map.db_map.len(), 2);
        assert_eq!(
            host_db_map
                .db_map
                .get("dbname")
                .expect("test_multiple_db")
                .len(),
            2
        );
        assert_eq!(
            host_db_map
                .db_map
                .get("dbname")
                .expect("test_multiple_db")
                .get("127.0.0.1:7000")
                .expect("test_multiple_db")
                .len(),
            1
        );
        assert_eq!(
            host_db_map
                .db_map
                .get("dbname")
                .expect("test_multiple_db")
                .get("127.0.0.1:7001")
                .expect("test_multiple_db")
                .len(),
            1
        );
        assert_eq!(
            host_db_map
                .db_map
                .get("another_db")
                .expect("test_multiple_nodes")
                .len(),
            1
        );
        assert_eq!(
            host_db_map
                .db_map
                .get("another_db")
                .expect("test_multiple_db")
                .get("127.0.0.1:7002")
                .expect("test_multiple_db")
                .len(),
            1
        );
    }

    #[test]
    fn test_clusters_config() {
        let args = vec![
            "mydb",
            "compression_strategy",
            "allow_all",
            "otherdb",
            "migration_offset_threshold",
            "233",
            "mydb",
            "migration_max_migration_time",
            "666",
        ];
        let mut it = args.iter().map(|s| s.to_string()).peekable();
        let clusters_config = ClusterConfigMap::parse(&mut it).expect("test_clusters_config");
        assert_eq!(clusters_config.config_map.len(), 2);
        assert_eq!(
            clusters_config
                .config_map
                .get("mydb")
                .expect("test_clusters_config")
                .compression_strategy,
            CompressionStrategy::AllowAll
        );
        assert_eq!(
            clusters_config
                .config_map
                .get("mydb")
                .expect("test_clusters_config")
                .migration_config
                .max_migration_time,
            666
        );
        assert_eq!(
            clusters_config
                .config_map
                .get("otherdb")
                .expect("test_clusters_config")
                .migration_config
                .offset_threshold,
            233
        );

        let mut result_args = clusters_config.to_args();
        result_args.sort();
        let mut full_args = vec![
            "mydb",
            "compression_strategy",
            "allow_all",
            "mydb",
            "migration_offset_threshold",
            "50000",
            "mydb",
            "migration_max_migration_time",
            "666",
            "mydb",
            "migration_max_blocking_time",
            "10000",
            "mydb",
            "migration_min_blocking_time",
            "100",
            "mydb",
            "migration_max_redirection_time",
            "5000",
            "mydb",
            "migration_switch_retry_interval",
            "10",
            "otherdb",
            "compression_strategy",
            "disabled",
            "otherdb",
            "migration_offset_threshold",
            "233",
            "otherdb",
            "migration_max_migration_time",
            "600000",
            "otherdb",
            "migration_max_blocking_time",
            "10000",
            "otherdb",
            "migration_min_blocking_time",
            "100",
            "otherdb",
            "migration_max_redirection_time",
            "5000",
            "otherdb",
            "migration_switch_retry_interval",
            "10",
        ];
        full_args.sort();
        assert_eq!(result_args, full_args);
    }

    #[test]
    fn test_to_map() {
        let arguments = vec![
            "dbname",
            "127.0.0.1:7000",
            "0-1000",
            "dbname",
            "127.0.0.1:7001",
            "importing",
            "1001-2000",
            "233",
            "127.0.0.2:7001",
            "127.0.0.2:6001",
            "127.0.0.1:7001",
            "127.0.0.1:6002",
            "another_db",
            "127.0.0.1:7002",
            "migrating",
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
        let r = HostDBMap::parse(&mut it);
        let host_db_map = r.expect("test_to_map");

        let db_map = HostDBMap::new(host_db_map.db_map);
        let mut args = db_map.db_map_to_args();
        let mut db_args: Vec<String> = arguments.into_iter().map(|s| s.to_string()).collect();
        args.sort();
        db_args.sort();
        assert_eq!(args, db_args);
    }

    #[test]
    fn test_parse_proxy_db_meta() {
        let arguments = vec![
            "233",
            "FORCE",
            "dbname",
            "127.0.0.1:7000",
            "0-1000",
            "dbname",
            "127.0.0.1:7001",
            "1001-2000",
            "PEER",
            "dbname",
            "127.0.0.2:7001",
            "2001-3000",
            "dbname",
            "127.0.0.2:7002",
            "3001-4000",
            "CONFIG",
            "dbname",
            "compression_strategy",
            "set_get_only",
        ];
        let mut it = arguments
            .clone()
            .into_iter()
            .map(|s| s.to_string())
            .peekable();

        let db_meta = ProxyDBMeta::parse(&mut it).expect("test_parse_proxy_db_meta");
        assert_eq!(db_meta.epoch, 233);
        assert!(db_meta.flags.force);
        let local = db_meta.local.get_map();
        let peer = db_meta.peer.get_map();
        let config = db_meta.clusters_config.get_map();
        assert_eq!(local.len(), 1);
        assert_eq!(
            local.get("dbname").expect("test_parse_proxy_db_meta").len(),
            2
        );
        assert_eq!(
            local
                .get("dbname")
                .expect("test_parse_proxy_db_meta")
                .get("127.0.0.1:7000")
                .expect("test_parse_proxy_db_meta")
                .get(0)
                .expect("test_parse_proxy_db_meta")
                .start,
            0
        );
        assert_eq!(
            local
                .get("dbname")
                .expect("test_parse_proxy_db_meta")
                .get("127.0.0.1:7001")
                .expect("test_parse_proxy_db_meta")
                .get(0)
                .expect("test_parse_proxy_db_meta")
                .start,
            1001
        );
        assert_eq!(peer.len(), 1);
        assert_eq!(
            peer.get("dbname").expect("test_parse_proxy_db_meta").len(),
            2
        );
        assert_eq!(
            peer.get("dbname")
                .expect("test_parse_proxy_db_meta")
                .get("127.0.0.2:7001")
                .expect("test_parse_proxy_db_meta")
                .get(0)
                .expect("test_parse_proxy_db_meta")
                .start,
            2001
        );
        assert_eq!(
            peer.get("dbname")
                .expect("test_parse_proxy_db_meta")
                .get("127.0.0.2:7002")
                .expect("test_parse_proxy_db_meta")
                .get(0)
                .expect("test_parse_proxy_db_meta")
                .start,
            3001
        );
        assert_eq!(config.len(), 1);
        assert_eq!(
            config
                .get("dbname")
                .expect("test_parse_proxy_db_meta")
                .compression_strategy,
            CompressionStrategy::SetGetOnly
        );

        let mut args = db_meta.to_args();
        let mut db_args: Vec<String> = arguments.into_iter().map(|s| s.to_string()).collect();
        let extended = vec![
            "dbname",
            "migration_offset_threshold",
            "50000",
            "dbname",
            "migration_max_migration_time",
            "600000",
            "dbname",
            "migration_max_blocking_time",
            "10000",
            "dbname",
            "migration_min_blocking_time",
            "100",
            "dbname",
            "migration_max_redirection_time",
            "5000",
            "dbname",
            "migration_switch_retry_interval",
            "10",
        ]
        .into_iter()
        .map(|s| s.to_string());
        db_args.extend(extended);
        args.sort();
        db_args.sort();
        assert_eq!(args, db_args);
    }
}
