use super::cluster::SlotRange;
use super::utils::{has_flags, CmdParseError};
use crate::common::cluster::DBName;
use crate::common::config::ClusterConfig;
use crate::protocol::{Array, BulkStr, Resp};
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
    local: ProxyDBMap,
    peer: ProxyDBMap,
    clusters_config: ClusterConfigMap,
}

impl ProxyDBMeta {
    pub fn new(
        epoch: u64,
        flags: DBMapFlags,
        local: ProxyDBMap,
        peer: ProxyDBMap,
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

    pub fn get_local(&self) -> &ProxyDBMap {
        &self.local
    }
    pub fn get_peer(&self) -> &ProxyDBMap {
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

        // Skip the "UMCTL SETDB"
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

        let flags = DBMapFlags::from_arg(&try_get!(it.next()));

        let local = ProxyDBMap::parse(it)?;
        let mut peer = ProxyDBMap::new(HashMap::new());
        let mut clusters_config = ClusterConfigMap::default();
        let mut extended_meta_result = Ok(());

        while let Some(token) = it.next() {
            match token.to_uppercase().as_str() {
                PEER_PREFIX => peer = ProxyDBMap::parse(it)?,
                CONFIG_PREFIX => match ClusterConfigMap::parse(it) {
                    Ok(c) => clusters_config = c,
                    Err(_) => {
                        if local.get_map().is_empty() || peer.get_map().is_empty() {
                            return Err(CmdParseError {});
                        } else {
                            error!("invalid cluster config from UMCTL SETDB but the local and peer metadata are complete. Ignore this error to protect the core functionality.");
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
pub struct ProxyDBMap {
    db_map: HashMap<DBName, HashMap<String, Vec<SlotRange>>>,
}

impl ProxyDBMap {
    pub fn new(db_map: HashMap<DBName, HashMap<String, Vec<SlotRange>>>) -> Self {
        Self { db_map }
    }

    pub fn get_map(&self) -> &HashMap<DBName, HashMap<String, Vec<SlotRange>>> {
        &self.db_map
    }

    pub fn db_map_to_args(&self) -> Vec<String> {
        let mut args = vec![];
        for (db_name, node_map) in &self.db_map {
            for (node, slot_ranges) in node_map {
                for slot_range in slot_ranges {
                    args.push(db_name.to_string());
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

    fn parse_db<It>(it: &mut Peekable<It>) -> Result<(DBName, String, SlotRange), CmdParseError>
    where
        It: Iterator<Item = String>,
    {
        let dbname = try_get!(it.next());
        let dbname = DBName::from(&dbname).map_err(|_| CmdParseError {})?;
        let addr = try_get!(it.next());
        let slot_range = try_parse!(Self::parse_tagged_slot_range(it));
        Ok((dbname, addr, slot_range))
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
    config_map: HashMap<DBName, ClusterConfig>,
}

impl Default for ClusterConfigMap {
    fn default() -> Self {
        Self {
            config_map: HashMap::new(),
        }
    }
}

impl ClusterConfigMap {
    pub fn new(config_map: HashMap<DBName, ClusterConfig>) -> Self {
        Self { config_map }
    }

    pub fn get(&self, dbname: &DBName) -> ClusterConfig {
        self.config_map
            .get(dbname)
            .cloned()
            .unwrap_or_else(ClusterConfig::default)
    }

    pub fn get_map(&self) -> &HashMap<DBName, ClusterConfig> {
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
                return Err(CmdParseError {});
            }
        }

        Ok(Self { config_map })
    }

    fn parse_config<It>(it: &mut It) -> Result<(DBName, String, String), CmdParseError>
    where
        It: Iterator<Item = String>,
    {
        let dbname = try_get!(it.next());
        let dbname = DBName::from(&dbname).map_err(|_| CmdParseError {})?;
        let field = try_get!(it.next());
        let value = try_get!(it.next());
        Ok((dbname, field, value))
    }

    pub fn to_args(&self) -> Vec<String> {
        let mut args = vec![];
        for (db_name, config) in &self.config_map {
            for (k, v) in config.to_str_map().into_iter() {
                args.push(db_name.to_string());
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
    fn test_single_db() {
        let args = vec!["dbname", "127.0.0.1:6379", "1", "0-1000"];
        let mut arguments = args.iter().map(|s| s.to_string()).peekable();
        let r = ProxyDBMap::parse(&mut arguments);
        assert!(r.is_ok());
        let host_db_map = r.expect("test_single_db");
        assert_eq!(host_db_map.db_map.len(), 1);

        assert_eq!(host_db_map.db_map_to_args(), args);
    }

    #[test]
    fn test_multiple_slots() {
        let args = vec![
            "dbname",
            "127.0.0.1:6379",
            "1",
            "0-1000",
            "dbname",
            "127.0.0.1:6379",
            "1",
            "1001-2000",
        ];
        let mut arguments = args.iter().map(|s| s.to_string()).peekable();

        let r = ProxyDBMap::parse(&mut arguments);
        assert!(r.is_ok());
        let host_db_map = r.expect("test_multiple_slots");
        assert_eq!(host_db_map.db_map.len(), 1);
        let db_name = DBName::from("dbname").unwrap();
        assert_eq!(
            host_db_map
                .db_map
                .get(&db_name)
                .expect("test_multiple_slots")
                .len(),
            1
        );
        assert_eq!(
            host_db_map
                .db_map
                .get(&db_name)
                .expect("test_multiple_slots")
                .get("127.0.0.1:6379")
                .expect("test_multiple_slots")
                .len(),
            2
        );

        assert_eq!(host_db_map.db_map_to_args(), args);
    }

    #[test]
    fn test_multiple_nodes() {
        let args = vec![
            "dbname",
            "127.0.0.1:7000",
            "1",
            "0-1000",
            "dbname",
            "127.0.0.1:7001",
            "1",
            "1001-2000",
        ];
        let mut arguments = args.iter().map(|s| s.to_string()).peekable();
        let host_db_map = ProxyDBMap::parse(&mut arguments).expect("test_multiple_nodes");
        assert_eq!(host_db_map.db_map.len(), 1);
        let db_name = DBName::from("dbname").unwrap();
        assert_eq!(
            host_db_map
                .db_map
                .get(&db_name)
                .expect("test_multiple_nodes")
                .len(),
            2
        );
        assert_eq!(
            host_db_map
                .db_map
                .get(&db_name)
                .expect("test_multiple_nodes")
                .get("127.0.0.1:7000")
                .expect("test_multiple_nodes")
                .len(),
            1
        );
        assert_eq!(
            host_db_map
                .db_map
                .get(&db_name)
                .expect("test_multiple_nodes")
                .get("127.0.0.1:7001")
                .expect("test_multiple_nodes")
                .len(),
            1
        );

        let mut expected_args = args.clone();
        let mut actual_args = host_db_map.db_map_to_args();
        expected_args.sort();
        actual_args.sort();
        assert_eq!(actual_args, expected_args);
    }

    #[test]
    fn test_multiple_db() {
        let args = vec![
            "dbname",
            "127.0.0.1:7000",
            "1",
            "0-1000",
            "dbname",
            "127.0.0.1:7001",
            "1",
            "1001-2000",
            "another_db",
            "127.0.0.1:7002",
            "1",
            "0-2000",
        ];
        let mut arguments = args.iter().map(|s| s.to_string()).peekable();
        let r = ProxyDBMap::parse(&mut arguments);
        assert!(r.is_ok());
        let host_db_map = r.expect("test_multiple_db");
        assert_eq!(host_db_map.db_map.len(), 2);
        let db_name = DBName::from("dbname").unwrap();
        assert_eq!(
            host_db_map
                .db_map
                .get(&db_name)
                .expect("test_multiple_db")
                .len(),
            2
        );
        assert_eq!(
            host_db_map
                .db_map
                .get(&db_name)
                .expect("test_multiple_db")
                .get("127.0.0.1:7000")
                .expect("test_multiple_db")
                .len(),
            1
        );
        assert_eq!(
            host_db_map
                .db_map
                .get(&db_name)
                .expect("test_multiple_db")
                .get("127.0.0.1:7001")
                .expect("test_multiple_db")
                .len(),
            1
        );
        let another_db = DBName::from("another_db").unwrap();
        assert_eq!(
            host_db_map
                .db_map
                .get(&another_db)
                .expect("test_multiple_nodes")
                .len(),
            1
        );
        assert_eq!(
            host_db_map
                .db_map
                .get(&another_db)
                .expect("test_multiple_db")
                .get("127.0.0.1:7002")
                .expect("test_multiple_db")
                .len(),
            1
        );

        let mut expected_args = args.clone();
        let mut actual_args = host_db_map.db_map_to_args();
        expected_args.sort();
        actual_args.sort();
        assert_eq!(actual_args, expected_args);
    }

    #[test]
    fn test_clusters_config() {
        let args = vec![
            "mydb",
            "compression_strategy",
            "allow_all",
            "otherdb",
            "migration_delete_count",
            "233",
            "mydb",
            "migration_max_migration_time",
            "666",
        ];
        let mut it = args.iter().map(|s| s.to_string()).peekable();
        let clusters_config = ClusterConfigMap::parse(&mut it).expect("test_clusters_config");
        assert_eq!(clusters_config.config_map.len(), 2);
        let mydb = DBName::from("mydb").unwrap();
        assert_eq!(
            clusters_config
                .config_map
                .get(&mydb)
                .expect("test_clusters_config")
                .compression_strategy,
            CompressionStrategy::AllowAll
        );
        assert_eq!(
            clusters_config
                .config_map
                .get(&mydb)
                .expect("test_clusters_config")
                .migration_config
                .max_migration_time,
            666
        );
        assert_eq!(
            clusters_config
                .config_map
                .get(&DBName::from("otherdb").unwrap())
                .expect("test_clusters_config")
                .migration_config
                .delete_count,
            233
        );

        let mut result_args = clusters_config.to_args();

        let mut full_args = vec![
            "mydb",
            "compression_strategy",
            "allow_all",
            "mydb",
            "migration_delete_count",
            "16",
            "mydb",
            "migration_max_migration_time",
            "666",
            "mydb",
            "migration_max_blocking_time",
            "10000",
            "mydb",
            "migration_delete_interval",
            "500",
            "mydb",
            "migration_scan_interval",
            "500",
            "mydb",
            "migration_scan_count",
            "16",
            "otherdb",
            "compression_strategy",
            "disabled",
            "otherdb",
            "migration_delete_count",
            "233",
            "otherdb",
            "migration_max_migration_time",
            "10800",
            "otherdb",
            "migration_max_blocking_time",
            "10000",
            "otherdb",
            "migration_delete_interval",
            "500",
            "otherdb",
            "migration_scan_interval",
            "500",
            "otherdb",
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
            "dbname",
            "127.0.0.1:7000",
            "1",
            "0-1000",
            "dbname",
            "127.0.0.1:7001",
            "IMPORTING",
            "1",
            "1001-2000",
            "233",
            "127.0.0.2:7001",
            "127.0.0.2:6001",
            "127.0.0.1:7001",
            "127.0.0.1:6002",
            "another_db",
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
        let r = ProxyDBMap::parse(&mut it);
        let host_db_map = r.expect("test_to_map");

        let db_map = ProxyDBMap::new(host_db_map.db_map);
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
            "1",
            "0-1000",
            "dbname",
            "127.0.0.1:7001",
            "1",
            "1001-2000",
            "PEER",
            "dbname",
            "127.0.0.2:7001",
            "1",
            "2001-3000",
            "dbname",
            "127.0.0.2:7002",
            "1",
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

        let (db_meta, extended_res) =
            ProxyDBMeta::parse(&mut it).expect("test_parse_proxy_db_meta");
        assert!(extended_res.is_ok());
        assert_eq!(db_meta.epoch, 233);
        assert!(db_meta.flags.force);
        let local = db_meta.local.get_map();
        let peer = db_meta.peer.get_map();
        let config = db_meta.clusters_config.get_map();
        assert_eq!(local.len(), 1);
        let db_name = DBName::from("dbname").unwrap();
        assert_eq!(
            local.get(&db_name).expect("test_parse_proxy_db_meta").len(),
            2
        );
        assert_eq!(
            local
                .get(&db_name)
                .expect("test_parse_proxy_db_meta")
                .get("127.0.0.1:7000")
                .expect("test_parse_proxy_db_meta")[0]
                .get_range_list()
                .get_ranges()[0]
                .start(),
            0
        );
        assert_eq!(
            local
                .get(&db_name)
                .expect("test_parse_proxy_db_meta")
                .get("127.0.0.1:7001")
                .expect("test_parse_proxy_db_meta")[0]
                .get_range_list()
                .get_ranges()[0]
                .start(),
            1001
        );
        assert_eq!(peer.len(), 1);
        assert_eq!(
            peer.get(&db_name).expect("test_parse_proxy_db_meta").len(),
            2
        );
        assert_eq!(
            peer.get(&db_name)
                .expect("test_parse_proxy_db_meta")
                .get("127.0.0.2:7001")
                .expect("test_parse_proxy_db_meta")[0]
                .get_range_list()
                .get_ranges()[0]
                .start(),
            2001
        );
        assert_eq!(
            peer.get(&db_name)
                .expect("test_parse_proxy_db_meta")
                .get("127.0.0.2:7002")
                .expect("test_parse_proxy_db_meta")[0]
                .get_range_list()
                .get_ranges()[0]
                .start(),
            3001
        );
        assert_eq!(config.len(), 1);
        assert_eq!(
            config
                .get(&db_name)
                .expect("test_parse_proxy_db_meta")
                .compression_strategy,
            CompressionStrategy::SetGetOnly
        );

        let mut args = db_meta.to_args();
        let mut db_args: Vec<String> = arguments.into_iter().map(|s| s.to_string()).collect();
        let extended = vec![
            "dbname",
            "migration_delete_count",
            "16",
            "dbname",
            "migration_max_migration_time",
            "10800",
            "dbname",
            "migration_max_blocking_time",
            "10000",
            "dbname",
            "migration_delete_interval",
            "500",
            "dbname",
            "migration_scan_interval",
            "500",
            "dbname",
            "migration_scan_count",
            "16",
        ]
        .into_iter()
        .map(|s| s.to_string());
        db_args.extend(extended);
        args.sort();
        db_args.sort();
        assert_eq!(args, db_args);
    }

    #[test]
    fn test_parse_proxy_db_meta_without_peer() {
        let arguments = vec![
            "233",
            "FORCE",
            "dbname",
            "127.0.0.1:7000",
            "1",
            "0-1000",
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

        let (db_meta, extended_res) =
            ProxyDBMeta::parse(&mut it).expect("test_parse_proxy_db_meta_without_peer");
        assert!(extended_res.is_ok());
        assert_eq!(db_meta.epoch, 233);
        assert!(db_meta.flags.force);
        assert_eq!(
            db_meta
                .get_configs()
                .get(&DBName::from("dbname").unwrap())
                .compression_strategy,
            CompressionStrategy::SetGetOnly
        );
    }

    #[test]
    fn test_missing_config_db() {
        let arguments = vec![
            "233",
            "FORCE",
            "dbname",
            "127.0.0.1:7000",
            "1",
            "0-1000",
            "PEER",
            "dbname",
            "127.0.0.2:7001",
            "1",
            "2001-3000",
            "CONFIG",
            // "dbname", missing dbname
            "compression_strategy",
            "set_get_only",
        ];
        let mut it = arguments
            .clone()
            .into_iter()
            .map(|s| s.to_string())
            .peekable();

        let (db_meta, extended_res) = ProxyDBMeta::parse(&mut it).expect("test_missing_config_db");
        assert!(extended_res.is_err());
        assert_eq!(db_meta.epoch, 233);
        assert!(db_meta.flags.force);
    }

    #[test]
    fn test_invalid_config_field() {
        let arguments = vec![
            "233",
            "FORCE",
            "dbname",
            "127.0.0.1:7000",
            "1",
            "0-1000",
            "PEER",
            "dbname",
            "127.0.0.2:7001",
            "1",
            "2001-3000",
            "CONFIG",
            "dbname",
            "config_field_that_does_not_exist",
            "invalid_value",
        ];
        let mut it = arguments
            .clone()
            .into_iter()
            .map(|s| s.to_string())
            .peekable();

        let (db_meta, extended_res) =
            ProxyDBMeta::parse(&mut it).expect("test_invalid_config_field");
        assert!(extended_res.is_err());
        assert_eq!(db_meta.epoch, 233);
        assert!(db_meta.flags.force);
    }

    #[test]
    fn test_incomplete_main_meta_with_config_err() {
        let arguments = vec![
            "233",
            "FORCE",
            "dbname",
            "127.0.0.1:7000",
            "1",
            "0-1000",
            "CONFIG",
            "dbname",
            "config_field_that_does_not_exist",
            "invalid_value",
        ];
        let mut it = arguments
            .clone()
            .into_iter()
            .map(|s| s.to_string())
            .peekable();

        assert!(ProxyDBMeta::parse(&mut it).is_err());
    }
}
