use super::cluster::{SlotRange, SlotRangeTag};
use super::utils::{has_flags, CmdParseError};
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
const REPL_PREFIX: &str = "REPL";

#[derive(Debug, Clone)]
pub struct ProxyDBMeta {
    epoch: u64,
    flags: DBMapFlags,
    local: HostDBMap,
    peer: HostDBMap,
}

impl ProxyDBMeta {
    pub fn new(epoch: u64, flags: DBMapFlags, local: HostDBMap, peer: HostDBMap) -> Self {
        Self {
            epoch,
            flags,
            local,
            peer,
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

    pub fn from_resp(resp: &Resp) -> Result<Self, CmdParseError> {
        let arr = match resp {
            Resp::Arr(Array::Arr(ref arr)) => arr,
            _ => return Err(CmdParseError {}),
        };

        // Skip the "UMCTL SET_DB"
        let it = arr.iter().skip(2).flat_map(|resp| match resp {
            Resp::Bulk(BulkStr::Str(safe_str)) => match str::from_utf8(safe_str) {
                Ok(s) => Some(s.to_string()),
                _ => None,
            },
            _ => None,
        });
        let mut it = it.peekable();

        let epoch_str = try_get!(it.next());
        let epoch = try_parse!(epoch_str.parse::<u64>());

        let flags = DBMapFlags::from_arg(&try_get!(it.next()));

        // TODO: not that easy, add prefix checking
        let local = HostDBMap::parse(&mut it)?;
        let peer = HostDBMap::parse(&mut it)?;

        Ok(Self {
            epoch,
            flags,
            local,
            peer,
        })
    }

    pub fn to_args(&self) -> Vec<String> {
        let mut args = vec![self.epoch.to_string(), self.flags.to_arg()];
        let local = self.local.db_map_to_args();
        let peer = self.peer.db_map_to_args();
        args.extend_from_slice(&local);
        args.push(PEER_PREFIX.to_string());
        args.extend_from_slice(&peer);
        args.push(REPL_PREFIX.to_string());
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
                    if prefix == PEER_PREFIX || prefix == REPL_PREFIX {
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

#[cfg(test)]
mod tests {
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
}
