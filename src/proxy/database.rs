use std::sync;
use std::str;
use std::iter::Peekable;
use std::collections::HashMap;
use std::iter::Iterator;
use std::error::Error;
use std::fmt;
use caseless;
use protocol::{Resp, Array, BulkStr};
use super::backend::CmdTask;
use super::backend::{BackendError, CmdTaskSender};
use super::slot::{SlotMap, SlotRange, SlotRangeTag};
use super::command::get_key;

pub const DEFAULT_DB: &'static str = "admin";

fn gen_moved(slot: usize, addr: String) -> String {
    format!("MOVED {} {}", slot, addr)
}

#[derive(Debug)]
pub enum DBError {
    OldEpoch,
}

impl fmt::Display for DBError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for DBError {
    fn description(&self) -> &str {
        "db error"
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}

pub trait DBTag {
    fn get_db_name(&self) -> String;
    fn set_db_name(&self, db: String);
}

pub struct DatabaseMap<S: CmdTaskSender> where S::Task: DBTag {
    // (epoch, meta data)
    local_dbs: sync::RwLock<(u64, HashMap<String, Database<S>>)>,
    remote_dbs: sync::RwLock<(u64, HashMap<String, RemoteDB>)>,
}

impl<S: CmdTaskSender> DatabaseMap<S> where S::Task: DBTag {
    pub fn new() -> DatabaseMap<S> {
        Self{
            local_dbs: sync::RwLock::new((0, HashMap::new())),
            remote_dbs: sync::RwLock::new((0, HashMap::new())),
        }
    }

    pub fn send(&self, cmd_task: S::Task) -> Result<(), DBSendError<S::Task>> {
        let db_name = cmd_task.get_db_name();
        let (cmd_task, db_exists) = match self.local_dbs.read().unwrap().1.get(&db_name) {
            Some(db) => {
                match db.send(cmd_task) {
                    Err(DBSendError::SlotNotFound(cmd_task)) => (cmd_task, true),
                    others => return others,
                }
            },
            None => (cmd_task, false),
        };

        match self.remote_dbs.read().unwrap().1.get(&db_name) {
            Some(remote_db) => remote_db.send_remote(cmd_task),
            None => {
                if db_exists {
                    cmd_task.set_result(Ok(Resp::Error(format!("slot not found: {}", db_name.clone()).into_bytes())));
                    Err(DBSendError::SlotNotCovered)
                } else {
                    cmd_task.set_result(Ok(Resp::Error(format!("db not found: {}", db_name.clone()).into_bytes())));
                    Err(DBSendError::DBNotFound(db_name))
                }
            }
        }
    }

    pub fn get_dbs(&self) -> Vec<String> {
        self.local_dbs.read().unwrap().1.keys().map(|s| s.clone()).collect()
    }

    pub fn clear(&self) {
        self.local_dbs.write().unwrap().1.clear()
    }

    pub fn set_dbs(&self, db_map: HostDBMap) -> Result<(), DBError> {
        if self.local_dbs.read().unwrap().0 >= db_map.epoch {
            return Err(DBError::OldEpoch)
        }

        let mut map = HashMap::new();
        for (db_name, slot_ranges) in db_map.db_map {
            let db = Database::from_slot_map(db_name.clone(), db_map.epoch, slot_ranges);
            map.insert(db_name, db);
        }

        let mut local = self.local_dbs.write().unwrap();
        if db_map.epoch <= local.0 {
            return Err(DBError::OldEpoch)
        }
        *local = (db_map.epoch, map);
        Ok(())
    }

    pub fn set_peers(&self, db_map: HostDBMap) -> Result<(), DBError> {
        if self.remote_dbs.read().unwrap().0 >= db_map.epoch {
            return Err(DBError::OldEpoch)
        }

        let mut map = HashMap::new();
        for (db_name, slot_ranges) in db_map.db_map {
            let remote_db = RemoteDB::from_slot_map(db_name.clone(), db_map.epoch, slot_ranges);
            map.insert(db_name, remote_db);
        }

        let mut remote = self.remote_dbs.write().unwrap();
        if db_map.epoch <= remote.0 {
            return Err(DBError::OldEpoch)
        }
        *remote = (db_map.epoch, map);
        Ok(())
    }

    pub fn gen_cluster_nodes(&self, dbname: String) -> String {
        let local = self.local_dbs.read().unwrap().1.get(&dbname).
            map_or("".to_string(), |db| db.gen_local_cluster_nodes());
        let remote = self.remote_dbs.read().unwrap().1.get(&dbname).
            map_or("".to_string(), |db| db.gen_remote_cluster_nodes());
        format!("{}{}", local, remote)
    }
}

struct LocalDB<S: CmdTaskSender> {
    nodes: HashMap<String, S>,
    slot_map: SlotMap,
}

pub struct Database<S: CmdTaskSender> {
    name: String,
    epoch: u64,
    local_db: LocalDB<S>,
    slot_ranges: HashMap<String, Vec<SlotRange>>,
}

impl<S: CmdTaskSender> Database<S> {
    pub fn from_slot_map(name: String, epoch: u64, slot_map: HashMap<String, Vec<SlotRange>>) -> Database<S> {
        let mut nodes = HashMap::new();
        for addr in slot_map.keys() {
            nodes.insert(addr.to_string(), S::new(addr.to_string()));
        }
        let local_db = LocalDB{
            nodes: nodes,
            slot_map: SlotMap::from_ranges(slot_map.clone()),
        };
        Database{
            name: name,
            epoch: epoch,
            local_db: local_db,
            slot_ranges: slot_map,
        }
    }

    pub fn send(&self, cmd_task: S::Task) -> Result<(), DBSendError<S::Task>> {
        let key = match get_key(cmd_task.get_resp()) {
            Some(key) => key,
            None => {
                cmd_task.set_result(Ok(Resp::Error("missing key".to_string().into_bytes())));
                return Err(DBSendError::MissingKey)
            },
        };

        match self.local_db.slot_map.get_by_key(&key) {
            Some(addr) => {
                match self.local_db.nodes.get(&addr) {
                    Some(sender) => {
                        sender.send(cmd_task).map_err(|err| DBSendError::Backend(err))
                    },
                    None => {
                        println!("Failed to get node");
                        Err(DBSendError::SlotNotFound(cmd_task))
                    },
                }
            }
            None => {
                println!("Failed to get slot");
                Err(DBSendError::SlotNotFound(cmd_task))
            }
        }
    }

    pub fn gen_local_cluster_nodes(&self) -> String {
        gen_cluster_nodes_helper(&self.name, self.epoch, &self.slot_ranges)
    }
}

pub struct RemoteDB {
    name: String,
    epoch: u64,
    slot_map: SlotMap,
    slot_ranges: HashMap<String, Vec<SlotRange>>,
}

impl RemoteDB {
    pub fn from_slot_map(name: String, epoch: u64, slot_map: HashMap<String, Vec<SlotRange>>) -> RemoteDB {
        RemoteDB{
            name: name,
            epoch: epoch,
            slot_map: SlotMap::from_ranges(slot_map.clone()),
            slot_ranges: slot_map,
        }
    }

    pub fn send_remote<T: CmdTask>(&self, cmd_task: T) -> Result<(), DBSendError<T>> {
        let key = match get_key(cmd_task.get_resp()) {
            Some(key) => key,
            None => {
                cmd_task.set_result(Ok(Resp::Error("missing key".to_string().into_bytes())));
                return Err(DBSendError::MissingKey)
            },
        };
        match self.slot_map.get_by_key(&key) {
            Some(addr) => {
                cmd_task.set_result(Ok(Resp::Error(gen_moved(self.slot_map.get_slot(&key), addr).into_bytes())));
                Ok(())
            }
            None => {
                cmd_task.set_result(Ok(Resp::Error(format!("slot not covered {:?}", key).into_bytes())));
                Err(DBSendError::SlotNotCovered)
            }
        }
    }

    pub fn gen_remote_cluster_nodes(&self) -> String {
        gen_cluster_nodes_helper(&self.name, self.epoch, &self.slot_ranges)
    }
}

#[derive(Debug)]
pub enum DBSendError<T: CmdTask> {
    MissingKey,
    DBNotFound(String),
    SlotNotFound(T),
    SlotNotCovered,
    Backend(BackendError),
}

impl<T: CmdTask> fmt::Display for DBSendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<T: CmdTask> Error for DBSendError<T> {
    fn description(&self) -> &str {
        match self {
            DBSendError::MissingKey => "missing key",
            DBSendError::DBNotFound(_) => "db not found",
            DBSendError::SlotNotFound(_) => "slot not found",
            DBSendError::Backend(_) => "backend error",
            DBSendError::SlotNotCovered => "slot not covered",
        }
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            DBSendError::MissingKey => None,
            DBSendError::DBNotFound(_) => None,
            DBSendError::SlotNotFound(_) => None,
            DBSendError::Backend(err) => Some(err),
            DBSendError::SlotNotCovered => None,
        }
    }
}


const MIGRATING_TAG: &'static str = "MIGRATING";

pub struct HostDBMap {
    epoch: u64,
    db_map: HashMap<String, HashMap<String, Vec<SlotRange>>>,
}

#[derive(Debug)]
pub struct NMCtlParseError {}

macro_rules! try_parse {
    ($expression:expr) => ({
        match $expression {
            Ok(v) => (v),
            Err(_) => return Err(NMCtlParseError{}),
        }
    })
}

macro_rules! try_get {
    ($expression:expr) => ({
        match $expression {
            Some(v) => (v),
            None => return Err(NMCtlParseError{}),
        }
    })
}

impl HostDBMap {
    pub fn from_resp(resp: &Resp) -> Result<Self, NMCtlParseError> {
        let arr = match resp {
            Resp::Arr(Array::Arr(ref arr)) => {
                arr
            }
            _ => return Err(NMCtlParseError{}),
        };

        let it = arr.iter().skip(2).flat_map(|resp| {
            match resp {
                Resp::Bulk(BulkStr::Str(safe_str)) => {
                    match str::from_utf8(safe_str) {
                        Ok(s) => Some(s.to_string()),
                        _ => return None,
                    }
                },
                _ => None,
            }
        });
        let mut it = it.peekable();

        let (epoch, db_map) = try_parse!(Self::parse(&mut it));

        Ok(Self{
            epoch: epoch,
            db_map: db_map,
        })
    }

    fn parse<It>(it: &mut Peekable<It>) -> Result<(u64, HashMap<String, HashMap<String, Vec<SlotRange>>>), NMCtlParseError>
            where It: Iterator<Item=String> {
        let epoch_str = try_get!(it.next());
        let epoch = try_parse!(epoch_str.parse::<u64>());

        let _flags = try_get!(it.next());

        let mut db_map = HashMap::new();

        while let Some(_) = it.peek() {
            let (dbname, address, slot_range) = try_parse!(Self::parse_db(it));
            let db = db_map.entry(dbname).or_insert(HashMap::new());
            let slots = db.entry(address).or_insert(vec![]);
            slots.push(slot_range);
        }

        return Ok((epoch, db_map))
    }

    fn parse_db<It>(it: &mut It) -> Result<(String, String, SlotRange), NMCtlParseError>
            where It: Iterator<Item=String> {
        let dbname = try_get!(it.next());
        let addr = try_get!(it.next());
        let slot_range = try_parse!(Self::parse_tagged_slot_range(it));
        Ok((dbname, addr, slot_range))
    }

    fn parse_tagged_slot_range<It>(it: &mut It) -> Result<SlotRange, NMCtlParseError> where It: Iterator<Item=String> {
        let slot_range = try_get!(it.next());
        if !caseless::canonical_caseless_match_str(&slot_range, MIGRATING_TAG) {
            return Self::parse_slot_range(slot_range);
        }

        let dst = try_get!(it.next());
        let mut slot_range = try_parse!(Self::parse_slot_range(try_get!(it.next())));
        slot_range.tag = SlotRangeTag::Migrating(dst);
        Ok(slot_range)
    }

    fn parse_slot_range(s: String) -> Result<SlotRange, NMCtlParseError> {
        let mut slot_range = s.split('-');
        let start_str = try_get!(slot_range.next());
        let end_str = try_get!(slot_range.next());
        let start = try_parse!(start_str.parse::<usize>());
        let end = try_parse!(end_str.parse::<usize>());
        Ok(SlotRange{
            start: start,
            end: end,
            tag: SlotRangeTag::None,
        })
    }
}

fn gen_cluster_nodes_helper(name: &String, epoch: u64, slot_ranges: &HashMap<String, Vec<SlotRange>>) -> String {
    let mut cluster_nodes = String::from("");
    let mut name_seg = format!("{:_<20}", name);
    name_seg.truncate(20);
    for (addr, ranges) in slot_ranges {
        let mut addr_seg = format!("{:_<20}", addr);
        let id = format!("{}{}", name_seg, addr_seg);
        addr_seg.truncate(20);

        let slot_range = ranges.iter().
            map(|range| format!("{}-{}", range.start, range.end)).
            collect::<Vec<String>>().join(" ");

        let line = format!(
            "{id} {addr} {flags} {master} {ping_sent} {pong_recv} {epoch} {link_state} {slot_range}\n",
            id=id, addr=addr, flags="master", master="-", ping_sent=0, pong_recv=0, epoch=epoch,
            link_state="connected", slot_range=slot_range,
        );
        cluster_nodes.extend(line.chars());
    }
    cluster_nodes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_db() {
        let mut arguments = vec![
            "233", "noflag", "dbname", "127.0.0.1:6379", "0-1000",
        ].into_iter().map(|s| s.to_string()).peekable();
        let r = HostDBMap::parse(&mut arguments);
        assert!(r.is_ok());
        let (epoch, hash) = r.unwrap();
        assert_eq!(epoch, 233);
        assert_eq!(hash.len(), 1);
    }

    #[test]
    fn test_multiple_slots() {
        let mut arguments = vec![
            "233", "noflag",
            "dbname", "127.0.0.1:6379", "0-1000",
            "dbname", "127.0.0.1:6379", "1001-2000",
        ].into_iter().map(|s| s.to_string()).peekable();
        let r = HostDBMap::parse(&mut arguments);
        assert!(r.is_ok());
        let (epoch, hash) = r.unwrap();
        assert_eq!(epoch, 233);
        assert_eq!(hash.len(), 1);
        assert_eq!(hash.get("dbname").unwrap().len(), 1);
        assert_eq!(hash.get("dbname").unwrap().get("127.0.0.1:6379").unwrap().len(), 2);
    }

    #[test]
    fn test_multiple_nodes() {
        let mut arguments = vec![
            "233", "noflag",
            "dbname", "127.0.0.1:7000", "0-1000",
            "dbname", "127.0.0.1:7001", "1001-2000",
        ].into_iter().map(|s| s.to_string()).peekable();
        let r = HostDBMap::parse(&mut arguments);
        assert!(r.is_ok());
        let (epoch, hash) = r.unwrap();
        assert_eq!(epoch, 233);
        assert_eq!(hash.len(), 1);
        assert_eq!(hash.get("dbname").unwrap().len(), 2);
        assert_eq!(hash.get("dbname").unwrap().get("127.0.0.1:7000").unwrap().len(), 1);
        assert_eq!(hash.get("dbname").unwrap().get("127.0.0.1:7001").unwrap().len(), 1);
    }

    #[test]
    fn test_multiple_db() {
        let mut arguments = vec![
            "233", "noflag",
            "dbname", "127.0.0.1:7000", "0-1000",
            "dbname", "127.0.0.1:7001", "1001-2000",
            "another_db", "127.0.0.1:7002", "0-2000",
        ].into_iter().map(|s| s.to_string()).peekable();
        let r = HostDBMap::parse(&mut arguments);
        assert!(r.is_ok());
        let (epoch, hash) = r.unwrap();
        assert_eq!(epoch, 233);
        assert_eq!(hash.len(), 2);
        assert_eq!(hash.get("dbname").unwrap().len(), 2);
        assert_eq!(hash.get("dbname").unwrap().get("127.0.0.1:7000").unwrap().len(), 1);
        assert_eq!(hash.get("dbname").unwrap().get("127.0.0.1:7001").unwrap().len(), 1);
        assert_eq!(hash.get("another_db").unwrap().len(), 1);
        assert_eq!(hash.get("another_db").unwrap().get("127.0.0.1:7002").unwrap().len(), 1);
    }
}
