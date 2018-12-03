use std::sync;
use std::str;
use std::collections::HashMap;
use std::iter::Iterator;
use caseless;
use protocol::{Resp, Array, BulkStr};
use super::backend::CmdTask;
use super::backend::{BackendError, CmdTaskSender};
use super::slot::{SlotMap, SLOT_NUM, SlotRange, SlotRangeTag};

pub const DEFAULT_DB : &'static str = "default_db";

pub trait DBTag {
    fn get_db_name(&self) -> String;
    fn set_db_name(&self, db: String);
}

pub struct DatabaseMap<S: CmdTaskSender> where S::Task: DBTag {
    // (epoch, meta data)
    local_dbs: sync::RwLock<(u64, HashMap<String, Database<S>>)>,
}

impl<S: CmdTaskSender> DatabaseMap<S> where S::Task: DBTag {
    pub fn new() -> DatabaseMap<S> {
        let default_db = Database::new(DEFAULT_DB.to_string());
        let mut db_map = HashMap::new();
        db_map.insert(DEFAULT_DB.to_string(), default_db);
        Self{
            local_dbs: sync::RwLock::new((0, db_map)),
        }
    }

    pub fn send(&self, cmd_task: S::Task) -> Result<(), BackendError> {
        let db_name = cmd_task.get_db_name();
        match self.local_dbs.read().unwrap().1.get(&db_name) {
            Some(db) => {
                db.send(cmd_task)
            },
            None => {
                cmd_task.set_result(Ok(Resp::Error(format!("db not found: {}", db_name).into_bytes())));
                Ok(())
            },
        }
    }

    pub fn get_dbs(&self) -> Vec<String> {
        self.local_dbs.read().unwrap().1.keys().map(|s| s.clone()).collect()
    }

    pub fn clear(&self) {
        self.local_dbs.write().unwrap().1.clear()
    }

    pub fn set_dbs(&self, db_map: HostDBMap) {
        let mut map = HashMap::new();
        for (db_name, slot_ranges) in db_map.db_map {
            let db = Database::from_slot_map(db_name.clone(), slot_ranges);
            map.insert(db_name, db);
        }

        let mut local = self.local_dbs.write().unwrap();
        if db_map.epoch == local.0 {
            return
        }
        *local = (db_map.epoch, map);
    }
}

pub struct Database<S: CmdTaskSender> {
    name: String,
    // We can improve this by using some concurrent map implementation.
    nodes: sync::RwLock<HashMap<String, S>>,
    slot_map: SlotMap,
}

impl<S: CmdTaskSender> Database<S> {
    pub fn new(name: String) -> Database<S> {
        // TODO: remove this default database config later
        let mut slot_map = HashMap::new();
        let mut slots = Vec::new();
        for s in 0..SLOT_NUM {
            slots.push(s);
        }
        let addr = "127.0.0.1:6379";
        slot_map.insert(addr.to_string(), slots);
        let mut nodes = HashMap::new();
        nodes.insert(addr.to_string(), S::new(addr.to_string()));
        Database{
            name: name,
            nodes: sync::RwLock::new(nodes),
            slot_map: SlotMap::new(slot_map),
        }
    }

    pub fn from_slot_map(name: String, slot_map: HashMap<String, Vec<SlotRange>>) -> Database<S> {
        let mut nodes = HashMap::new();
        for addr in slot_map.keys() {
            nodes.insert(addr.to_string(), S::new(addr.to_string()));
        }
        Database{
            name: name,
            nodes: sync::RwLock::new(nodes),
            slot_map: SlotMap::from_ranges(slot_map),
        }
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn update(&self, slot_map: HashMap<String, Vec<usize>>) {
        println!("{} is updating", self.get_name());
        self.slot_map.update(slot_map)
    }

    // TODO: use other error type
    pub fn send(&self, cmd_task: S::Task) -> Result<(), BackendError> {
        {
            // TODO: get the key
            let _resp: &Resp = cmd_task.get_resp();
        }
        match self.slot_map.get_by_key("dummy_key".as_bytes()) {
            Some(addr) => {
                let guard = self.nodes.read().unwrap();
                match guard.get(&addr) {
                    Some(sender) => {
                        sender.send(cmd_task)
                    },
                    None => {
                        println!("Failed to get node");
                        Err(BackendError::Canceled)
                    },
                }
            }
            None => {
                println!("Failed to get slot");
                Err(BackendError::Canceled)
            }
        }
    }
}

const MIGRATING_TAG: &'static str = "MIGRATING";

pub struct HostDBMap {
    epoch: u64,
    db_map: HashMap<String, HashMap<String, Vec<SlotRange>>>,
}

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

        let mut db_map = HashMap::new();
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

        let epoch_str = try_get!(it.next());
        let epoch = try_parse!(epoch_str.parse::<u64>());

        let _flags = try_get!(it.next());

        while let Some(_) = it.peek() {
            let (dbname, address, slot_range) = try_parse!(Self::parse_db(&mut it));
            let db = db_map.entry(dbname).or_insert(HashMap::new());
            let slots = db.entry(address).or_insert(vec![]);
            slots.push(slot_range);
        }

        Ok(Self{
            epoch: epoch,
            db_map: db_map,
        })
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
