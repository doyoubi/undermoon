use std::sync;
use std::iter::Peekable;
use std::collections::HashMap;
use std::iter::Iterator;
use std::error::Error;
use std::fmt;
use caseless;
use protocol::{Resp, Array, BulkStr};
use ::common::db::HostDBMap;
use ::common::cluster::{SlotRange, SlotRangeTag};
use super::backend::CmdTask;
use super::backend::{BackendError, CmdTaskSender};
use super::slot::SlotMap;
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
        if self.local_dbs.read().unwrap().0 >= db_map.get_epoch() {
            return Err(DBError::OldEpoch)
        }

        let epoch = db_map.get_epoch();
        let mut map = HashMap::new();
        for (db_name, slot_ranges) in db_map.into_map() {
            let db = Database::from_slot_map(db_name.clone(), epoch, slot_ranges);
            map.insert(db_name, db);
        }

        let mut local = self.local_dbs.write().unwrap();
        if epoch <= local.0 {
            return Err(DBError::OldEpoch)
        }
        *local = (epoch, map);
        Ok(())
    }

    pub fn set_peers(&self, db_map: HostDBMap) -> Result<(), DBError> {
        if self.remote_dbs.read().unwrap().0 >= db_map.get_epoch() {
            return Err(DBError::OldEpoch)
        }

        let epoch = db_map.get_epoch();
        let mut map = HashMap::new();
        for (db_name, slot_ranges) in db_map.into_map() {
            let remote_db = RemoteDB::from_slot_map(db_name.clone(), epoch, slot_ranges);
            map.insert(db_name, remote_db);
        }

        let mut remote = self.remote_dbs.write().unwrap();
        if epoch <= remote.0 {
            return Err(DBError::OldEpoch)
        }
        *remote = (epoch, map);
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
