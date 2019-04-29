use super::backend::CmdTask;
use super::backend::{BackendError, CmdTaskSender, CmdTaskSenderFactory};
use super::slot::SlotMap;
use common::cluster::SlotRange;
use common::db::HostDBMap;
use common::utils::get_key;
use protocol::{Array, BulkStr, Resp};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::iter::Iterator;
use std::sync;

pub const DEFAULT_DB: &str = "admin";

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

pub struct DatabaseMap<F: CmdTaskSenderFactory>
where
    <<F as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task: DBTag,
{
    // (epoch, meta data)
    local_dbs: sync::RwLock<(u64, HashMap<String, Database<F>>)>,
    remote_dbs: sync::RwLock<(u64, HashMap<String, RemoteDB>)>,
    sender_factory: F,
}

impl<F: CmdTaskSenderFactory> DatabaseMap<F>
where
    <<F as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task: DBTag,
{
    pub fn new(sender_factory: F) -> DatabaseMap<F> {
        Self {
            local_dbs: sync::RwLock::new((0, HashMap::new())),
            remote_dbs: sync::RwLock::new((0, HashMap::new())),
            sender_factory,
        }
    }

    pub fn send(
        &self,
        cmd_task: <<F as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task,
    ) -> Result<(), DBSendError<<<F as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>> {
        let db_name = cmd_task.get_db_name();
        let (cmd_task, db_exists) = match self.local_dbs.read().unwrap().1.get(&db_name) {
            Some(db) => match db.send(cmd_task) {
                Err(DBSendError::SlotNotFound(cmd_task)) => (cmd_task, true),
                others => return others,
            },
            None => (cmd_task, false),
        };

        match self.remote_dbs.read().unwrap().1.get(&db_name) {
            Some(remote_db) => remote_db.send_remote(cmd_task),
            None => {
                if db_exists {
                    let resp =
                        Resp::Error(format!("slot not found: {}", db_name.clone()).into_bytes());
                    cmd_task.set_resp_result(Ok(resp));
                    Err(DBSendError::SlotNotCovered)
                } else {
                    let resp =
                        Resp::Error(format!("db not found: {}", db_name.clone()).into_bytes());
                    cmd_task.set_resp_result(Ok(resp));
                    Err(DBSendError::DBNotFound(db_name))
                }
            }
        }
    }

    pub fn get_dbs(&self) -> Vec<String> {
        self.local_dbs.read().unwrap().1.keys().cloned().collect()
    }

    pub fn clear(&self) {
        self.local_dbs.write().unwrap().1.clear()
    }

    pub fn set_dbs(&self, db_map: HostDBMap) -> Result<(), DBError> {
        let force = db_map.get_flags().force;

        if !force && self.local_dbs.read().unwrap().0 >= db_map.get_epoch() {
            return Err(DBError::OldEpoch);
        }

        let epoch = db_map.get_epoch();
        let mut map = HashMap::new();
        for (db_name, slot_ranges) in db_map.into_map() {
            let db =
                Database::from_slot_map(&self.sender_factory, db_name.clone(), epoch, slot_ranges);
            map.insert(db_name, db);
        }

        let mut local = self.local_dbs.write().unwrap();
        if !force && epoch <= local.0 {
            return Err(DBError::OldEpoch);
        }
        *local = (epoch, map);
        Ok(())
    }

    pub fn set_peers(&self, db_map: HostDBMap) -> Result<(), DBError> {
        let force = db_map.get_flags().force;

        if !force && self.remote_dbs.read().unwrap().0 >= db_map.get_epoch() {
            return Err(DBError::OldEpoch);
        }

        let epoch = db_map.get_epoch();
        let mut map = HashMap::new();
        for (db_name, slot_ranges) in db_map.into_map() {
            let remote_db = RemoteDB::from_slot_map(db_name.clone(), epoch, slot_ranges);
            map.insert(db_name, remote_db);
        }

        let mut remote = self.remote_dbs.write().unwrap();
        if !force && epoch <= remote.0 {
            return Err(DBError::OldEpoch);
        }
        *remote = (epoch, map);
        Ok(())
    }

    pub fn gen_cluster_nodes(&self, dbname: String, service_address: String) -> String {
        let local = self
            .local_dbs
            .read()
            .unwrap()
            .1
            .get(&dbname)
            .map_or("".to_string(), |db| {
                db.gen_local_cluster_nodes(service_address)
            });
        let remote = self
            .remote_dbs
            .read()
            .unwrap()
            .1
            .get(&dbname)
            .map_or("".to_string(), RemoteDB::gen_remote_cluster_nodes);
        format!("{}{}", local, remote)
    }

    pub fn gen_cluster_slots(
        &self,
        dbname: String,
        service_address: String,
    ) -> Result<Resp, String> {
        let mut local = self
            .local_dbs
            .read()
            .unwrap()
            .1
            .get(&dbname)
            .map_or(Ok(vec![]), |db| db.gen_local_cluster_slots(service_address))?;
        let mut remote = self
            .remote_dbs
            .read()
            .unwrap()
            .1
            .get(&dbname)
            .map_or(Ok(vec![]), RemoteDB::gen_remote_cluster_slots)?;
        local.append(&mut remote);
        Ok(Resp::Arr(Array::Arr(local)))
    }

    pub fn add_redirection(&self) {}
}

// We combine the nodes and slot_map to let them fit into
// the same lock with a smaller critical section
// compared to the one we need if splitting them.
struct LocalDB<S: CmdTaskSender> {
    nodes: HashMap<String, S>,
    slot_map: SlotMap,
}

pub struct Database<F: CmdTaskSenderFactory> {
    name: String,
    epoch: u64,
    local_db: LocalDB<F::Sender>,
    slot_ranges: HashMap<String, Vec<SlotRange>>,
}

impl<F: CmdTaskSenderFactory> Database<F> {
    pub fn from_slot_map(
        sender_factory: &F,
        name: String,
        epoch: u64,
        slot_map: HashMap<String, Vec<SlotRange>>,
    ) -> Database<F> {
        let mut nodes = HashMap::new();
        for addr in slot_map.keys() {
            nodes.insert(addr.to_string(), sender_factory.create(addr.to_string()));
        }
        let local_db = LocalDB {
            nodes,
            slot_map: SlotMap::from_ranges(slot_map.clone()),
        };
        Database {
            name,
            epoch,
            local_db,
            slot_ranges: slot_map,
        }
    }

    pub fn send(
        &self,
        cmd_task: <<F as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task,
    ) -> Result<(), DBSendError<<<F as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>> {
        let key = match get_key(cmd_task.get_resp()) {
            Some(key) => key,
            None => {
                let resp = Resp::Error("missing key".to_string().into_bytes());
                cmd_task.set_resp_result(Ok(resp));
                return Err(DBSendError::MissingKey);
            }
        };

        match self.local_db.slot_map.get_by_key(&key) {
            Some(addr) => match self.local_db.nodes.get(&addr) {
                Some(sender) => sender.send(cmd_task).map_err(DBSendError::Backend),
                None => {
                    error!("failed to get node");
                    Err(DBSendError::SlotNotFound(cmd_task))
                }
            },
            None => {
                error!("failed to get slot");
                Err(DBSendError::SlotNotFound(cmd_task))
            }
        }
    }

    pub fn gen_local_cluster_nodes(&self, service_address: String) -> String {
        let slots: Vec<SlotRange> = self
            .slot_ranges
            .values()
            .cloned()
            .flatten()
            .collect::<Vec<SlotRange>>();
        let mut slot_ranges = HashMap::new();
        slot_ranges.insert(service_address, slots);
        gen_cluster_nodes_helper(&self.name, self.epoch, &slot_ranges)
    }

    pub fn gen_local_cluster_slots(&self, service_address: String) -> Result<Vec<Resp>, String> {
        let slots: Vec<SlotRange> = self
            .slot_ranges
            .values()
            .cloned()
            .flatten()
            .collect::<Vec<SlotRange>>();
        let mut slot_ranges = HashMap::new();
        slot_ranges.insert(service_address, slots);
        gen_cluster_slots_helper(&slot_ranges)
    }
}

pub struct RemoteDB {
    name: String,
    epoch: u64,
    slot_map: SlotMap,
    slot_ranges: HashMap<String, Vec<SlotRange>>,
}

impl RemoteDB {
    pub fn from_slot_map(
        name: String,
        epoch: u64,
        slot_map: HashMap<String, Vec<SlotRange>>,
    ) -> RemoteDB {
        RemoteDB {
            name,
            epoch,
            slot_map: SlotMap::from_ranges(slot_map.clone()),
            slot_ranges: slot_map,
        }
    }

    pub fn send_remote<T: CmdTask>(&self, cmd_task: T) -> Result<(), DBSendError<T>> {
        let key = match get_key(cmd_task.get_resp()) {
            Some(key) => key,
            None => {
                let resp = Resp::Error("missing key".to_string().into_bytes());
                cmd_task.set_resp_result(Ok(resp));
                return Err(DBSendError::MissingKey);
            }
        };
        match self.slot_map.get_by_key(&key) {
            Some(addr) => {
                let resp = Resp::Error(gen_moved(self.slot_map.get_slot(&key), addr).into_bytes());
                cmd_task.set_resp_result(Ok(resp));
                Ok(())
            }
            None => {
                let resp = Resp::Error(format!("slot not covered {:?}", key).into_bytes());
                cmd_task.set_resp_result(Ok(resp));
                Err(DBSendError::SlotNotCovered)
            }
        }
    }

    pub fn gen_remote_cluster_nodes(&self) -> String {
        gen_cluster_nodes_helper(&self.name, self.epoch, &self.slot_ranges)
    }

    pub fn gen_remote_cluster_slots(&self) -> Result<Vec<Resp>, String> {
        gen_cluster_slots_helper(&self.slot_ranges)
    }
}

#[derive(Debug)]
pub enum DBSendError<T: CmdTask> {
    MissingKey,
    DBNotFound(String),
    SlotNotFound(T),
    SlotNotCovered,
    Backend(BackendError),
    MigrationQueueError,
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
            DBSendError::MigrationQueueError => "migration queue error",
        }
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            DBSendError::MissingKey => None,
            DBSendError::DBNotFound(_) => None,
            DBSendError::SlotNotFound(_) => None,
            DBSendError::Backend(err) => Some(err),
            DBSendError::SlotNotCovered => None,
            DBSendError::MigrationQueueError => None,
        }
    }
}

fn gen_cluster_nodes_helper(
    name: &str,
    epoch: u64,
    slot_ranges: &HashMap<String, Vec<SlotRange>>,
) -> String {
    let mut cluster_nodes = String::from("");
    let mut name_seg = format!("{:_<20}", name);
    name_seg.truncate(20);
    for (addr, ranges) in slot_ranges {
        let mut addr_seg = format!("{:_<20}", addr);
        let id = format!("{}{}", name_seg, addr_seg);
        addr_seg.truncate(20);

        let slot_range = ranges
            .iter()
            .map(|range| format!("{}-{}", range.start, range.end))
            .collect::<Vec<String>>()
            .join(" ");

        let line = format!(
            "{id} {addr} {flags} {master} {ping_sent} {pong_recv} {epoch} {link_state} {slot_range}\n",
            id=id, addr=addr, flags="master", master="-", ping_sent=0, pong_recv=0, epoch=epoch,
            link_state="connected", slot_range=slot_range,
        );
        cluster_nodes.push_str(&line);
    }
    cluster_nodes
}

fn gen_cluster_slots_helper(
    slot_ranges: &HashMap<String, Vec<SlotRange>>,
) -> Result<Vec<Resp>, String> {
    let mut slot_range_element = Vec::new();
    for (addr, ranges) in slot_ranges {
        let mut segs = addr.split(':');
        let host = segs
            .next()
            .ok_or_else(|| format!("invalid address {}", addr))?;
        let port = segs
            .next()
            .ok_or_else(|| format!("invalid address {}", addr))?;

        for slot_range in ranges {
            let mut arr = if slot_range.start == slot_range.end {
                vec![Resp::Integer(slot_range.start.to_string().into_bytes())]
            } else {
                vec![
                    Resp::Integer(slot_range.start.to_string().into_bytes()),
                    Resp::Integer(slot_range.end.to_string().into_bytes()),
                ]
            };
            let ip_port_array = Resp::Arr(Array::Arr(vec![
                Resp::Bulk(BulkStr::Str(host.as_bytes().to_vec())),
                Resp::Bulk(BulkStr::Str(port.as_bytes().to_vec())),
            ]));
            arr.push(ip_port_array);
            slot_range_element.push(Resp::Arr(Array::Arr(arr)))
        }
    }
    Ok(slot_range_element)
}
