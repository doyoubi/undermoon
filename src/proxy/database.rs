use super::backend::CmdTask;
use super::backend::{BackendError, CmdTaskSender, CmdTaskSenderFactory};
use super::slot::SlotMap;
use common::cluster::{SlotRange, SlotRangeTag};
use common::db::ProxyDBMeta;
use common::utils::{gen_moved, get_key, get_slot};
use protocol::{Array, BulkStr, Resp};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::iter::Iterator;

pub const DEFAULT_DB: &str = "admin";

#[derive(Debug)]
pub enum DBError {
    OldEpoch,
    TryAgain,
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
    local_dbs: HashMap<String, Database<F>>,
    remote_dbs: HashMap<String, RemoteDB>,
}

impl<F: CmdTaskSenderFactory> Default for DatabaseMap<F>
where
    <<F as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task: DBTag,
{
    fn default() -> Self {
        Self {
            local_dbs: HashMap::new(),
            remote_dbs: HashMap::new(),
        }
    }
}

impl<F: CmdTaskSenderFactory> DatabaseMap<F>
where
    <<F as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task: DBTag,
{
    pub fn from_db_map(db_meta: &ProxyDBMeta, sender_factory: &F) -> Self {
        let epoch = db_meta.get_epoch();

        let mut local_dbs = HashMap::new();
        for (db_name, slot_ranges) in db_meta.get_local().get_map().iter() {
            let db = Database::from_slot_map(
                sender_factory,
                db_name.clone(),
                epoch,
                slot_ranges.clone(),
            );
            local_dbs.insert(db_name.clone(), db);
        }

        let mut remote_dbs = HashMap::new();
        for (db_name, slot_ranges) in db_meta.get_peer().get_map().iter() {
            let remote_db = RemoteDB::from_slot_map(db_name.clone(), epoch, slot_ranges.clone());
            remote_dbs.insert(db_name.clone(), remote_db);
        }
        Self {
            local_dbs,
            remote_dbs,
        }
    }

    pub fn info(&self) -> String {
        let local = self
            .local_dbs
            .iter()
            .map(|(_, db)| db.info())
            .collect::<Vec<String>>()
            .join("\r\n");
        let peer = self
            .remote_dbs
            .iter()
            .map(|(_, db)| db.info())
            .collect::<Vec<String>>()
            .join("\r\n");
        format!("{}\r\n{}", local, peer)
    }

    pub fn send(
        &self,
        cmd_task: <<F as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task,
    ) -> Result<(), DBSendError<<<F as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>> {
        let db_name = cmd_task.get_db_name();
        let (cmd_task, db_exists) = match self.local_dbs.get(&db_name) {
            Some(db) => match db.send(cmd_task) {
                Err(DBSendError::SlotNotFound(cmd_task)) => (cmd_task, true),
                others => return others,
            },
            None => (cmd_task, false),
        };

        match self.remote_dbs.get(&db_name) {
            Some(remote_db) => remote_db.send_remote(cmd_task),
            None => {
                if db_exists {
                    let resp =
                        Resp::Error(format!("slot not found: {}", db_name.clone()).into_bytes());
                    cmd_task.set_resp_result(Ok(resp));
                    Err(DBSendError::SlotNotCovered)
                } else {
                    debug!("db not found: {}", db_name);
                    let resp =
                        Resp::Error(format!("db not found: {}", db_name.clone()).into_bytes());
                    cmd_task.set_resp_result(Ok(resp));
                    Err(DBSendError::DBNotFound(db_name))
                }
            }
        }
    }

    pub fn get_dbs(&self) -> Vec<String> {
        self.local_dbs.keys().cloned().collect()
    }

    pub fn gen_cluster_nodes(&self, dbname: String, service_address: String) -> String {
        let local = self.local_dbs.get(&dbname).map_or("".to_string(), |db| {
            db.gen_local_cluster_nodes(service_address)
        });
        let remote = self
            .remote_dbs
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
            .get(&dbname)
            .map_or(Ok(vec![]), |db| db.gen_local_cluster_slots(service_address))?;
        let mut remote = self
            .remote_dbs
            .get(&dbname)
            .map_or(Ok(vec![]), RemoteDB::gen_remote_cluster_slots)?;
        local.append(&mut remote);
        Ok(Resp::Arr(Array::Arr(local)))
    }

    pub fn auto_select_db(&self) -> Option<String> {
        {
            let local = &self.local_dbs;
            if local.len() == 1 {
                return local.keys().next().cloned();
            } else if local.len() > 1 {
                return None;
            }
        }
        {
            let remote = &self.remote_dbs;
            if remote.len() == 1 {
                return remote.keys().next().cloned();
            }
        }
        None
    }
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

    pub fn info(&self) -> String {
        let mut lines = vec![
            format!("name: {}", self.name),
            format!("epoch: {}", self.epoch),
            "nodes:".to_string(),
        ];
        for (node, slot_ranges) in self.slot_ranges.iter() {
            let slot_ranges = slot_ranges
                .iter()
                .map(|slot_range| {
                    format!(
                        "{}-{} {:?}",
                        slot_range.start,
                        slot_range.end,
                        slot_range.tag.get_migration_meta()
                    )
                })
                .collect::<Vec<String>>()
                .join(", ");
            lines.push(format!("{}: {}", node, slot_ranges));
        }
        lines.join("\r\n")
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
                    warn!("failed to get node");
                    Err(DBSendError::SlotNotFound(cmd_task))
                }
            },
            None => Err(DBSendError::SlotNotFound(cmd_task)),
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

    pub fn info(&self) -> String {
        let mut lines = vec!["peers:".to_string()];
        for (node, slot_ranges) in self.slot_ranges.iter() {
            let slot_ranges = slot_ranges
                .iter()
                .map(|slot_range| {
                    format!(
                        "{}-{} {:?}",
                        slot_range.start,
                        slot_range.end,
                        slot_range.tag.get_migration_meta()
                    )
                })
                .collect::<Vec<String>>()
                .join(", ");
            lines.push(format!("{}: {}", node, slot_ranges));
        }
        lines.join("\r\n")
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
                let resp = Resp::Error(gen_moved(get_slot(&key), addr).into_bytes());
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
    MigrationError,
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
            DBSendError::MigrationError => "migration queue error",
        }
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            DBSendError::MissingKey => None,
            DBSendError::DBNotFound(_) => None,
            DBSendError::SlotNotFound(_) => None,
            DBSendError::Backend(err) => Some(err),
            DBSendError::SlotNotCovered => None,
            DBSendError::MigrationError => None,
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

        let mut slot_range_str = String::new();
        let slot_range = ranges
            .iter()
            .map(|range| match range.tag {
                SlotRangeTag::Importing(ref _meta) => None,
                _ if range.start == range.end => Some(range.start.to_string()),
                _ => Some(format!("{}-{}", range.start, range.end)),
            })
            .filter_map(|s| s)
            .collect::<Vec<String>>()
            .join(" ");
        if !slot_range.is_empty() {
            slot_range_str.push(' ');
            slot_range_str.push_str(&slot_range);
        }

        let line = format!(
            "{id} {addr} {flags} {master} {ping_sent} {pong_recv} {epoch} {link_state}{slot_range}\n",
            id=id, addr=addr, flags="master", master="-", ping_sent=0, pong_recv=0, epoch=epoch,
            link_state="connected", slot_range=slot_range_str,
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
            if let SlotRangeTag::Importing(_) = slot_range.tag {
                continue;
            }

            let mut arr = vec![
                Resp::Integer(slot_range.start.to_string().into_bytes()),
                Resp::Integer(slot_range.end.to_string().into_bytes()),
            ];
            let ip_port_array = Resp::Arr(Array::Arr(vec![
                Resp::Bulk(BulkStr::Str(host.as_bytes().to_vec())),
                Resp::Integer(port.as_bytes().to_vec()),
            ]));
            arr.push(ip_port_array);
            slot_range_element.push(Resp::Arr(Array::Arr(arr)))
        }
    }
    Ok(slot_range_element)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::common::cluster::MigrationMeta;

    fn gen_testing_slot_ranges() -> HashMap<String, Vec<SlotRange>> {
        let mut slot_ranges = HashMap::new();
        slot_ranges.insert(
            "127.0.0.1:5299".to_string(),
            vec![
                SlotRange {
                    start: 0,
                    end: 100,
                    tag: SlotRangeTag::None,
                },
                SlotRange {
                    start: 300,
                    end: 300,
                    tag: SlotRangeTag::None,
                },
            ],
        );
        slot_ranges
    }

    fn gen_testing_impporting_slot_ranges() -> HashMap<String, Vec<SlotRange>> {
        let mut slot_ranges = HashMap::new();
        slot_ranges.insert(
            "127.0.0.1:5299".to_string(),
            vec![SlotRange {
                start: 0,
                end: 1000,
                tag: SlotRangeTag::Importing(MigrationMeta {
                    epoch: 200,
                    src_proxy_address: "127.0.0.1:7000".to_string(),
                    src_node_address: "127.0.0.1:6379".to_string(),
                    dst_proxy_address: "127.0.0.1:7001".to_string(),
                    dst_node_address: "127.0.0.1:6380".to_string(),
                }),
            }],
        );
        slot_ranges
    }

    #[test]
    fn test_gen_cluster_nodes() {
        let slot_ranges = gen_testing_slot_ranges();
        let output = gen_cluster_nodes_helper("testdb", 233, &slot_ranges);
        assert_eq!(output, "testdb______________127.0.0.1:5299______ 127.0.0.1:5299 master - 0 0 233 connected 0-100 300\n");
    }

    #[test]
    fn test_gen_importing_cluster_nodes() {
        let slot_ranges = gen_testing_impporting_slot_ranges();
        let output = gen_cluster_nodes_helper("testdb", 233, &slot_ranges);
        assert_eq!(
            output,
            "testdb______________127.0.0.1:5299______ 127.0.0.1:5299 master - 0 0 233 connected\n"
        );
    }

    #[test]
    fn test_gen_cluster_slots() {
        let slot_ranges = gen_testing_slot_ranges();
        let output = gen_cluster_slots_helper(&slot_ranges).expect("test_gen_cluster_slots");
        let slot_range1 = Resp::Arr(Array::Arr(vec![
            Resp::Integer(0.to_string().into_bytes()),
            Resp::Integer(100.to_string().into_bytes()),
            Resp::Arr(Array::Arr(vec![
                Resp::Bulk(BulkStr::Str("127.0.0.1".to_string().into_bytes())),
                Resp::Integer(5299.to_string().into_bytes()),
            ])),
        ]));
        let slot_range2 = Resp::Arr(Array::Arr(vec![
            Resp::Integer(300.to_string().into_bytes()),
            Resp::Integer(300.to_string().into_bytes()),
            Resp::Arr(Array::Arr(vec![
                Resp::Bulk(BulkStr::Str("127.0.0.1".to_string().into_bytes())),
                Resp::Integer(5299.to_string().into_bytes()),
            ])),
        ]));
        if output != vec![slot_range2.clone(), slot_range1.clone()] {
            assert_eq!(output, vec![slot_range1, slot_range2]);
        }
    }

    #[test]
    fn test_gen_importing_cluster_slots() {
        let slot_ranges = gen_testing_impporting_slot_ranges();
        let output = gen_cluster_slots_helper(&slot_ranges).expect("test_gen_cluster_slots");
        assert_eq!(output, vec![]);
    }
}
