use super::backend::CmdTask;
use super::backend::{BackendError, CmdTaskSender, CmdTaskSenderFactory};
use super::slot::SlotMap;
use crate::common::cluster::{DBName, Range, SlotRange, SlotRangeTag};
use crate::common::config::ClusterConfig;
use crate::common::db::ProxyDBMeta;
use crate::common::utils::{gen_moved, get_slot};
use crate::migration::task::MigrationState;
use crate::protocol::{Array, BulkStr, Resp, RespVec};
use crc64::crc64;
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

    fn cause(&self) -> Option<&dyn Error> {
        None
    }
}

pub trait DBTag {
    fn get_db_name(&self) -> DBName;
    fn set_db_name(&mut self, db: DBName);
}

pub struct DatabaseMap<S: CmdTaskSender>
where
    <S as CmdTaskSender>::Task: DBTag,
{
    local_dbs: HashMap<DBName, Database<S>>,
    remote_dbs: HashMap<DBName, RemoteDB>,
}

impl<S: CmdTaskSender> Default for DatabaseMap<S>
where
    <S as CmdTaskSender>::Task: DBTag,
{
    fn default() -> Self {
        Self {
            local_dbs: HashMap::new(),
            remote_dbs: HashMap::new(),
        }
    }
}

impl<S: CmdTaskSender> DatabaseMap<S>
where
    <S as CmdTaskSender>::Task: DBTag,
{
    pub fn from_db_map<F: CmdTaskSenderFactory<Sender = S>>(
        db_meta: &ProxyDBMeta,
        sender_factory: &F,
    ) -> Self {
        let epoch = db_meta.get_epoch();

        let mut local_dbs = HashMap::new();
        for (db_name, slot_ranges) in db_meta.get_local().get_map().iter() {
            let config = db_meta.get_configs().get(db_name);
            let db = Database::from_slot_map(
                sender_factory,
                db_name.clone(),
                epoch,
                slot_ranges.clone(),
                config,
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
        cmd_task: <S as CmdTaskSender>::Task,
    ) -> Result<(), DBSendError<<S as CmdTaskSender>::Task>> {
        let (cmd_task, db_exists) = match self.local_dbs.get(&cmd_task.get_db_name()) {
            Some(db) => match db.send(cmd_task) {
                Err(DBSendError::SlotNotFound(cmd_task)) => (cmd_task, true),
                others => return others,
            },
            None => (cmd_task, false),
        };

        match self.remote_dbs.get(&cmd_task.get_db_name()) {
            Some(remote_db) => remote_db.send_remote(cmd_task),
            None => {
                if db_exists {
                    let resp = Resp::Error(
                        format!("slot not found: {}", cmd_task.get_db_name()).into_bytes(),
                    );
                    cmd_task.set_resp_result(Ok(resp));
                    Err(DBSendError::SlotNotCovered)
                } else {
                    let db_name = cmd_task.get_db_name().to_string();
                    debug!("db not found: {}", db_name);
                    let resp = Resp::Error(format!("db not found: {}", db_name).into_bytes());
                    cmd_task.set_resp_result(Ok(resp));
                    Err(DBSendError::DBNotFound(db_name))
                }
            }
        }
    }

    pub fn get_dbs(&self) -> Vec<DBName> {
        self.local_dbs.keys().cloned().collect()
    }

    pub fn gen_cluster_nodes(
        &self,
        dbname: DBName,
        service_address: String,
        migration_states: &HashMap<Range, MigrationState>,
    ) -> String {
        let local = self.local_dbs.get(&dbname).map_or("".to_string(), |db| {
            db.gen_local_cluster_nodes(service_address, migration_states)
        });
        let remote = self
            .remote_dbs
            .get(&dbname)
            .map_or("".to_string(), |remote_db| {
                remote_db.gen_remote_cluster_nodes(migration_states)
            });
        format!("{}{}", local, remote)
    }

    pub fn gen_cluster_slots(
        &self,
        dbname: DBName,
        service_address: String,
        migration_states: &HashMap<Range, MigrationState>,
    ) -> Result<RespVec, String> {
        let mut local = self.local_dbs.get(&dbname).map_or(Ok(vec![]), |db| {
            db.gen_local_cluster_slots(service_address, migration_states)
        })?;
        let mut remote = self
            .remote_dbs
            .get(&dbname)
            .map_or(Ok(vec![]), |remote_db| {
                remote_db.gen_remote_cluster_slots(migration_states)
            })?;
        local.append(&mut remote);
        Ok(Resp::Arr(Array::Arr(local)))
    }

    pub fn auto_select_db(&self) -> Option<DBName> {
        {
            let local = &self.local_dbs;
            match local.len() {
                0 => (),
                1 => return local.keys().next().cloned(),
                _ => return None,
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

    pub fn get_config(&self, dbname: &DBName) -> Option<&ClusterConfig> {
        self.local_dbs.get(dbname).map(|db| &db.config)
    }
}

// We combine the nodes and slot_map to let them fit into
// the same lock with a smaller critical section
// compared to the one we need if splitting them.
struct LocalDB<S: CmdTaskSender> {
    nodes: HashMap<String, S>,
    slot_map: SlotMap,
}

pub struct Database<S: CmdTaskSender> {
    name: DBName,
    epoch: u64,
    local_db: LocalDB<S>,
    slot_ranges: HashMap<String, Vec<SlotRange>>,
    config: ClusterConfig,
}

impl<S: CmdTaskSender> Database<S> {
    pub fn from_slot_map<F: CmdTaskSenderFactory<Sender = S>>(
        sender_factory: &F,
        name: DBName,
        epoch: u64,
        slot_map: HashMap<String, Vec<SlotRange>>,
        config: ClusterConfig,
    ) -> Self {
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
            config,
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
        cmd_task: <S as CmdTaskSender>::Task,
    ) -> Result<(), DBSendError<<S as CmdTaskSender>::Task>> {
        let key = match cmd_task.get_key() {
            Some(key) => key,
            None => {
                let resp = Resp::Error("missing key".to_string().into_bytes());
                cmd_task.set_resp_result(Ok(resp));
                return Err(DBSendError::MissingKey);
            }
        };

        match self.local_db.slot_map.get_by_key(key) {
            Some(addr) => match self.local_db.nodes.get(addr) {
                Some(sender) => sender.send(cmd_task).map_err(DBSendError::Backend),
                None => {
                    warn!("failed to get node");
                    Err(DBSendError::SlotNotFound(cmd_task))
                }
            },
            None => Err(DBSendError::SlotNotFound(cmd_task)),
        }
    }

    pub fn gen_local_cluster_nodes(
        &self,
        service_address: String,
        migration_states: &HashMap<Range, MigrationState>,
    ) -> String {
        let slots: Vec<SlotRange> = self
            .slot_ranges
            .values()
            .cloned()
            .flatten()
            .collect::<Vec<SlotRange>>();
        let mut slot_ranges = HashMap::new();
        slot_ranges.insert(service_address, slots);
        gen_cluster_nodes_helper(&self.name, self.epoch, &slot_ranges, migration_states)
    }

    pub fn gen_local_cluster_slots(
        &self,
        service_address: String,
        migration_states: &HashMap<Range, MigrationState>,
    ) -> Result<Vec<RespVec>, String> {
        let slots: Vec<SlotRange> = self
            .slot_ranges
            .values()
            .cloned()
            .flatten()
            .collect::<Vec<SlotRange>>();
        let mut slot_ranges = HashMap::new();
        slot_ranges.insert(service_address, slots);
        gen_cluster_slots_helper(&slot_ranges, migration_states)
    }
}

pub struct RemoteDB {
    name: DBName,
    epoch: u64,
    slot_map: SlotMap,
    slot_ranges: HashMap<String, Vec<SlotRange>>,
}

impl RemoteDB {
    pub fn from_slot_map(
        name: DBName,
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
        let key = match cmd_task.get_key() {
            Some(key) => key,
            None => {
                let resp = Resp::Error("missing key".to_string().into_bytes());
                cmd_task.set_resp_result(Ok(resp));
                return Err(DBSendError::MissingKey);
            }
        };
        match self.slot_map.get_by_key(key) {
            Some(addr) => {
                let resp = Resp::Error(gen_moved(get_slot(key), addr.to_string()).into_bytes());
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

    pub fn gen_remote_cluster_nodes(
        &self,
        migration_states: &HashMap<Range, MigrationState>,
    ) -> String {
        gen_cluster_nodes_helper(&self.name, self.epoch, &self.slot_ranges, migration_states)
    }

    pub fn gen_remote_cluster_slots(
        &self,
        migration_states: &HashMap<Range, MigrationState>,
    ) -> Result<Vec<RespVec>, String> {
        gen_cluster_slots_helper(&self.slot_ranges, migration_states)
    }
}

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

impl<T: CmdTask> fmt::Debug for DBSendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::MissingKey => "DBSendError::MissingKey".to_string(),
            Self::DBNotFound(db) => format!("DBSendError::DBNotFound({})", db),
            Self::SlotNotFound(_) => "DBSendError::SlotNotFound".to_string(),
            Self::SlotNotCovered => "DBSendError::SlotNotCovered".to_string(),
            Self::Backend(err) => format!("DBSendError::Backend({})", err),
            Self::MigrationError => "DBSendError::MigrationError".to_string(),
        };
        write!(f, "{}", s)
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

    fn cause(&self) -> Option<&dyn Error> {
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
    name: &DBName,
    epoch: u64,
    slot_ranges: &HashMap<String, Vec<SlotRange>>,
    migration_states: &HashMap<Range, MigrationState>,
) -> String {
    let mut cluster_nodes = String::from("");
    let mut name_seg = format!("{:_<20}", name.to_string());
    name_seg.truncate(20);
    for (addr, ranges) in slot_ranges {
        let mut addr_hash_seg = format!("{:_<20x}", crc64(0, addr.as_bytes()));
        addr_hash_seg.truncate(20);
        let id = format!("{}{}", name_seg, addr_hash_seg);

        let mut slot_range_str = String::new();
        let slot_range = ranges
            .iter()
            .map(|range| match range.tag {
                // In the new migration protocol, after switching at the very beginning,
                // the importing nodes will take care of all the migrating slots.
                SlotRangeTag::Migrating(ref _meta)
                    if migration_states.get(&range.to_range()).cloned()
                        != Some(MigrationState::PreCheck) =>
                {
                    None
                }
                SlotRangeTag::Importing(ref _meta)
                    if migration_states.get(&range.to_range()).cloned()
                        == Some(MigrationState::PreCheck) =>
                {
                    None
                }
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
    migration_states: &HashMap<Range, MigrationState>,
) -> Result<Vec<RespVec>, String> {
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
            // In the new migration protocol, after switching at the very beginning,
            // the importing nodes will take care of all the migrating slots.
            match slot_range.tag {
                SlotRangeTag::Migrating(ref _meta)
                    if migration_states.get(&slot_range.to_range()).cloned()
                        != Some(MigrationState::PreCheck) =>
                {
                    continue
                }
                SlotRangeTag::Importing(ref _meta)
                    if migration_states.get(&slot_range.to_range()).cloned()
                        == Some(MigrationState::PreCheck) =>
                {
                    continue
                }
                _ => (),
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
    use crate::common::cluster::MigrationMeta;
    use crate::protocol::{Array, BulkStr};
    use std::iter::repeat;

    fn gen_testing_slot_ranges(address: &str) -> HashMap<String, Vec<SlotRange>> {
        let mut slot_ranges = HashMap::new();
        slot_ranges.insert(
            address.to_string(),
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

    fn gen_testing_migration_slot_ranges(migrating: bool) -> HashMap<String, Vec<SlotRange>> {
        let mut slot_ranges = HashMap::new();
        let meta = MigrationMeta {
            epoch: 200,
            src_proxy_address: "127.0.0.1:7000".to_string(),
            src_node_address: "127.0.0.1:6379".to_string(),
            dst_proxy_address: "127.0.0.1:7001".to_string(),
            dst_node_address: "127.0.0.1:6380".to_string(),
        };
        let tag = if migrating {
            SlotRangeTag::Migrating(meta)
        } else {
            SlotRangeTag::Importing(meta)
        };
        slot_ranges.insert(
            "127.0.0.1:5299".to_string(),
            vec![SlotRange {
                start: 0,
                end: 1000,
                tag,
            }],
        );
        slot_ranges
    }

    #[test]
    fn test_gen_cluster_nodes() {
        let m = HashMap::new();
        let slot_ranges = gen_testing_slot_ranges("127.0.0.1:5299");
        let output =
            gen_cluster_nodes_helper(&DBName::from("testdb").unwrap(), 233, &slot_ranges, &m);
        assert_eq!(output, "testdb______________9f8fca2805923328____ 127.0.0.1:5299 master - 0 0 233 connected 0-100 300\n");
    }

    #[test]
    fn test_gen_cluster_nodes_with_long_address() {
        // Should always be able to find the migration state.
        // This will never be empty. But should also work.
        let m = HashMap::new();
        let long_address: String = repeat('x').take(50).collect();
        let slot_ranges = gen_testing_slot_ranges(&long_address);
        let output =
            gen_cluster_nodes_helper(&DBName::from("testdb").unwrap(), 233, &slot_ranges, &m);
        assert_eq!(output, format!("testdb______________a744988af9aa86ed____ {} master - 0 0 233 connected 0-100 300\n", long_address));
    }

    #[test]
    fn test_gen_importing_cluster_nodes() {
        let m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(false);
        let output =
            gen_cluster_nodes_helper(&DBName::from("testdb").unwrap(), 233, &slot_ranges, &m);
        assert_eq!(
            output,
            "testdb______________9f8fca2805923328____ 127.0.0.1:5299 master - 0 0 233 connected 0-1000\n"
        );
    }

    #[test]
    fn test_gen_importing_cluster_nodes_without_pre_check() {
        let mut m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(false);
        for slot_ranges in slot_ranges.values() {
            for slot_range in slot_ranges {
                m.insert(slot_range.to_range(), MigrationState::PreCheck);
            }
        }
        let output =
            gen_cluster_nodes_helper(&DBName::from("testdb").unwrap(), 233, &slot_ranges, &m);
        assert_eq!(
            output,
            "testdb______________9f8fca2805923328____ 127.0.0.1:5299 master - 0 0 233 connected\n"
        );
    }

    #[test]
    fn test_gen_importing_cluster_nodes_with_pre_check() {
        let mut m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(false);
        for slot_ranges in slot_ranges.values() {
            for slot_range in slot_ranges {
                m.insert(slot_range.to_range(), MigrationState::PreBlocking);
            }
        }
        let output =
            gen_cluster_nodes_helper(&DBName::from("testdb").unwrap(), 233, &slot_ranges, &m);
        assert_eq!(
            output,
            "testdb______________9f8fca2805923328____ 127.0.0.1:5299 master - 0 0 233 connected 0-1000\n"
        );
    }

    #[test]
    fn test_gen_migrating_cluster_nodes() {
        // Should always be able to find the migration state.
        // This will never be empty. But should also work.
        let m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(true);
        let output =
            gen_cluster_nodes_helper(&DBName::from("testdb").unwrap(), 233, &slot_ranges, &m);
        assert_eq!(
            output,
            "testdb______________9f8fca2805923328____ 127.0.0.1:5299 master - 0 0 233 connected\n"
        );
    }

    #[test]
    fn test_gen_migrating_cluster_nodes_without_pre_check() {
        let mut m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(true);
        for slot_ranges in slot_ranges.values() {
            for slot_range in slot_ranges {
                m.insert(slot_range.to_range(), MigrationState::PreCheck);
            }
        }
        let output =
            gen_cluster_nodes_helper(&DBName::from("testdb").unwrap(), 233, &slot_ranges, &m);
        assert_eq!(
            output,
            "testdb______________9f8fca2805923328____ 127.0.0.1:5299 master - 0 0 233 connected 0-1000\n"
        );
    }

    #[test]
    fn test_gen_migrating_cluster_nodes_with_pre_check() {
        let mut m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(true);
        for slot_ranges in slot_ranges.values() {
            for slot_range in slot_ranges {
                m.insert(slot_range.to_range(), MigrationState::PreBlocking);
            }
        }
        let output =
            gen_cluster_nodes_helper(&DBName::from("testdb").unwrap(), 233, &slot_ranges, &m);
        assert_eq!(
            output,
            "testdb______________9f8fca2805923328____ 127.0.0.1:5299 master - 0 0 233 connected\n"
        );
    }

    #[test]
    fn test_gen_cluster_slots() {
        let m = HashMap::new();
        let slot_ranges = gen_testing_slot_ranges("127.0.0.1:5299");
        let output = gen_cluster_slots_helper(&slot_ranges, &m).expect("test_gen_cluster_slots");
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
        let m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(false);
        let output = gen_cluster_slots_helper(&slot_ranges, &m).expect("test_gen_cluster_slots");
        assert_eq!(output.len(), 1);
    }

    #[test]
    fn test_gen_migrating_cluster_slots() {
        let m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(true);
        let output = gen_cluster_slots_helper(&slot_ranges, &m).expect("test_gen_cluster_slots");
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn test_default_db_length() {
        DBName::from(DEFAULT_DB).unwrap();
    }
}
