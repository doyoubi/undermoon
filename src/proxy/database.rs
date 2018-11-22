use std::sync;
use std::collections::HashMap;
use protocol::Resp;
use super::backend::CmdTask;
use super::backend::{BackendError, CmdTaskSender};
use super::slot::{SlotMap, SLOT_NUM};

pub const DEFAULT_DB : &'static str = "default_db";

pub trait DBTag {
    fn get_db_name(&self) -> String;
    fn set_db_name(&self, db: String);
}

pub struct DatabaseMap<S: CmdTaskSender> where S::Task: DBTag {
    local_dbs: sync::RwLock<HashMap<String, Database<S>>>,
}

impl<S: CmdTaskSender> DatabaseMap<S> where S::Task: DBTag {
    pub fn new() -> DatabaseMap<S> {
        let default_db = Database::new(DEFAULT_DB.to_string());
        let mut db_map = HashMap::new();
        db_map.insert(DEFAULT_DB.to_string(), default_db);
        Self{
            local_dbs: sync::RwLock::new(db_map),
        }
    }

    pub fn send(&self, cmd_task: S::Task) -> Result<(), BackendError> {
        let db_name = cmd_task.get_db_name();
        match self.local_dbs.read().unwrap().get(&db_name) {
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
        self.local_dbs.read().unwrap().keys().map(|s| s.clone()).collect()
    }

    pub fn clear(&self) {
        self.local_dbs.write().unwrap().clear()
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

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn update(&self, slot_map: HashMap<String, Vec<usize>>) {
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
