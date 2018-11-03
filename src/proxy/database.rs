use std::sync;
use std::collections::HashMap;
use protocol::Resp;
use super::backend::CmdTask;
use super::backend::{BackendError, CmdTaskSender};
use super::slot::{SlotMap, SLOT_NUM};

pub struct Database<S: CmdTaskSender> {
    name: String,
    // We can improve this by using some concurrent map implementation.
    nodes: sync::RwLock<HashMap<String, S>>,
    slot_map: SlotMap,
}

impl<S: CmdTaskSender> Database<S> {
    pub fn new(name: String) -> Database<S> {
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
}

impl<S: CmdTaskSender> Database<S> {
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
