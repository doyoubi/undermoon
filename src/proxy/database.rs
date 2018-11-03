use std::sync;
use std::collections::HashMap;
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
        slot_map.insert("127.0.0.1:6379".to_string(), slots);
        Database{
            name: name,
            nodes: sync::RwLock::new(HashMap::new()),
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

impl<S: CmdTaskSender> CmdTaskSender for Database<S> {
    type Task = S::Task;

    fn send(&self, cmd_task: S::Task) -> Result<(), BackendError> {
        Ok(())
    }
}
