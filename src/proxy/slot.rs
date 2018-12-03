use std::sync::RwLock;
use std::collections::HashMap;
use crc16::{State, XMODEM};

pub const SLOT_NUM: usize = 16384;

pub enum SlotRangeTag {
    Migrating(String),
    None,
}

pub struct SlotRange {
    pub start: usize,
    pub end: usize,
    pub tag: SlotRangeTag,
}

pub struct SlotMap {
    data: RwLock<SlotMapData>,
}

impl SlotMap {
    pub fn new(slot_map: HashMap<String, Vec<usize>>) -> SlotMap {
        SlotMap{
            data: RwLock::new(SlotMapData::new(slot_map)),
        }
    }

    pub fn from_ranges(slot_map: HashMap<String, Vec<SlotRange>>) -> Self {
        let mut map = HashMap::new();
        let mut addrs = Vec::new();
        for (addr, slot_ranges) in slot_map {
            addrs.push(addr.clone());
            let mut slots = Vec::new();
            for range in slot_ranges {
                let mut slot = range.start;
                while slot < range.end {
                    if slot >= SLOT_NUM {
                        continue;
                    }
                    slots.push(slot);
                    slot += 1;
                }
            }
            map.insert(addr, slots);
        }
        Self{data: RwLock::new(SlotMapData::new(map))}
    }

    pub fn get_by_key(&self, key: &[u8]) -> Option<String> {
        let slot = State::<XMODEM>::calculate(key) as usize % SLOT_NUM;
        self.get(slot)
    }

    pub fn get(&self, slot: usize) -> Option<String> {
        self.data.read().unwrap().get(slot)
    }

    pub fn update(&self, slot_map: HashMap<String, Vec<usize>>) {
        let new_data = SlotMapData::new(slot_map);
        let mut guard = self.data.write().unwrap();
        *guard = new_data;
    }
}

pub struct SlotMapData {
    slot_arr: Vec<Option<usize>>,
    addrs: Vec<String>,
}

impl SlotMapData {
    pub fn new(slot_map: HashMap<String, Vec<usize>>) -> SlotMapData {
        let mut slot_arr = Vec::with_capacity(SLOT_NUM);
        let mut addrs = Vec::with_capacity(slot_map.len());
        for _ in 0..SLOT_NUM {
            slot_arr.push(None);
        }
        for (addr, slots) in slot_map.into_iter() {
            addrs.push(addr);
            for s in slots.into_iter() {
                slot_arr.get_mut(s).map(|opt| {
                    *opt = Some(addrs.len() - 1);
                });
            }
        }
        SlotMapData{
            slot_arr: slot_arr,
            addrs: addrs,
        }
    }

    pub fn get(&self, slot: usize) -> Option<String> {
        let addr_index = self.slot_arr.get(slot).and_then(|opt| opt.clone())?;
        self.addrs.get(addr_index).and_then(|s| Some(s.clone()))
    }
}