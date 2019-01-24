use std::collections::HashMap;
use crc16::{State, XMODEM};

pub const SLOT_NUM: usize = 16384;

#[derive(Debug, Clone)]
pub enum SlotRangeTag {
    Migrating(String),
    None,
}

#[derive(Debug, Clone)]
pub struct SlotRange {
    pub start: usize,
    pub end: usize,
    pub tag: SlotRangeTag,
}

pub struct SlotMap {
    data: SlotMapData,
}

impl SlotMap {
    pub fn from_ranges(slot_map: HashMap<String, Vec<SlotRange>>) -> Self {
        let mut map = HashMap::new();
        for (addr, slot_ranges) in slot_map {
            let mut slots = Vec::new();
            for range in slot_ranges {
                if range.end < range.start {
                    continue
                }
                let mut slot = range.start;
                while slot < range.end && slot < SLOT_NUM {
                    slots.push(slot);
                    slot += 1;
                }
            }
            map.insert(addr, slots);
        }
        SlotMap{
            data: SlotMapData::new(map),
        }
    }

    pub fn get_slot(&self, key: &[u8]) -> usize {
        State::<XMODEM>::calculate(key) as usize % SLOT_NUM
    }

    pub fn get_by_key(&self, key: &[u8]) -> Option<String> {
        let slot = self.get_slot(key);
        self.get(slot)
    }

    pub fn get(&self, slot: usize) -> Option<String> {
        self.data.get(slot)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_map() {
        let mut range_map = HashMap::new();
        let backend = "127.0.0.1:6379".to_string();
        range_map.insert(backend.clone(), vec![SlotRange{
            start: 0,
            end: SLOT_NUM,
            tag: SlotRangeTag::None,
        }]);

        let slot_map = SlotMap::from_ranges(range_map);
        for slot in 0..SLOT_NUM {
            let node = slot_map.get(slot);
            assert_eq!(node, Some(backend.clone()));
        }
    }
}