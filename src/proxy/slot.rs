use crate::common::cluster::SlotRange;
use crate::common::utils::SLOT_NUM;
use std::collections::HashMap;

pub struct SlotMap {
    data: SlotMapData,
}

impl SlotMap {
    pub fn from_ranges(slot_map: HashMap<String, Vec<SlotRange>>) -> Self {
        let mut map = HashMap::new();
        for (addr, slot_ranges) in slot_map {
            let mut slots = Vec::new();
            for slot_range in slot_ranges {
                for range in slot_range.get_range_list().get_ranges() {
                    slots.push((range.start(), range.end()));
                }
            }
            map.insert(addr, slots);
        }
        SlotMap {
            data: SlotMapData::new(map),
        }
    }

    pub fn get(&self, slot: usize) -> Option<&str> {
        self.data.get(slot)
    }
}

pub struct SlotMapData {
    slot_arr: Vec<Option<usize>>,
    addrs: Vec<String>,
}

impl SlotMapData {
    pub fn new(slot_map: HashMap<String, Vec<(usize, usize)>>) -> SlotMapData {
        let mut slot_arr = Vec::with_capacity(SLOT_NUM);
        let mut addrs = Vec::with_capacity(slot_map.len());
        for _ in 0..SLOT_NUM {
            slot_arr.push(None);
        }
        for (addr, slots) in slot_map.into_iter() {
            addrs.push(addr);
            for range in slots {
                let (start, end) = range;
                if start > end {
                    continue;
                }
                for s in start..=end {
                    if s >= SLOT_NUM {
                        break;
                    }
                    if let Some(opt) = slot_arr.get_mut(s) {
                        *opt = Some(addrs.len() - 1);
                    }
                }
            }
        }
        SlotMapData { slot_arr, addrs }
    }

    pub fn get(&self, slot: usize) -> Option<&str> {
        let addr_index = self.slot_arr.get(slot).and_then(|opt| *opt)?;
        self.addrs.get(addr_index).map(|s| s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::cluster::{RangeList, SlotRange, SlotRangeTag};
    use std::convert::TryFrom;

    #[test]
    fn test_slot_map() {
        let mut range_map = HashMap::new();
        let backend = "127.0.0.1:6379".to_string();
        range_map.insert(
            backend.clone(),
            vec![SlotRange {
                range_list: RangeList::try_from(format!("1 0-{}", SLOT_NUM - 1).as_str()).unwrap(),
                tag: SlotRangeTag::None,
            }],
        );

        let slot_map = SlotMap::from_ranges(range_map);
        for slot in 0..SLOT_NUM {
            let node = slot_map.get(slot).unwrap();
            assert_eq!(node, backend);
        }
    }
}
