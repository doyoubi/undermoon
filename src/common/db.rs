use std::collections::HashMap;
use std::iter::Peekable;
use std::str;
use ::protocol::{Resp, Array, BulkStr};
use super::cluster::{SlotRangeTag, SlotRange};

const MIGRATING_TAG: &'static str = "MIGRATING";

pub struct HostDBMap {
    epoch: u64,
    db_map: HashMap<String, HashMap<String, Vec<SlotRange>>>,
}

#[derive(Debug)]
pub struct NMCtlParseError {}

macro_rules! try_parse {
    ($expression:expr) => ({
        match $expression {
            Ok(v) => (v),
            Err(_) => return Err(NMCtlParseError{}),
        }
    })
}

macro_rules! try_get {
    ($expression:expr) => ({
        match $expression {
            Some(v) => (v),
            None => return Err(NMCtlParseError{}),
        }
    })
}

impl HostDBMap {
    pub fn get_epoch(&self) -> u64 { self.epoch }

    pub fn into_map(self) -> HashMap<String, HashMap<String, Vec<SlotRange>>> { self.db_map }

    pub fn from_resp(resp: &Resp) -> Result<Self, NMCtlParseError> {
        let arr = match resp {
            Resp::Arr(Array::Arr(ref arr)) => {
                arr
            }
            _ => return Err(NMCtlParseError{}),
        };

        let it = arr.iter().skip(2).flat_map(|resp| {
            match resp {
                Resp::Bulk(BulkStr::Str(safe_str)) => {
                    match str::from_utf8(safe_str) {
                        Ok(s) => Some(s.to_string()),
                        _ => return None,
                    }
                },
                _ => None,
            }
        });
        let mut it = it.peekable();

        let (epoch, db_map) = try_parse!(Self::parse(&mut it));

        Ok(Self{
            epoch: epoch,
            db_map: db_map,
        })
    }

    fn parse<It>(it: &mut Peekable<It>) -> Result<(u64, HashMap<String, HashMap<String, Vec<SlotRange>>>), NMCtlParseError>
        where It: Iterator<Item=String> {
        let epoch_str = try_get!(it.next());
        let epoch = try_parse!(epoch_str.parse::<u64>());

        let _flags = try_get!(it.next());

        let mut db_map = HashMap::new();

        while let Some(_) = it.peek() {
            let (dbname, address, slot_range) = try_parse!(Self::parse_db(it));
            let db = db_map.entry(dbname).or_insert(HashMap::new());
            let slots = db.entry(address).or_insert(vec![]);
            slots.push(slot_range);
        }

        return Ok((epoch, db_map))
    }

    fn parse_db<It>(it: &mut It) -> Result<(String, String, SlotRange), NMCtlParseError>
        where It: Iterator<Item=String> {
        let dbname = try_get!(it.next());
        let addr = try_get!(it.next());
        let slot_range = try_parse!(Self::parse_tagged_slot_range(it));
        Ok((dbname, addr, slot_range))
    }

    fn parse_tagged_slot_range<It>(it: &mut It) -> Result<SlotRange, NMCtlParseError> where It: Iterator<Item=String> {
        let slot_range = try_get!(it.next());
        if !caseless::canonical_caseless_match_str(&slot_range, MIGRATING_TAG) {
            return Self::parse_slot_range(slot_range);
        }

        let dst = try_get!(it.next());
        let mut slot_range = try_parse!(Self::parse_slot_range(try_get!(it.next())));
        slot_range.tag = SlotRangeTag::Migrating(dst);
        Ok(slot_range)
    }

    fn parse_slot_range(s: String) -> Result<SlotRange, NMCtlParseError> {
        let mut slot_range = s.split('-');
        let start_str = try_get!(slot_range.next());
        let end_str = try_get!(slot_range.next());
        let start = try_parse!(start_str.parse::<usize>());
        let end = try_parse!(end_str.parse::<usize>());
        Ok(SlotRange{
            start: start,
            end: end,
            tag: SlotRangeTag::None,
        })
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_db() {
        let mut arguments = vec![
            "233", "noflag", "dbname", "127.0.0.1:6379", "0-1000",
        ].into_iter().map(|s| s.to_string()).peekable();
        let r = HostDBMap::parse(&mut arguments);
        assert!(r.is_ok());
        let (epoch, hash) = r.unwrap();
        assert_eq!(epoch, 233);
        assert_eq!(hash.len(), 1);
    }

    #[test]
    fn test_multiple_slots() {
        let mut arguments = vec![
            "233", "noflag",
            "dbname", "127.0.0.1:6379", "0-1000",
            "dbname", "127.0.0.1:6379", "1001-2000",
        ].into_iter().map(|s| s.to_string()).peekable();
        let r = HostDBMap::parse(&mut arguments);
        assert!(r.is_ok());
        let (epoch, hash) = r.unwrap();
        assert_eq!(epoch, 233);
        assert_eq!(hash.len(), 1);
        assert_eq!(hash.get("dbname").unwrap().len(), 1);
        assert_eq!(hash.get("dbname").unwrap().get("127.0.0.1:6379").unwrap().len(), 2);
    }

    #[test]
    fn test_multiple_nodes() {
        let mut arguments = vec![
            "233", "noflag",
            "dbname", "127.0.0.1:7000", "0-1000",
            "dbname", "127.0.0.1:7001", "1001-2000",
        ].into_iter().map(|s| s.to_string()).peekable();
        let r = HostDBMap::parse(&mut arguments);
        assert!(r.is_ok());
        let (epoch, hash) = r.unwrap();
        assert_eq!(epoch, 233);
        assert_eq!(hash.len(), 1);
        assert_eq!(hash.get("dbname").unwrap().len(), 2);
        assert_eq!(hash.get("dbname").unwrap().get("127.0.0.1:7000").unwrap().len(), 1);
        assert_eq!(hash.get("dbname").unwrap().get("127.0.0.1:7001").unwrap().len(), 1);
    }

    #[test]
    fn test_multiple_db() {
        let mut arguments = vec![
            "233", "noflag",
            "dbname", "127.0.0.1:7000", "0-1000",
            "dbname", "127.0.0.1:7001", "1001-2000",
            "another_db", "127.0.0.1:7002", "0-2000",
        ].into_iter().map(|s| s.to_string()).peekable();
        let r = HostDBMap::parse(&mut arguments);
        assert!(r.is_ok());
        let (epoch, hash) = r.unwrap();
        assert_eq!(epoch, 233);
        assert_eq!(hash.len(), 2);
        assert_eq!(hash.get("dbname").unwrap().len(), 2);
        assert_eq!(hash.get("dbname").unwrap().get("127.0.0.1:7000").unwrap().len(), 1);
        assert_eq!(hash.get("dbname").unwrap().get("127.0.0.1:7001").unwrap().len(), 1);
        assert_eq!(hash.get("another_db").unwrap().len(), 1);
        assert_eq!(hash.get("another_db").unwrap().get("127.0.0.1:7002").unwrap().len(), 1);
    }
}
