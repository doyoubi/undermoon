use crate::protocol::RespVec;
use crate::protocol::{Array, BulkStr, Resp};
use caseless;
use crc16::{State, XMODEM};
use futures::{stream, Stream};
use std::net::{SocketAddr, ToSocketAddrs};
use std::str;

pub trait ThreadSafe: Send + Sync + 'static {}

#[derive(Debug)]
pub struct CmdParseError {}

pub fn has_flags(s: &str, delimiter: char, flag: &'static str) -> bool {
    s.split(delimiter)
        .any(|s| caseless::canonical_caseless_match_str(s, flag))
}

pub fn resolve_first_address(address: &str) -> Option<SocketAddr> {
    match address.to_socket_addrs() {
        Ok(mut address_list) => match address_list.next() {
            Some(address) => Some(address),
            None => {
                error!("can not resolve address {}", address);
                None
            }
        },
        Err(e) => {
            error!("failed to parse address {} {:?}", address, e);
            None
        }
    }
}

pub fn get_resp_bytes(resp: &RespVec) -> Option<Vec<Vec<u8>>> {
    match resp {
        Resp::Arr(Array::Arr(ref resps)) => {
            let mut strs = vec![];
            for resp in resps.iter() {
                match resp {
                    Resp::Bulk(BulkStr::Str(s)) => strs.push(s.clone()),
                    Resp::Simple(s) => strs.push(s.clone()),
                    _ => return None,
                }
            }
            Some(strs)
        }
        _ => None,
    }
}

pub fn get_resp_strings<T: AsRef<[u8]>>(resp: &Resp<T>) -> Option<Vec<String>> {
    match resp {
        Resp::Arr(Array::Arr(ref resps)) => {
            let mut strs = vec![];
            for resp in resps.iter() {
                match resp {
                    Resp::Bulk(BulkStr::Str(s)) => {
                        strs.push(str::from_utf8(s.as_ref()).ok()?.to_string())
                    }
                    Resp::Simple(s) => strs.push(str::from_utf8(s.as_ref()).ok()?.to_string()),
                    _ => return None,
                }
            }
            Some(strs)
        }
        _ => None,
    }
}

pub fn get_command_element<T: AsRef<[u8]>>(resp: &Resp<T>, index: usize) -> Option<&[u8]> {
    match resp {
        Resp::Arr(Array::Arr(ref resps)) => resps.get(index).and_then(|resp| match resp {
            Resp::Bulk(BulkStr::Str(s)) => Some(s.as_ref()),
            _ => None,
        }),
        _ => None,
    }
}

pub fn change_bulk_array_element(resp: &mut RespVec, index: usize, data: Vec<u8>) -> bool {
    match resp {
        Resp::Arr(Array::Arr(ref mut resps)) => {
            Some(true) == resps.get_mut(index).map(|resp| change_bulk_str(resp, data))
        }
        _ => false,
    }
}

pub fn change_bulk_str(resp: &mut RespVec, data: Vec<u8>) -> bool {
    match resp {
        Resp::Bulk(BulkStr::Str(s)) => {
            *s = data;
            true
        }
        _ => false,
    }
}

pub fn gen_moved(slot: usize, addr: String) -> String {
    format!("MOVED {} {}", slot, addr)
}

pub fn get_hash_tag(key: &[u8]) -> &[u8] {
    if let Some(begin) = key.iter().position(|x| *x as char == '{') {
        if let Some(end_offset) = key[begin + 1..].iter().position(|x| *x as char == '}') {
            if end_offset == 0 {
                return key;
            }
            return &key[begin + 1..begin + 1 + end_offset];
        }
    }
    key
}

pub fn get_slot(key: &[u8]) -> usize {
    State::<XMODEM>::calculate(get_hash_tag(key)) as usize % SLOT_NUM
}

pub fn pretty_print_bytes(data: &[u8]) -> String {
    match str::from_utf8(data) {
        Ok(s) => s.to_string(),
        Err(_) => format!("{:?}", data),
    }
}

pub const OK_REPLY: &str = "OK";
pub const OLD_EPOCH_REPLY: &str = "OLD_EPOCH";
pub const TRY_AGAIN_REPLY: &str = "TRY_AGAIN";
pub const NOT_READY_FOR_SWITCHING_REPLY: &str = "NOT_READY_FOR_SWITCHING";
pub const SLOT_NUM: usize = 16384;

pub const MIGRATING_TAG: &str = "MIGRATING";
pub const IMPORTING_TAG: &str = "IMPORTING";

pub fn vec_result_to_stream<T, E>(res: Result<Vec<T>, E>) -> impl Stream<Item = Result<T, E>> {
    let elements = match res {
        Ok(v) => v.into_iter().map(Ok).collect(),
        Err(err) => vec![Err(err)],
    };
    stream::iter(elements)
}

pub struct Wrapper<T>(pub T);

impl<T> Wrapper<T> {
    pub fn into_inner(self) -> T {
        let Self(t) = self;
        t
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_hash_tag() {
        assert_eq!(
            get_hash_tag("{user1000}.following".as_bytes()),
            "user1000".as_bytes()
        );
        assert_eq!(
            get_hash_tag("foo{}{bar}".as_bytes()),
            "foo{}{bar}".as_bytes()
        );
        assert_eq!(get_hash_tag("foo{{bar}}".as_bytes()), "{bar".as_bytes());
        assert_eq!(get_hash_tag("foo{bar}{zap}".as_bytes()), "bar".as_bytes());
        assert_eq!(get_hash_tag("{}xxxxx".as_bytes()), "{}xxxxx".as_bytes());
    }
}
