use super::response::ERR_MOVED;
use crate::protocol::{Array, BulkStr, Resp};
use crate::protocol::{BinSafeStr, RespVec};
use crc16::{State, ARC, XMODEM};
use futures::{stream, Stream};
use std::cmp::min;
use std::fmt;
use std::net::SocketAddr;
use std::str;
use tokio::net::lookup_host;

pub trait ThreadSafe: Send + Sync + 'static {}

impl<T: Send + Sync + 'static> ThreadSafe for T {}

#[derive(Debug)]
pub struct CmdParseError {}

pub fn has_flags(s: &str, delimiter: char, flag: &'static str) -> bool {
    s.split(delimiter)
        .any(|s| str_ascii_case_insensitive_eq(s, flag))
}

pub async fn resolve_first_address(address: &str) -> Option<SocketAddr> {
    match lookup_host(address).await {
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

pub fn get_command_len<T>(resp: &Resp<T>) -> Option<usize> {
    match resp {
        Resp::Arr(Array::Arr(ref resps)) => Some(resps.len()),
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

pub fn left_trim_array<T>(resp: &mut Resp<T>, removed_num: usize) -> Option<usize> {
    match resp {
        Resp::Arr(Array::Arr(ref mut resps)) => {
            let start = min(removed_num, resps.len());
            let new_resps = resps.drain(start..).collect();
            *resps = new_resps;
            Some(resps.len())
        }
        _ => None,
    }
}

// Returns success or not
pub fn array_append_front(resp: &mut RespVec, preceding_elements: Vec<BinSafeStr>) -> bool {
    match resp {
        Resp::Arr(Array::Arr(ref mut resps)) => {
            let mut new_resps: Vec<_> = preceding_elements
                .into_iter()
                .map(|s| Resp::Bulk(BulkStr::Str(s)))
                .collect();
            new_resps.append(resps);
            *resps = new_resps;
            true
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
    format!("{} {} {}", ERR_MOVED, slot, addr)
}

pub fn get_hash_tag(key: &[u8]) -> &[u8] {
    if let Some(begin) = key.iter().position(|x| *x as char == '{') {
        if let Some(end_offset) = key
            .get(begin + 1..)
            .and_then(|t| t.iter().position(|x| *x as char == '}'))
        {
            if end_offset == 0 {
                return key;
            }
            let start = begin + 1;
            let end = start + end_offset;
            return match key.get(start..end) {
                Some(slice) => slice,
                None => {
                    error!(
                        "FATAL get_hash_tag data len: {} range: {} {}",
                        key.len(),
                        start,
                        end
                    );
                    key
                }
            };
        }
    }
    key
}

// Hash function for request routing.
pub fn generate_slot(key: &[u8]) -> usize {
    State::<XMODEM>::calculate(get_hash_tag(key)) as usize % SLOT_NUM
}

// Hash function for locking in migration.
// This should be different from `generate_slot`
// to alleviate lock contention.
pub fn generate_lock_slot(key: &[u8]) -> usize {
    State::<ARC>::calculate(key) as usize % SLOT_NUM
}

pub fn same_slot<'a, It: Iterator<Item = &'a [u8]>>(mut key_iter: It) -> bool {
    let slot = match key_iter.next() {
        None => return false,
        Some(k) => generate_slot(k),
    };
    for k in key_iter {
        if generate_slot(k) != slot {
            return false;
        }
    }
    true
}

pub fn pretty_print_bytes(data: &[u8]) -> String {
    match str::from_utf8(data) {
        Ok(s) => s.to_string(),
        Err(_) => format!("{:?}", data),
    }
}

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

impl<T> From<T> for Wrapper<T> {
    fn from(t: T) -> Self {
        Wrapper(t)
    }
}

pub fn str_ascii_case_insensitive_eq(lhs: &str, rhs: &str) -> bool {
    bytes_ascii_case_insensitive_eq(lhs.as_bytes(), rhs.as_bytes())
}

pub fn bytes_ascii_case_insensitive_eq(lhs: &[u8], rhs: &[u8]) -> bool {
    const DELTA: u8 = b'a' - b'A';
    if lhs.len() != rhs.len() {
        return false;
    }
    // Use this trick if needed:
    // https://blog.cloudflare.com/the-oldest-trick-in-the-ascii-book/
    for (a, b) in lhs.iter().zip(rhs) {
        let a = *a;
        let b = *b;
        if a == b {
            continue;
        }
        if b'a' <= a && a <= b'z' && a == b + DELTA {
            continue;
        }
        if b'A' <= a && a <= b'Z' && a + DELTA == b {
            continue;
        }
        return false;
    }
    true
}

#[inline]
pub fn byte_to_uppercase(b: u8) -> u8 {
    const DELTA: u8 = b'a' - b'A';
    if b'a' <= b && b <= b'z' {
        b - DELTA
    } else {
        b
    }
}

pub struct RetryError<T> {
    inner: T,
}

impl<T: fmt::Debug> fmt::Debug for RetryError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RetryError({:?})", self.inner)
    }
}

impl<T> RetryError<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

pub fn extract_host_from_address(address: &str) -> Option<&str> {
    let mut it = address.splitn(2, ':');
    let host = it.next()?;
    it.next()?;
    Some(host)
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
        assert_eq!(get_hash_tag("{".as_bytes()), "{".as_bytes());
    }

    #[test]
    fn test_bytes_ascii_case_insensitive_eq() {
        assert!(bytes_ascii_case_insensitive_eq(b"a", b"a"));
        assert!(bytes_ascii_case_insensitive_eq(b"a", b"A"));
        assert!(bytes_ascii_case_insensitive_eq(b"za", b"zA"));
        assert!(bytes_ascii_case_insensitive_eq(b"cUi", b"CuI"));
        assert!(!bytes_ascii_case_insensitive_eq(b"a", b"aa"));
        assert!(!bytes_ascii_case_insensitive_eq(b"ab", b"aa"));

        for (l, u) in (b'a'..=b'z').into_iter().zip(b'A'..=b'Z') {
            let a = [l];
            let b = [u];
            assert!(bytes_ascii_case_insensitive_eq(&a, &b));
        }
    }

    #[test]
    fn test_byte_to_uppercase() {
        assert_eq!(byte_to_uppercase(b'@'), b'@');
        for (l, u) in (b'a'..=b'z').into_iter().zip(b'A'..=b'Z') {
            assert_eq!(byte_to_uppercase(l), u);
        }
    }

    #[test]
    fn test_extract_host_from_address() {
        assert_eq!(
            extract_host_from_address("localhost:12345").unwrap(),
            "localhost"
        );
        assert!(extract_host_from_address("").is_none());
        assert!(extract_host_from_address("localhost").is_none());
    }
}
