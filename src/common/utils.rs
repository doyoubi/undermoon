use caseless;
use std::net::{SocketAddr, ToSocketAddrs};
use std::str;

pub trait ThreadSafe: Send + Sync + 'static {}

#[derive(Debug)]
pub struct CmdParseError {}

pub fn has_flags(s: &str, delimiter: char, flag: &'static str) -> bool {
    s.split(delimiter)
        .any(|s| caseless::canonical_caseless_match_str(s, flag))
}

pub fn revolve_first_address(address: &str) -> Option<SocketAddr> {
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

pub const OLD_EPOCH_REPLY: &str = "old_epoch";
