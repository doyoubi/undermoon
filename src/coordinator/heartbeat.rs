use std::io;
use std::fmt;
use std::error::Error;
use futures::{future, Future};
use ::common::cluster::{Cluster, Host};

pub struct DBMetaSender {}

impl DBMetaSender {
    fn send(&self, _host: Host) -> Box<dyn Future<Item = (), Error = HeartbeatError> + Send> {
        Box::new(future::ok(()))
    }
}

pub struct HostPeerMeta {
    epoch: u64,
    clusters: Vec<Cluster>,
}

pub struct PeerMetaSender {}

impl PeerMetaSender {
    fn send(&self, _peer_meta: HostPeerMeta) -> Box<dyn Future<Item = (), Error = HeartbeatError> + Send> {
        Box::new(future::ok(()))
    }
}

#[derive(Debug)]
pub enum HeartbeatError {
    Io(io::Error),
    InvalidReply,
}

impl fmt::Display for HeartbeatError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for HeartbeatError {
    fn description(&self) -> &str {
        "heartbeat error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            HeartbeatError::Io(err) => Some(err),
            _ => None,
        }
    }
}
