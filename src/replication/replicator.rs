use std::fmt;
use std::io;
use std::error::Error;
use futures::{Future};
use protocol::RedisClientError;

pub struct MasterMeta {
    pub master_node_address: String,
    pub replica_node_address: String,
    pub replica_proxy_address: String,
}

pub struct ReplicaMeta {
    pub master_node_address: String,
    pub replica_node_address: String,
    pub master_proxy_address: String,
}

// MasterReplicator and ReplicaReplicator work together remotely to manage the replication.

pub trait MasterReplicator {
    fn start(&self) -> Box<Future<Item = (), Error = ReplicatorError>>;
    fn stop(&self) -> Box<Future<Item = (), Error = ReplicatorError>>;
    fn start_migrating(&self) -> Box<Future<Item = (), Error = ReplicatorError>>;
    fn commit_migrating(&self) -> Box<Future<Item = (), Error = ReplicatorError>>;
}

pub trait ReplicaReplicator {
    fn start(&self) -> Box<Future<Item = (), Error = ReplicatorError>>;
    fn stop(&self) -> Box<Future<Item = (), Error = ReplicatorError>>;
    fn start_importing(&self) -> Box<Future<Item = (), Error = ReplicatorError>>;
    fn commit_importing(&self) -> Box<Future<Item = (), Error = ReplicatorError>>;
}

#[derive(Debug)]
pub enum ReplicatorError {
    IncompatibleVersion,
    InvalidAddress,
    AlreadyStarted,
    AlreadyEnded,
    Canceled,
    RedisError(RedisClientError),
    Io(io::Error),
}

impl fmt::Display for ReplicatorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for ReplicatorError {
    fn description(&self) -> &str {
        "replicator error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            ReplicatorError::Io(err) => Some(err),
            _ => None,
        }
    }
}
