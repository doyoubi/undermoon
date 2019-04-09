use futures::Future;
use protocol::RedisClientError;
use std::error::Error;
use std::fmt;
use std::io;

#[derive(Debug, PartialEq, Clone)]
pub struct MasterMeta {
    pub master_node_address: String,
    pub replica_node_address: String,
    pub replica_proxy_address: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ReplicaMeta {
    pub master_node_address: String,
    pub replica_node_address: String,
    pub master_proxy_address: String,
}

// MasterReplicator and ReplicaReplicator work together remotely to manage the replication.

pub trait MasterReplicator: Send + Sync {
    fn start(&self) -> Box<dyn Future<Item = (), Error = ReplicatorError> + Send>;
    fn stop(&self) -> Box<dyn Future<Item = (), Error = ReplicatorError> + Send>;
    fn start_migrating(&self) -> Box<dyn Future<Item = (), Error = ReplicatorError> + Send>;
    fn commit_migrating(&self) -> Box<dyn Future<Item = (), Error = ReplicatorError> + Send>;
    fn get_meta(&self) -> &MasterMeta;
}

pub trait ReplicaReplicator: Send + Sync {
    fn start(&self) -> Box<dyn Future<Item = (), Error = ReplicatorError> + Send>;
    fn stop(&self) -> Box<dyn Future<Item = (), Error = ReplicatorError> + Send>;
    fn start_importing(&self) -> Box<dyn Future<Item = (), Error = ReplicatorError> + Send>;
    fn commit_importing(&self) -> Box<dyn Future<Item = (), Error = ReplicatorError> + Send>;
    fn get_meta(&self) -> &ReplicaMeta;
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
