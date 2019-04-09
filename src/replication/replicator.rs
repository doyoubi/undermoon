use common::utils::CmdParseError;
use futures::Future;
use protocol::RedisClientError;
use protocol::{Array, BulkStr, Resp};
use std::error::Error;
use std::fmt;
use std::io;
use std::str;

#[derive(Debug, PartialEq, Clone)]
pub struct MasterMeta {
    pub master_node_address: String,
    pub replica_node_address: String,
    pub replica_proxy_address: String,
}

impl MasterMeta {
    pub fn from_resp(resp: &Resp) -> Result<Vec<Self>, CmdParseError> {
        let meta_array = parse_repl_meta(resp)?;
        Ok(meta_array
            .into_iter()
            .map(|meta| Self {
                master_node_address: meta.0,
                replica_node_address: meta.1,
                replica_proxy_address: meta.2,
            })
            .collect())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ReplicaMeta {
    pub master_node_address: String,
    pub replica_node_address: String,
    pub master_proxy_address: String,
}

impl ReplicaMeta {
    pub fn from_resp(resp: &Resp) -> Result<Vec<Self>, CmdParseError> {
        let meta_array = parse_repl_meta(resp)?;
        Ok(meta_array
            .into_iter()
            .map(|meta| Self {
                master_node_address: meta.0,
                replica_node_address: meta.1,
                master_proxy_address: meta.2,
            })
            .collect())
    }
}

struct ReplMeta(String, String, String);

fn parse_repl_meta(resp: &Resp) -> Result<Vec<ReplMeta>, CmdParseError> {
    let arr = match resp {
        Resp::Arr(Array::Arr(ref arr)) => arr,
        _ => return Err(CmdParseError {}),
    };

    // Skip the "UMCTL SETMASTER|SETREPLIA"
    let it = arr.iter().skip(2).flat_map(|resp| match resp {
        Resp::Bulk(BulkStr::Str(safe_str)) => match str::from_utf8(safe_str) {
            Ok(s) => Some(s.to_string()),
            _ => None,
        },
        _ => None,
    });
    let mut it = it.peekable();

    let mut meta_array = Vec::new();

    while it.peek().is_some() {
        let arg1 = it.next().ok_or(CmdParseError {})?;
        let arg2 = it.next().ok_or(CmdParseError {})?;
        let arg3 = it.next().ok_or(CmdParseError {})?;
        meta_array.push(ReplMeta(arg1, arg2, arg3));
    }

    Ok(meta_array)
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
