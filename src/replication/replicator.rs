use common::db::DBMapFlags;
use common::utils::CmdParseError;
use futures::Future;
use protocol::RedisClientError;
use protocol::{Array, BulkStr, Resp};
use std::error::Error;
use std::fmt;
use std::io;
use std::str;

#[derive(Debug, Clone)]
pub struct ReplicatorMeta {
    pub epoch: u64,
    pub flags: DBMapFlags,
    pub masters: Vec<MasterMeta>,
    pub replicas: Vec<ReplicaMeta>,
}

impl ReplicatorMeta {
    pub fn from_resp(resp: &Resp) -> Result<Self, CmdParseError> {
        parse_repl_meta(resp)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct MasterMeta {
    pub db_name: String,
    pub master_node_address: String,
    pub replica_node_address: String,
    pub replica_proxy_address: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ReplicaMeta {
    pub db_name: String,
    pub master_node_address: String,
    pub replica_node_address: String,
    pub master_proxy_address: String,
}

fn parse_repl_meta(resp: &Resp) -> Result<ReplicatorMeta, CmdParseError> {
    let arr = match resp {
        Resp::Arr(Array::Arr(ref arr)) => arr,
        _ => return Err(CmdParseError {}),
    };

    // Skip the "UMCTL SETREPL"
    let it = arr.iter().skip(2).flat_map(|resp| match resp {
        Resp::Bulk(BulkStr::Str(safe_str)) => match str::from_utf8(safe_str) {
            Ok(s) => Some(s.to_string()),
            _ => None,
        },
        _ => None,
    });
    let mut it = it.peekable();

    let epoch_str = it.next().ok_or(CmdParseError {})?;
    let epoch = epoch_str.parse::<u64>().map_err(|_e| CmdParseError {})?;

    let flags = DBMapFlags::from_arg(&it.next().ok_or(CmdParseError {})?);

    let mut master_meta_array = Vec::new();
    let mut replica_meta_array = Vec::new();

    while it.peek().is_some() {
        let role = it.next().ok_or(CmdParseError {})?;
        let db_name = it.next().ok_or(CmdParseError {})?;
        let master_node_address = it.next().ok_or(CmdParseError {})?;
        let replica_node_address = it.next().ok_or(CmdParseError {})?;
        let peer_proxy_address = it.next().ok_or(CmdParseError {})?;
        if role.to_uppercase() == "MASTER" {
            master_meta_array.push(MasterMeta {
                db_name,
                master_node_address,
                replica_node_address,
                replica_proxy_address: peer_proxy_address,
            })
        } else if role.to_uppercase() == "REPLICA" {
            replica_meta_array.push(ReplicaMeta {
                db_name,
                master_node_address,
                replica_node_address,
                master_proxy_address: peer_proxy_address,
            })
        } else {
            error!("invalid role {}", role);
            return Err(CmdParseError {});
        }
    }

    Ok(ReplicatorMeta {
        epoch,
        flags,
        masters: master_meta_array,
        replicas: replica_meta_array,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_replicator() {
        let arguments =
            "UMCTL SETREPL 233 force master testdb localhost:6000 localhost:6001 localhost:5299"
                .split(' ')
                .map(|s| Resp::Bulk(BulkStr::Str(s.to_string().into_bytes())))
                .collect();
        let resp = Resp::Arr(Array::Arr(arguments));
        let r = parse_repl_meta(&resp);
        assert!(r.is_ok());
        let meta = r.expect("not success");
        assert_eq!(meta.epoch, 233);
        assert_eq!(meta.flags, DBMapFlags { force: true });
        assert_eq!(meta.masters.len(), 1);
        assert_eq!(meta.replicas.len(), 0);
    }

    #[test]
    fn test_parse_multi_replicators() {
        let arguments = "UMCTL SETREPL 233 noflag master testdb localhost:6000 localhost:6001 localhost:5299 replica testdb localhost:6000 localhost:6001 localhost:5299"
            .split(' ')
            .map(|s| Resp::Bulk(BulkStr::Str(s.to_string().into_bytes())))
            .collect();
        let resp = Resp::Arr(Array::Arr(arguments));
        let r = parse_repl_meta(&resp);
        assert!(r.is_ok());
        let meta = r.expect("not success");
        assert_eq!(meta.epoch, 233);
        assert_eq!(meta.flags, DBMapFlags { force: false });
        assert_eq!(meta.masters.len(), 1);
        assert_eq!(meta.replicas.len(), 1);

        let master = &meta.masters[0];
        assert_eq!(master.db_name, "testdb");
        assert_eq!(master.master_node_address, "localhost:6000");
        assert_eq!(master.replica_node_address, "localhost:6001");
        assert_eq!(master.replica_proxy_address, "localhost:5299");

        let replica = &meta.replicas[0];
        assert_eq!(replica.db_name, "testdb");
        assert_eq!(replica.master_node_address, "localhost:6000");
        assert_eq!(replica.replica_node_address, "localhost:6001");
        assert_eq!(replica.master_proxy_address, "localhost:5299");
    }

}
