use std::io;
use std::fmt;
use std::error::Error;
use futures::{Future, Stream};
use ::common::utils::ThreadSafe;
use ::common::cluster::{Cluster, Node, Host};


pub trait MetaDataBroker: ThreadSafe {
    fn get_cluster_names(&self) -> Box<dyn Stream<Item = String, Error = MetaDataBrokerError> + Send>;
    fn get_cluster(&self, name: String) -> Box<dyn Future<Item = Option<Cluster>, Error = MetaDataBrokerError> + Send>;
    fn get_host_addresses(&self) -> Box<dyn Stream<Item = String, Error = MetaDataBrokerError> + Send>;
    fn get_host(&self, address: String) -> Box<dyn Future<Item = Option<Host>, Error = MetaDataBrokerError> + Send>;
    fn get_peer(&self, address: String) -> Box<dyn Future<Item = Option<Host>, Error = MetaDataBrokerError> + Send>;
    fn add_failure(&self, address: String, reporter_id: String) -> Box<dyn Future<Item = (), Error = MetaDataBrokerError> + Send>;
}

// Maybe we would want to support other database supporting redis protocol.
pub trait ElectionBroker {
    fn elect_node(&self, failed_node: Node) -> Box<dyn Future<Item = String , Error = ElectionBrokerError> + Send>;
}

#[derive(Debug)]
pub enum MetaDataBrokerError {
    Io(io::Error),
    InvalidReply,
}

impl fmt::Display for MetaDataBrokerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for MetaDataBrokerError {
    fn description(&self) -> &str {
        "broker error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            MetaDataBrokerError::Io(err) => Some(err),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum ElectionBrokerError {
    Io(io::Error),
    ResourceNotAvailable,
}

impl fmt::Display for ElectionBrokerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for ElectionBrokerError {
    fn description(&self) -> &str {
        "broker error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            ElectionBrokerError::Io(err) => Some(err),
            _ => None,
        }
    }
}
