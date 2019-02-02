use std::io;
use std::fmt;
use std::error::Error;
use futures::Future;
use super::cluster::{Cluster, Node, Host, Address};


pub trait MetaDataBroker {
    fn get_cluster_names(&self) -> Box<dyn Future<Item = Vec<String>, Error = MetaDataBrokerError> + Send>;
    fn get_cluster(&self, name: String) -> Box<dyn Future<Item = Cluster, Error = MetaDataBrokerError> + Send>;
    fn get_hosts(&self, name: String) -> Box<dyn Future<Item = Vec<Host>, Error = MetaDataBrokerError> + Send>;
}

// Maybe we would want to support other database supporting redis protocol.
pub trait ElectionBroker {
    fn elect_node(&self, failed_node: Node) -> Box<dyn Future<Item = Address , Error = ElectionBrokerError> + Send>;
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
