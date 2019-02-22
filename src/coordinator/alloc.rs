use std::io;
use std::fmt;
use std::error::Error;
use futures::Future;
use ::common::cluster::Node;
use super::broker::{ElectionBroker, ElectionBrokerError};

pub trait NodeAllocator {
    fn allocate(&self, cluster_name: String) -> Box<dyn Future<Item = String, Error = AllocateError> + Send>;
}

impl<T> ElectionBroker for T where T: NodeAllocator {
    fn elect_node(&self, failed_node: Node) -> Box<dyn Future<Item = String , Error = ElectionBrokerError> + Send> {
        let fut = self.allocate(failed_node.get_cluster_name().clone())
            .map_err(|e| {
                match e {
                    AllocateError::Io(e) => ElectionBrokerError::Io(e),
                    AllocateError::ResourceNotAvailable => ElectionBrokerError::ResourceNotAvailable,
                }
            });
        Box::new(fut)
    }
}

#[derive(Debug)]
pub enum AllocateError {
    Io(io::Error),
    ResourceNotAvailable,
}

impl fmt::Display for AllocateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for AllocateError {
    fn description(&self) -> &str {
        "heartbeat error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            AllocateError::Io(err) => Some(err),
            _ => None,
        }
    }
}
