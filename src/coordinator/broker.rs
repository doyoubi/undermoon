use crate::common::cluster::{Cluster, Host, MigrationTaskMeta};
use crate::common::utils::ThreadSafe;
use futures::{Future, Stream};
use std::error::Error;
use std::fmt;
use std::io;
use std::pin::Pin;

pub trait MetaDataBroker: ThreadSafe {
    fn get_cluster_names(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<String, MetaDataBrokerError>> + Send>>;
    fn get_cluster(
        &self,
        name: String,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Cluster>, MetaDataBrokerError>> + Send>>;
    fn get_host_addresses(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<String, MetaDataBrokerError>> + Send>>;
    fn get_host(
        &self,
        address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Host>, MetaDataBrokerError>> + Send>>;
    fn add_failure(
        &self,
        address: String,
        reporter_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaDataBrokerError>> + Send>>;
    fn get_failures(&self) -> Pin<Box<dyn Stream<Item = Result<String, MetaDataBrokerError>> + Send>>;
}

// Maybe we would want to support other database supporting redis protocol.
// For them, we may need to trigger other action such as migrating data.
pub trait MetaManipulationBroker: ThreadSafe {
    fn replace_proxy(
        &self,
        failed_proxy_address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Host, MetaManipulationBrokerError>> + Send>>;
    fn commit_migration(
        &self,
        meta: MigrationTaskMeta,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaManipulationBrokerError>> + Send>>;
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

    fn cause(&self) -> Option<&dyn Error> {
        match self {
            MetaDataBrokerError::Io(err) => Some(err),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum MetaManipulationBrokerError {
    Io(io::Error),
    ResourceNotAvailable,
    InvalidReply,
}

impl fmt::Display for MetaManipulationBrokerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for MetaManipulationBrokerError {
    fn description(&self) -> &str {
        "broker error"
    }

    fn cause(&self) -> Option<&dyn Error> {
        match self {
            MetaManipulationBrokerError::Io(err) => Some(err),
            _ => None,
        }
    }
}
