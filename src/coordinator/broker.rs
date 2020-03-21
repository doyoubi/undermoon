use crate::common::cluster::{Cluster, ClusterName, MigrationTaskMeta, Proxy};
use crate::common::utils::ThreadSafe;
use futures::{Future, Stream};
use mockall::automock;
use std::error::Error;
use std::fmt;
use std::io;
use std::pin::Pin;

// Clippy accidentally thinks [automock] is an index expression.
#[allow(clippy::indexing_slicing)]
mod trait_mod {
    use super::*;

    // To support large result set, return Stream here in some APIs.
    #[automock]
    pub trait MetaDataBroker: ThreadSafe {
        fn get_cluster_names<'s>(
            &'s self,
        ) -> Pin<Box<dyn Stream<Item = Result<ClusterName, MetaDataBrokerError>> + Send + 's>>;

        fn get_cluster<'s>(
            &'s self,
            name: ClusterName,
        ) -> Pin<Box<dyn Future<Output = Result<Option<Cluster>, MetaDataBrokerError>> + Send + 's>>;

        fn get_proxy_addresses<'s>(
            &'s self,
        ) -> Pin<Box<dyn Stream<Item = Result<String, MetaDataBrokerError>> + Send + 's>>;

        fn get_proxy<'s>(
            &'s self,
            address: String,
        ) -> Pin<Box<dyn Future<Output = Result<Option<Proxy>, MetaDataBrokerError>> + Send + 's>>;

        fn add_failure<'s>(
            &'s self,
            address: String,
            reporter_id: String,
        ) -> Pin<Box<dyn Future<Output = Result<(), MetaDataBrokerError>> + Send + 's>>;

        fn get_failures<'s>(
            &'s self,
        ) -> Pin<Box<dyn Stream<Item = Result<String, MetaDataBrokerError>> + Send + 's>>;
    }

    // Maybe we would want to support other database supporting redis protocol.
    // For them, we may need to trigger other action such as migrating data.
    #[automock]
    pub trait MetaManipulationBroker: ThreadSafe {
        fn replace_proxy<'s>(
            &'s self,
            failed_proxy_address: String,
        ) -> Pin<Box<dyn Future<Output = Result<Proxy, MetaManipulationBrokerError>> + Send + 's>>;

        fn commit_migration<'s>(
            &'s self,
            meta: MigrationTaskMeta,
        ) -> Pin<Box<dyn Future<Output = Result<(), MetaManipulationBrokerError>> + Send + 's>>;
    }
}

pub use self::trait_mod::{
    MetaDataBroker, MetaManipulationBroker, MockMetaDataBroker, MockMetaManipulationBroker,
};

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
