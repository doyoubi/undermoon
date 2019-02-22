use std::collections::HashMap;
use std::sync::Arc;
use arc_swap::ArcSwap;
use ::common::cluster::{Node, Cluster, Host};

pub struct CachedFullMetaData {
    full_meta_data: ArcSwap<FullMetaData>,
}

impl CachedFullMetaData {
    fn new() -> Self {
        let meta = FullMetaData::new(HashMap::new(), HashMap::new());
        CachedFullMetaData{
            full_meta_data: ArcSwap::from(Arc::new(meta)),
        }
    }
}

// The data in `clusters` and `hosts` are likely to be inconsistent
// because they are retrieved by different read operations.
// Even the data in `clusters` and `hosts` could also be inconsistent too
// depending on the implementation of the brokers.
pub struct FullMetaData {
    clusters: HashMap<String, Cluster>,
    hosts: HashMap<String, Host>,
}

impl FullMetaData {
    fn new(clusters: HashMap<String, Cluster>, hosts: HashMap<String, Host>) -> Self {
        FullMetaData{
            hosts,
            clusters: HashMap::new(),  // TODO: construct clusters from hosts
        }
    }
}
