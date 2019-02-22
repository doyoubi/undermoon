use futures::{Future, Stream, future, stream};
use reqwest::async;
use serde_derive::Deserialize;
use ::common::cluster::{Cluster, Node, Host};
use super::broker::{MetaDataBroker, MetaDataBrokerError};

#[derive(Clone)]
pub struct HttpMetaBroker {
    broker_address: String,
    client: async::Client,
}

impl HttpMetaBroker {
    pub fn new(broker_address: String, client: async::Client) -> Self {
        HttpMetaBroker{ broker_address, client }
    }
}

impl MetaDataBroker for HttpMetaBroker {
    fn get_cluster_names(&self) -> Box<dyn Stream<Item = String, Error = MetaDataBrokerError> + Send> {
        let url = format!("http://{}/api/clusters/names", self.broker_address);
        let request = self.client.get(&url).send();
        let names_fut = request.map_err(|e| {
            println!("Failed to get cluster names {:?}", e);
            MetaDataBrokerError::InvalidReply
        }).and_then(|mut response| {
            response.json().map(|cluster_names| {
                let ClusterNamesPayload{ names } = cluster_names;
                return names;
            }).map_err(|e| {
                println!("Failed to get cluster names from json {:?}", e);
                MetaDataBrokerError::InvalidReply
            })
        });
        let s = names_fut.map(|names| {
            stream::iter(names.into_iter().map(|name| Ok(name)))
        }).flatten_stream();
        Box::new(s)
    }

    fn get_cluster(&self, name: String) -> Box<dyn Future<Item = Option<Cluster>, Error = MetaDataBrokerError> + Send> {
        let url = format!("http://{}/api/clusters/name/{}", self.broker_address, name);
        let request = self.client.get(&url).send();
        let cluster_fut = request.map_err(|e| {
            println!("Failed to get cluster {:?}", e);
            MetaDataBrokerError::InvalidReply
        }).and_then(|mut response| {
            response.json().map(|cluster_payload| {
                let ClusterPayload{ cluster } = cluster_payload;
                return cluster;
            }).map_err(|e| {
                println!("Failed to get cluster from json {:?}", e);
                MetaDataBrokerError::InvalidReply
            })
        });
        Box::new(cluster_fut)
    }

    fn get_host_addresses(&self) -> Box<dyn Stream<Item = String, Error = MetaDataBrokerError> + Send> {
        Box::new(stream::iter(vec![]))
    }
    fn get_host(&self, address: String) -> Box<dyn Future<Item = Option<Host>, Error = MetaDataBrokerError> + Send> {
        Box::new(future::ok(None))
    }
    fn add_failure(&self, address: String, reporter_id: String) -> Box<dyn Future<Item = (), Error = MetaDataBrokerError> + Send> {
        Box::new(future::ok(()))
    }
}

#[derive(Deserialize)]
struct ClusterNamesPayload {
    names: Vec<String>,
}

#[derive(Deserialize)]
struct ClusterPayload {
    cluster: Option<Cluster>,
}
