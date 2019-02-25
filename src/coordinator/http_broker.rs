use futures::{Future, Stream, future, stream};
use reqwest::async;
use serde_derive::Deserialize;
use ::common::utils::ThreadSafe;
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

impl ThreadSafe for HttpMetaBroker {}

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
            stream::iter_ok(names)
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
        let url = format!("http://{}/api/hosts/addresses", self.broker_address);
        let request = self.client.get(&url).send();
        let addresses_fut = request.map_err(|e| {
            println!("Failed to get host addresses {:?}", e);
            MetaDataBrokerError::InvalidReply
        }).and_then(|mut response| {
            response.json().map(|payload| {
                let HostAddressesPayload{ addresses } = payload;
                return addresses;
            }).map_err(|e| {
                println!("Failed to get host adddresses from json {:?}", e);
                MetaDataBrokerError::InvalidReply
            })
        });
        let s = addresses_fut.map(|addresses| {
            stream::iter_ok(addresses)
        }).flatten_stream();
        Box::new(s)
    }

    fn get_host(&self, address: String) -> Box<dyn Future<Item = Option<Host>, Error = MetaDataBrokerError> + Send> {
        let url = format!("http://{}/api/hosts/address/{}", self.broker_address, address);
        let request = self.client.get(&url).send();
        let host_fut = request.map_err(|e| {
            println!("Failed to get host {:?}", e);
            MetaDataBrokerError::InvalidReply
        }).and_then(|mut response| {
            response.json().map(|payload| {
                let HostPayload{ host } = payload;
                return host;
            }).map_err(|e| {
                println!("Failed to get host from json {:?}", e);
                MetaDataBrokerError::InvalidReply
            })
        });
        Box::new(host_fut)
    }

    fn get_peer(&self, address: String) -> Box<dyn Future<Item = Option<Host>, Error = MetaDataBrokerError> + Send> {
        // TODO: implement it
        self.get_host(address)
    }

    fn add_failure(&self, address: String, reporter_id: String) -> Box<dyn Future<Item = (), Error = MetaDataBrokerError> + Send> {
        let url = format!("http://{}/api/failures/{}/{}", self.broker_address, address, reporter_id);
        let request = self.client.post(&url).send();
        let fut = request.map_err(|e| {
            println!("Failed to add failures {:?}", e);
            MetaDataBrokerError::InvalidReply
        }).and_then(|response| {
            let status = response.status();
            let fut: Box<dyn Future<Item = (), Error = MetaDataBrokerError> + Send> = if status.is_success() {
                Box::new(future::ok(()))
            } else {
                println!("Failed to add failures: {:?}", status);
                let body_fut = response.into_body().collect().then(|result| {
                    match result {
                        Err(e) => {
                            println!("Failed to get body: {:?}", e);
                            future::err(MetaDataBrokerError::InvalidReply)
                        },
                        Ok(body) => {
                            println!("Error body: {:?}", body);
                            future::err(MetaDataBrokerError::InvalidReply)
                        },
                    }
                });
                Box::new(body_fut)
            };
            fut
        });
        Box::new(fut)
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

#[derive(Deserialize)]
struct HostAddressesPayload {
    addresses: Vec<String>,
}

#[derive(Deserialize)]
struct HostPayload {
    host: Option<Host>,
}
