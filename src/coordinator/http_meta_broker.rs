use super::broker::{MetaDataBroker, MetaDataBrokerError};
use crate::common::cluster::{Cluster, Host};
use crate::common::utils::ThreadSafe;
use futures01::{future, stream, Future, Stream};
use reqwest::r#async as request_async; // async is a keyword later
use serde_derive::Deserialize;

#[derive(Clone)]
pub struct HttpMetaBroker {
    broker_address: String,
    client: request_async::Client,
}

impl HttpMetaBroker {
    pub fn new(broker_address: String, client: request_async::Client) -> Self {
        HttpMetaBroker {
            broker_address,
            client,
        }
    }
}

impl ThreadSafe for HttpMetaBroker {}

impl MetaDataBroker for HttpMetaBroker {
    fn get_cluster_names(
        &self,
    ) -> Box<dyn Stream<Item = String, Error = MetaDataBrokerError> + Send> {
        let url = format!("http://{}/api/clusters/names", self.broker_address);
        let request = self.client.get(&url).send();
        let names_fut = request
            .map_err(|e| {
                error!("failed to get cluster names {:?}", e);
                MetaDataBrokerError::InvalidReply
            })
            .and_then(|mut response| {
                response
                    .json()
                    .map(|cluster_names| {
                        let ClusterNamesPayload { names } = cluster_names;
                        names
                    })
                    .map_err(|e| {
                        error!("failed to get cluster names from json {:?}", e);
                        MetaDataBrokerError::InvalidReply
                    })
            });
        let s = names_fut.map(stream::iter_ok).flatten_stream();
        Box::new(s)
    }

    fn get_cluster(
        &self,
        name: String,
    ) -> Box<dyn Future<Item = Option<Cluster>, Error = MetaDataBrokerError> + Send> {
        let url = format!("http://{}/api/clusters/meta/{}", self.broker_address, name);
        let request = self.client.get(&url).send();
        let cluster_fut = request
            .map_err(|e| {
                error!("failed to get cluster {:?}", e);
                MetaDataBrokerError::InvalidReply
            })
            .and_then(|mut response| {
                response
                    .json()
                    .map(|cluster_payload| {
                        let ClusterPayload { cluster } = cluster_payload;
                        cluster
                    })
                    .map_err(|e| {
                        error!("failed to get cluster from json {:?}", e);
                        MetaDataBrokerError::InvalidReply
                    })
            });
        Box::new(cluster_fut)
    }

    fn get_host_addresses(
        &self,
    ) -> Box<dyn Stream<Item = String, Error = MetaDataBrokerError> + Send> {
        let url = format!("http://{}/api/proxies/addresses", self.broker_address);
        let request = self.client.get(&url).send();
        let addresses_fut = request
            .map_err(|e| {
                error!("failed to get host addresses {:?}", e);
                MetaDataBrokerError::InvalidReply
            })
            .and_then(|mut response| {
                response
                    .json()
                    .map(|payload| {
                        let HostAddressesPayload { addresses } = payload;
                        addresses
                    })
                    .map_err(|e| {
                        error!("failed to get host adddresses from json {:?}", e);
                        MetaDataBrokerError::InvalidReply
                    })
            });
        let s = addresses_fut.map(stream::iter_ok).flatten_stream();
        Box::new(s)
    }

    fn get_host(
        &self,
        address: String,
    ) -> Box<dyn Future<Item = Option<Host>, Error = MetaDataBrokerError> + Send> {
        let url = format!(
            "http://{}/api/proxies/meta/{}",
            self.broker_address, address
        );
        let request = self.client.get(&url).send();
        let host_fut = request
            .map_err(|e| {
                error!("failed to get host {:?}", e);
                MetaDataBrokerError::InvalidReply
            })
            .and_then(|mut response| {
                response
                    .json()
                    .map(|payload| {
                        let HostPayload { host } = payload;
                        host
                    })
                    .map_err(move |e| {
                        error!("failed to get host {} from json {:?}", address, e);
                        MetaDataBrokerError::InvalidReply
                    })
            });
        Box::new(host_fut)
    }

    fn add_failure(
        &self,
        address: String,
        reporter_id: String,
    ) -> Box<dyn Future<Item = (), Error = MetaDataBrokerError> + Send> {
        let url = format!(
            "http://{}/api/failures/{}/{}",
            self.broker_address, address, reporter_id
        );
        let request = self.client.post(&url).send();
        let fut = request
            .map_err(|e| {
                error!("failed to add failures {:?}", e);
                MetaDataBrokerError::InvalidReply
            })
            .and_then(|response| {
                let status = response.status();
                let fut: Box<dyn Future<Item = (), Error = MetaDataBrokerError> + Send> =
                    if status.is_success() {
                        Box::new(future::ok(()))
                    } else {
                        let body_fut = response.into_body().collect().then(|result| match result {
                            Err(e) => {
                                error!("Failed to get body: {:?}", e);
                                future::err(MetaDataBrokerError::InvalidReply)
                            }
                            Ok(body) => {
                                error!("Error body: {:?}", body);
                                future::err(MetaDataBrokerError::InvalidReply)
                            }
                        });
                        Box::new(body_fut)
                    };
                fut
            });
        Box::new(fut)
    }

    fn get_failures(&self) -> Box<dyn Stream<Item = String, Error = MetaDataBrokerError> + Send> {
        let url = format!("http://{}/api/failures", self.broker_address);
        let request = self.client.get(&url).send();
        let addresses_fut = request
            .map_err(|e| {
                error!("Failed to get failures {:?}", e);
                MetaDataBrokerError::InvalidReply
            })
            .and_then(|mut response| {
                response
                    .json()
                    .map(|failures| {
                        let FailuresPayload { addresses } = failures;
                        addresses
                    })
                    .map_err(|e| {
                        error!("Failed to get cluster names from json {:?}", e);
                        MetaDataBrokerError::InvalidReply
                    })
            });
        let s = addresses_fut.map(stream::iter_ok).flatten_stream();
        Box::new(s)
    }
}

#[derive(Deserialize, Serialize)]
pub struct ClusterNamesPayload {
    pub names: Vec<String>,
}

#[derive(Deserialize, Serialize)]
pub struct ClusterPayload {
    pub cluster: Option<Cluster>,
}

#[derive(Deserialize, Serialize)]
pub struct HostAddressesPayload {
    pub addresses: Vec<String>,
}

#[derive(Deserialize, Serialize)]
pub struct HostPayload {
    pub host: Option<Host>,
}

#[derive(Deserialize, Serialize)]
pub struct FailuresPayload {
    pub addresses: Vec<String>,
}
