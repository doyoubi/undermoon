use super::broker::{MetaDataBroker, MetaDataBrokerError};
use common::cluster::{Cluster, Host, Node, ReplMeta, Role};
use common::utils::ThreadSafe;
use futures::{future, stream, Future, Stream};
use itertools::Itertools;
use reqwest::r#async as request_async; // async is a keyword later
use serde_derive::Deserialize;
use std::collections::HashSet;

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
        let url = format!("http://{}/api/clusters/name/{}", self.broker_address, name);
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
        let url = format!("http://{}/api/hosts/addresses", self.broker_address);
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
            "http://{}/api/hosts/address/{}",
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

    fn get_peer(
        &self,
        address: String,
    ) -> Box<dyn Future<Item = Option<Host>, Error = MetaDataBrokerError> + Send> {
        debug!("get peer {}", address);
        let address_clone1 = address.clone();
        let self_clone = self.clone();
        let host_fut = self.get_host(address).and_then(move |host| -> Box<dyn Future<Item = Option<Host>, Error = MetaDataBrokerError> + Send> {
            let (epoch, clusters) = match host {
                None => return Box::new(future::ok(None)),
                Some(host) => (
                    host.get_epoch(),
                    host.get_nodes().iter()
                        .filter(|node| node.get_role() == Role::Master)
                        .map(|node| node.get_cluster_name().clone()).collect::<HashSet<String>>()
                ),
            };

            let s = stream::iter_ok(clusters);
            let f = s.map(move |cluster_name| self_clone.get_cluster(cluster_name))
                .buffer_unordered(1000)  // try buffering all
                .skip_while(|cluster| future::ok(cluster.is_none())).map(Option::unwrap)
                .map(|cluster| {
                    let cluster_name = cluster.get_name().clone();
                    // Ignore replicas
                    cluster.into_nodes()
                        .into_iter()
                        .filter(|n| n.get_role() == Role::Master)
                        .group_by(|node| node.get_proxy_address().clone()).into_iter()
                        .map(|(proxy_address, nodes)| {
                            // Collect all slots from masters.
                            let slots = nodes.map(Node::into_slots).flatten().collect();
                            // proxy as node
                            let repl_meta = ReplMeta::new(Role::Master, vec![]);
                            Node::new(proxy_address.clone(), proxy_address, cluster_name.clone(), slots, repl_meta)
                        })
                        .collect::<Vec<Node>>()
                })
                .collect()
                .map(move |nested_nodes| {
                    let address_clone2 = address_clone1.clone();
                    let nodes = nested_nodes.into_iter()
                        .flatten()
                        .filter(move |node| {
                            node.get_proxy_address().ne(&address_clone1)
                        })
                        .collect::<Vec<Node>>();
                    debug!("get peer meta {} {} {:?}", address_clone2, epoch, nodes);
                    Host::new(address_clone2, epoch, nodes)
                });
            Box::new(f.map(Some))
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

#[derive(Deserialize)]
struct FailuresPayload {
    addresses: Vec<String>,
}
