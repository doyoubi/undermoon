use super::broker::{MetaManipulationBroker, MetaManipulationBrokerError};
use common::cluster::Node;
use common::utils::ThreadSafe;
use futures::{future, Future, Stream};
use reqwest::r#async as request_async; // async is a keyword later
use serde_derive::{Deserialize, Serialize};

#[derive(Clone)]
pub struct HttpMetaManipulationBroker {
    broker_address: String,
    client: request_async::Client,
}

impl HttpMetaManipulationBroker {
    pub fn new(broker_address: String, client: request_async::Client) -> Self {
        HttpMetaManipulationBroker {
            broker_address,
            client,
        }
    }
}

impl ThreadSafe for HttpMetaManipulationBroker {}

impl MetaManipulationBroker for HttpMetaManipulationBroker {
    fn replace_node(
        &self,
        cluster_epoch: u64,
        failed_node: Node,
    ) -> Box<dyn Future<Item = Node, Error = MetaManipulationBrokerError> + Send> {
        let url = format!("http://{}/api/clusters/nodes", self.broker_address);
        let request_payload = ReplaceNodePayload {
            cluster_epoch,
            node: failed_node,
        };
        let request = self.client.put(&url).json(&request_payload).send();
        let fut = request
            .map_err(|e| {
                error!("Failed to replace node {:?}", e);
                MetaManipulationBrokerError::InvalidReply
            })
            .and_then(|mut response| {
                let status = response.status();
                let fut: Box<dyn Future<Item = Node, Error = MetaManipulationBrokerError> + Send> =
                    if status.is_success() {
                        let node_fut = response.json().map_err(|e| {
                            error!("Failed to get json payload {:?}", e);
                            MetaManipulationBrokerError::InvalidReply
                        });
                        Box::new(node_fut)
                    } else {
                        error!("Failed to replace node: status code {:?}", status);
                        let body_fut = response.into_body().collect().then(|result| match result {
                            Ok(body) => {
                                error!("Error body: {:?}", body);
                                future::err(MetaManipulationBrokerError::InvalidReply)
                            }
                            Err(e) => {
                                error!("Failed to get body: {:?}", e);
                                future::err(MetaManipulationBrokerError::InvalidReply)
                            }
                        });
                        Box::new(body_fut)
                    };
                fut
            });
        Box::new(fut)
    }
}

#[derive(Serialize, Deserialize)]
struct ReplaceNodePayload {
    cluster_epoch: u64,
    node: Node,
}
