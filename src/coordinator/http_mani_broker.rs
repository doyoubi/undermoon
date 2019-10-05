use super::broker::{MetaManipulationBroker, MetaManipulationBrokerError};
use common::cluster::{Host, MigrationTaskMeta};
use common::utils::ThreadSafe;
use futures::{future, Future, Stream};
use reqwest::r#async as request_async; // async is a keyword later

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
    fn replace_proxy(
        &self,
        failed_proxy_address: String,
    ) -> Box<dyn Future<Item = Host, Error = MetaManipulationBrokerError> + Send> {
        let url = format!(
            "http://{}/api/proxies/failover/{}",
            self.broker_address, failed_proxy_address
        );
        let request = self.client.post(&url).send();
        let fut = request
            .map_err(|e| {
                error!("Failed to replace proxy {:?}", e);
                MetaManipulationBrokerError::InvalidReply
            })
            .and_then(|mut response| {
                let status = response.status();
                let fut: Box<dyn Future<Item = Host, Error = MetaManipulationBrokerError> + Send> =
                    if status.is_success() {
                        let host_fut = response.json().map_err(|e| {
                            error!("Failed to get json payload {:?}", e);
                            MetaManipulationBrokerError::InvalidReply
                        });
                        Box::new(host_fut)
                    } else {
                        error!(
                            "replace_proxy: Failed to replace node: status code {:?}",
                            status
                        );
                        let body_fut = response.into_body().collect().then(|result| match result {
                            Ok(body) => {
                                error!("replace_proxy: Error body: {:?}", body);
                                future::err(MetaManipulationBrokerError::InvalidReply)
                            }
                            Err(e) => {
                                error!("replace_proxy: Failed to get body: {:?}", e);
                                future::err(MetaManipulationBrokerError::InvalidReply)
                            }
                        });
                        Box::new(body_fut)
                    };
                fut
            });
        Box::new(fut)
    }

    fn commit_migration(
        &self,
        meta: MigrationTaskMeta,
    ) -> Box<dyn Future<Item = (), Error = MetaManipulationBrokerError> + Send> {
        let url = format!("http://{}/api/clusters/migrations", self.broker_address);
        let request_payload = meta.clone();

        let request = self.client.put(&url).json(&request_payload).send();
        let fut = request
            .map_err(|e| {
                error!("Failed to commit migration {:?}", e);
                MetaManipulationBrokerError::InvalidReply
            })
            .and_then(|response| {
                let status = response.status();
                let fut: Box<dyn Future<Item = (), Error = MetaManipulationBrokerError> + Send> =
                    if status.is_success() || status.as_u16() == 404 {
                        Box::new(future::ok(()))
                    } else {
                        error!("Failed to commit migration status code {:?}", status);
                        let body_fut = response.into_body().collect().then(|result| match result {
                            Ok(body) => {
                                error!("HttpMetaManipulationBroker::commit_migration Error body: {:?}", body);
                                future::err(MetaManipulationBrokerError::InvalidReply)
                            }
                            Err(e) => {
                                error!("HttpMetaManipulationBroker::commit_migration Failed to get body: {:?}", e);
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
