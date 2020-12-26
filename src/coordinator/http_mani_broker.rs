use super::broker::{MetaManipulationBroker, MetaManipulationBrokerError};
use super::service::BrokerAddresses;
use crate::broker::MEM_BROKER_API_VERSION;
use crate::common::cluster::{MigrationTaskMeta, Proxy};
use futures::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct HttpMetaManipulationBroker {
    broker_addresses: BrokerAddresses,
    broker_index: AtomicUsize,
    client: reqwest::Client,
}

impl HttpMetaManipulationBroker {
    pub fn new(broker_addresses: BrokerAddresses, client: reqwest::Client) -> Self {
        HttpMetaManipulationBroker {
            broker_addresses,
            broker_index: AtomicUsize::new(0),
            client,
        }
    }
}

impl HttpMetaManipulationBroker {
    fn gen_url(&self, path: &str) -> Option<String> {
        let broker_addresses = self.broker_addresses.lease();
        let num = broker_addresses.len();
        let curr_index = self.broker_index.fetch_add(1, Ordering::Relaxed);
        let broker = broker_addresses.get(curr_index % num)?;
        let url = format!("http://{}{}{}", broker, MEM_BROKER_API_VERSION, path);
        Some(url)
    }

    async fn replace_proxy_impl(
        &self,
        failed_proxy_address: String,
    ) -> Result<Option<Proxy>, MetaManipulationBrokerError> {
        let url = self
            .gen_url(&format!("/proxies/failover/{}", failed_proxy_address))
            .ok_or(MetaManipulationBrokerError::NoBroker)?;
        let response = self.client.post(&url).send().await.map_err(|e| {
            error!("Failed to replace proxy {:?}", e);
            MetaManipulationBrokerError::RequestFailed
        })?;

        let status = response.status();

        if status.is_success() {
            let ReplaceProxyResponse { proxy } = response.json().await.map_err(|e| {
                error!("Failed to get json payload {:?}", e);
                MetaManipulationBrokerError::InvalidReply
            })?;
            Ok(proxy)
        } else {
            error!(
                "replace_proxy: Failed to replace node: status code {:?}",
                status
            );
            let result = response.text().await;
            match result {
                Ok(body) => {
                    error!("replace_proxy: Error body: {:?}", body);
                    Err(MetaManipulationBrokerError::InvalidReply)
                }
                Err(e) => {
                    error!("replace_proxy: Failed to get body: {:?}", e);
                    Err(MetaManipulationBrokerError::InvalidReply)
                }
            }
        }
    }

    async fn commit_migration_impl(
        &self,
        meta: MigrationTaskMeta,
    ) -> Result<(), MetaManipulationBrokerError> {
        let url = self
            .gen_url("/clusters/migrations")
            .ok_or(MetaManipulationBrokerError::NoBroker)?;

        let response = self
            .client
            .put(&url)
            .json(&meta)
            .send()
            .await
            .map_err(|e| {
                error!("Failed to commit migration {:?}", e);
                MetaManipulationBrokerError::RequestFailed
            })?;

        let status = response.status();

        if status.is_success() || status.as_u16() == 404 {
            Ok(())
        } else {
            error!("Failed to commit migration status code {:?}", status);
            let result = response.text().await;
            match result {
                Ok(body) => {
                    error!(
                        "HttpMetaManipulationBroker::commit_migration Error body: {:?}",
                        body
                    );
                    Err(MetaManipulationBrokerError::InvalidReply)
                }
                Err(e) => {
                    error!(
                        "HttpMetaManipulationBroker::commit_migration Failed to get body: {:?}",
                        e
                    );
                    Err(MetaManipulationBrokerError::InvalidReply)
                }
            }
        }
    }
}

impl MetaManipulationBroker for HttpMetaManipulationBroker {
    fn replace_proxy<'s>(
        &'s self,
        failed_proxy_address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Proxy>, MetaManipulationBrokerError>> + Send + 's>>
    {
        Box::pin(self.replace_proxy_impl(failed_proxy_address))
    }

    fn commit_migration<'s>(
        &'s self,
        meta: MigrationTaskMeta,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaManipulationBrokerError>> + Send + 's>> {
        Box::pin(self.commit_migration_impl(meta))
    }
}

#[derive(Deserialize, Serialize)]
pub struct ReplaceProxyResponse {
    pub proxy: Option<Proxy>,
}
