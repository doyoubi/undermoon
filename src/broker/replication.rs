use super::persistence::MetaSyncError;
use super::service::ReplicaAddresses;
use super::store::MetaStore;
use crate::broker::MEM_BROKER_API_VERSION;
use futures::{future, Future};
use std::pin::Pin;
use std::sync::Arc;

pub trait MetaReplicator {
    fn sync_meta<'s>(
        &'s self,
        store: Arc<MetaStore>,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaSyncError>> + Send + 's>>;
}

pub struct JsonMetaReplicator {
    replica_addresses: ReplicaAddresses,
    client: reqwest::Client,
}

impl JsonMetaReplicator {
    pub fn new(replica_addresses: ReplicaAddresses, client: reqwest::Client) -> Self {
        Self {
            replica_addresses,
            client,
        }
    }

    fn gen_url(address: &str) -> String {
        format!("http://{}/api/{}/metadata", address, MEM_BROKER_API_VERSION,)
    }

    async fn sync_one_replica(
        &self,
        meta_store: Arc<MetaStore>,
        replica_address: &str,
    ) -> Result<(), MetaSyncError> {
        let url = Self::gen_url(replica_address);

        let response = self
            .client
            .put(&url)
            .json(&(*meta_store))
            .send()
            .await
            .map_err(|e| {
                error!("Failed to sync meta to replica {} {}", replica_address, e);
                MetaSyncError::Replication
            })?;

        let status = response.status();

        if !status.is_success() {
            error!("Failed to sync meta to replica: status code {:?}", status);
            let result = response.text().await;
            match result {
                Ok(body) => {
                    error!("Failed to sync meta to replica: Error body: {:?}", body);
                }
                Err(e) => {
                    error!(
                        "Failed to sync meta to replica: Failed to get body: {:?}",
                        e
                    );
                }
            }
            return Err(MetaSyncError::Replication);
        }
        Ok(())
    }

    async fn sync_meta_impl(&self, store: Arc<MetaStore>) -> Result<(), MetaSyncError> {
        let replica_addresses = self.replica_addresses.lease();
        let futs = replica_addresses
            .iter()
            .map(|address| self.sync_one_replica(store.clone(), address.as_str()))
            .collect::<Vec<_>>();
        let res_list = future::join_all(futs).await;
        for res in res_list.into_iter() {
            res?;
        }
        Ok(())
    }
}

impl MetaReplicator for JsonMetaReplicator {
    fn sync_meta<'s>(
        &'s self,
        store: Arc<MetaStore>,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaSyncError>> + Send + 's>> {
        Box::pin(self.sync_meta_impl(store))
    }
}
