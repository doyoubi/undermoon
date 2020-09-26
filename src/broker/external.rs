use super::store::MetaStore;
use crate::broker::storage::MetaStorage;
use crate::broker::MetaStoreError;
use arc_swap::ArcSwap;
use futures::{future, Future};
use std::pin::Pin;
use std::sync::Arc;

const EXTERNAL_HTTP_STORE_API_PATH: &'static str = "/api/v1/store";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExternalStore {
    pub store: MetaStore,
    // Just like the `ResourceVersion` in kubernetes,
    // it can only be used in `equal` operation.
    pub version: String,
}

pub struct ExternalHttpStorage {
    http_service_address: String,
    client: reqwest::Client,

    // Not protected by `version`,
    // but should be acceptable for server proxies.
    cached_store: ArcSwap<MetaStore>,
}

impl ExternalHttpStorage {
    pub fn new(http_service_address: String, enable_ordered_proxy: bool) -> Self {
        Self {
            http_service_address,
            client: reqwest::Client::new(),
            cached_store: ArcSwap::new(Arc::new(MetaStore::new(enable_ordered_proxy))),
        }
    }

    fn gen_url(&self) -> String {
        format!(
            "http://{}{}",
            self.http_service_address, EXTERNAL_HTTP_STORE_API_PATH
        )
    }

    async fn get_external_store(&self) -> Result<ExternalStore, MetaStoreError> {
        // GET /api/v1/store
        // Response:
        //     HTTP 200: ExternalStore json
        let url = self.gen_url();
        let response = self.client.get(&url).send().await.map_err(|e| {
            error!("Failed to get external http store {:?}", e);
            MetaStoreError::External
        })?;

        let status = response.status();

        if status.is_success() {
            let store: ExternalStore = response.json().await.map_err(|e| {
                error!("Failed to get json payload {:?}", e);
                MetaStoreError::External
            })?;
            Ok(store)
        } else {
            error!(
                "get_external_store: Failed to get store from external http service: http code {:?}",
                status
            );
            let result = response.text().await;
            match result {
                Ok(body) => {
                    error!("get_external_store: Error body: {:?}", body);
                    Err(MetaStoreError::External)
                }
                Err(e) => {
                    error!("get_external_store: Failed to get body: {:?}", e);
                    Err(MetaStoreError::External)
                }
            }
        }
    }

    async fn get_external_store_and_refresh_cache(&self) -> Result<ExternalStore, MetaStoreError> {
        let external_store = self.get_external_store().await?;
        self.cached_store
            .swap(Arc::new(external_store.store.clone()));
        Ok(external_store)
    }

    async fn update_external_store(
        &self,
        external_store: ExternalStore,
    ) -> Result<(), MetaStoreError> {
        // PUT /api/v1/store
        // Request: ExternalStore json
        // Response:
        //     HTTP 200 for success
        //     HTTP 400 for version conflict
        let url = self.gen_url();
        let response = self
            .client
            .put(&url)
            .json(&external_store)
            .send()
            .await
            .map_err(|e| {
                error!("Failed to update external http store {:?}", e);
                MetaStoreError::External
            })?;

        let status = response.status();

        if status.is_success() {
            Ok(())
        } else if status == reqwest::StatusCode::BAD_REQUEST {
            // The external http service should perform version check
            // for consistency.
            error!("update_external_store get conflict version");
            Err(MetaStoreError::Retry)
        } else {
            error!(
                "update_external_store: Failed to update store to external http service: http code {:?}",
                status
            );
            let result = response.text().await;
            match result {
                Ok(body) => {
                    error!("update_external_store: Error body: {:?}", body);
                    Err(MetaStoreError::External)
                }
                Err(e) => {
                    error!("update_external_store: Failed to get body: {:?}", e);
                    Err(MetaStoreError::External)
                }
            }
        }
    }
}
