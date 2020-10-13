use super::service::MemBrokerConfig;
use super::storage::MetaStorage;
use super::store::{ClusterInfo, MetaStore, ScaleOp, NODES_PER_PROXY};
use super::MetaStoreError;
use crate::common::cluster::{Cluster, ClusterName, MigrationTaskMeta, Node, Proxy};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use futures_timer::Delay;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const EXTERNAL_HTTP_STORE_API_PATH: &str = "/api/v1/store";
const HTTP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExternalStore {
    pub store: MetaStore,
    // Just like the `ResourceVersion` in kubernetes,
    // it can only be used in `equal` operation.
    //
    // The external service may skip version check if this is None.
    pub version: Option<String>,
}

pub struct ExternalHttpStorage {
    // This name is used for the external storage
    // to differentiate different undermoon clusters.
    storage_name: String,
    storage_password: String,

    http_service_address: String,
    client: reqwest::Client,

    // Not protected by `version`,
    // but should be acceptable for server proxies.
    cached_store: ArcSwap<MetaStore>,
}

impl ExternalHttpStorage {
    pub fn new(
        storage_name: String,
        storage_password: String,
        http_service_address: String,
        enable_ordered_proxy: bool,
    ) -> Self {
        Self {
            storage_name,
            storage_password,
            http_service_address,
            client: reqwest::Client::new(),
            cached_store: ArcSwap::new(Arc::new(MetaStore::new(enable_ordered_proxy))),
        }
    }

    fn gen_url(&self) -> String {
        format!(
            "http://{}{}/{}",
            self.http_service_address, EXTERNAL_HTTP_STORE_API_PATH, self.storage_name,
        )
    }

    async fn get_external_store(&self) -> Result<ExternalStore, MetaStoreError> {
        // GET /api/v1/store
        // Response:
        //     HTTP 200: ExternalStore json
        let url = self.gen_url();
        let request = self.client.get(&url).timeout(HTTP_TIMEOUT).basic_auth(
            self.storage_name.clone(),
            Some(self.storage_password.clone()),
        );
        let response = request.send().await.map_err(|e| {
            error!("Failed to get external http store {:?}", e);
            if e.is_timeout() {
                MetaStoreError::ExternalTimeout
            } else {
                MetaStoreError::External
            }
        })?;

        let status = response.status();

        if status.is_success() {
            let store: ExternalStore = response.json().await.map_err(|e| {
                error!("Failed to get json payload {:?}", e);
                MetaStoreError::External
            })?;
            trace!("Query store {:?}", store);
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

    async fn update_external_store(
        &self,
        external_store: ExternalStore,
    ) -> Result<(), MetaStoreError> {
        // PUT /api/v1/store
        // Request: ExternalStore json
        // Response:
        //     HTTP 200 for success
        //     HTTP 409 for version conflict
        trace!("Update json {:?}", serde_json::to_string(&external_store));
        let url = self.gen_url();
        let request = self.client.put(&url).timeout(HTTP_TIMEOUT).basic_auth(
            self.storage_name.clone(),
            Some(self.storage_password.clone()),
        );
        let response = request.json(&external_store).send().await.map_err(|e| {
            error!("Failed to update external http store {:?}", e);
            if e.is_timeout() {
                MetaStoreError::ExternalTimeout
            } else {
                MetaStoreError::External
            }
        })?;

        let status = response.status();

        if status.is_success() {
            Ok(())
        } else if status == reqwest::StatusCode::CONFLICT {
            // The external http service should perform version check
            // for consistency.
            warn!("update_external_store get conflict version");
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

    async fn update_external_store_and_cache(
        &self,
        external_store: ExternalStore,
    ) -> Result<(), MetaStoreError> {
        // This method should only used for non-empty version.
        if external_store.version.is_none() {
            return Err(MetaStoreError::EmptyExternalVersion);
        }
        self.update_external_store(external_store.clone()).await?;
        self.cached_store.swap(Arc::new(external_store.store));
        Ok(())
    }

    pub async fn keep_refreshing_cache(
        &self,
        config: MemBrokerConfig,
        refresh_interval: Duration,
    ) -> ! {
        info!("try initializing the data");
        self.try_init().await;

        info!("external http storage start refreshing task");
        let failure_ttl = chrono::Duration::seconds(config.failure_ttl as i64);
        loop {
            Delay::new(refresh_interval).await;
            let ExternalStore { mut store, version } = match self.get_external_store().await {
                Ok(data) => data,
                Err(err) => {
                    error!(
                        "keep_refreshing_cache failed to get data from external store: {:?}",
                        err
                    );
                    continue;
                }
            };

            // Cleanup the failures map
            if store.cleanup_failures(failure_ttl, config.failure_quorum) {
                let res = self
                    .update_external_store_and_cache(ExternalStore { store, version })
                    .await;
                if let Err(err) = res {
                    error!("failed to refresh cache: {:?}", err);
                }
            } else {
                self.cached_store.swap(Arc::new(store));
            };
        }
    }

    async fn try_init(&self) {
        let store = self.cached_store.lease().clone();
        // The external HTTP service should ignore this None version request
        // if there're already data.
        if let Err(err) = self
            .update_external_store(ExternalStore {
                version: None,
                store,
            })
            .await
        {
            warn!("failed to set init store: {:?}", err);
        }
    }
}

#[async_trait]
impl MetaStorage for ExternalHttpStorage {
    async fn get_all_metadata(&self) -> Result<MetaStore, MetaStoreError> {
        let store = self.cached_store.lease();
        Ok(store.clone())
    }

    // This will NOT update the remote store as it does not contains `version`.
    async fn restore_metadata(&self, meta_store: MetaStore) -> Result<(), MetaStoreError> {
        // Avoid updating the store frequently.
        let store = self.cached_store.lease();
        if store.get_global_epoch() >= meta_store.get_global_epoch() {
            return Ok(());
        }

        self.cached_store.swap(Arc::new(meta_store));
        Ok(())
    }

    async fn get_cluster_names(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Result<Vec<ClusterName>, MetaStoreError> {
        let store = self.cached_store.lease();
        let names = store.get_cluster_names_with_pagination(offset, limit);
        Ok(names)
    }

    async fn get_cluster_by_name(
        &self,
        name: &str,
        migration_limit: u64,
    ) -> Result<Option<Cluster>, MetaStoreError> {
        let store = self.cached_store.lease();
        let cluster = store.get_cluster_by_name(name, migration_limit);
        Ok(cluster)
    }

    async fn get_proxy_addresses(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Result<Vec<String>, MetaStoreError> {
        let store = self.cached_store.lease();
        let addresses = store.get_proxies_with_pagination(offset, limit);
        Ok(addresses)
    }

    async fn get_proxy_by_address(
        &self,
        address: &str,
        migration_limit: u64,
    ) -> Result<Option<Proxy>, MetaStoreError> {
        let store = self.cached_store.lease();
        let proxy = store.get_proxy_by_address(address, migration_limit);
        Ok(proxy)
    }

    async fn get_failures(
        &self,
        failure_ttl: chrono::Duration,
        failure_quorum: u64,
    ) -> Result<Vec<String>, MetaStoreError> {
        let mut store = self.cached_store.lease().clone();
        // Move to periodic update `refresh_cache`
        let failures = store.get_failures(failure_ttl, failure_quorum);
        Ok(failures)
    }

    async fn add_failure(
        &self,
        address: String,
        reporter_id: String,
    ) -> Result<(), MetaStoreError> {
        let ExternalStore { mut store, version } = self.get_external_store().await?;
        store.add_failure(address, reporter_id);
        self.update_external_store_and_cache(ExternalStore { store, version })
            .await?;
        Ok(())
    }

    async fn replace_failed_proxy(
        &self,
        failed_proxy_address: String,
        migration_limit: u64,
    ) -> Result<Option<Proxy>, MetaStoreError> {
        let ExternalStore { mut store, version } = self.get_external_store().await?;
        let res = store.replace_failed_proxy(failed_proxy_address, migration_limit);
        // It may change the store even on error.
        self.update_external_store_and_cache(ExternalStore { store, version })
            .await?;
        res
    }

    async fn commit_migration(
        &self,
        task: MigrationTaskMeta,
        clear_free_nodes: bool,
    ) -> Result<(), MetaStoreError> {
        let ExternalStore { mut store, version } = self.get_external_store().await?;
        store.commit_migration(task, clear_free_nodes)?;
        self.update_external_store_and_cache(ExternalStore { store, version })
            .await?;
        Ok(())
    }

    async fn get_failed_proxies(&self) -> Result<Vec<String>, MetaStoreError> {
        let store = self.cached_store.lease();
        let failures = store.get_failed_proxies();
        Ok(failures)
    }

    async fn get_cluster_info_by_name(
        &self,
        cluster_name: &str,
        migration_limit: u64,
    ) -> Result<Option<ClusterInfo>, MetaStoreError> {
        let store = self.cached_store.lease();
        let cluster = store.get_cluster_info_by_name(cluster_name, migration_limit);
        Ok(cluster)
    }

    async fn add_cluster(
        &self,
        cluster_name: String,
        node_num: usize,
    ) -> Result<(), MetaStoreError> {
        let ExternalStore { mut store, version } = self.get_external_store().await?;
        store.add_cluster(cluster_name, node_num)?;
        self.update_external_store_and_cache(ExternalStore { store, version })
            .await?;
        Ok(())
    }

    async fn remove_cluster(&self, cluster_name: String) -> Result<(), MetaStoreError> {
        let ExternalStore { mut store, version } = self.get_external_store().await?;
        store.remove_cluster(cluster_name)?;
        self.update_external_store_and_cache(ExternalStore { store, version })
            .await?;
        Ok(())
    }

    async fn auto_add_nodes(
        &self,
        cluster_name: String,
        node_num: usize,
    ) -> Result<Vec<Node>, MetaStoreError> {
        let ExternalStore { mut store, version } = self.get_external_store().await?;
        let nodes = store.auto_add_nodes(cluster_name, node_num)?;
        self.update_external_store_and_cache(ExternalStore { store, version })
            .await?;
        Ok(nodes)
    }

    async fn auto_scale_up_nodes(
        &self,
        cluster_name: String,
        cluster_node_num: usize,
    ) -> Result<Vec<Node>, MetaStoreError> {
        let ExternalStore { mut store, version } = self.get_external_store().await?;
        let nodes = store.auto_scale_up_nodes(cluster_name, cluster_node_num)?;
        self.update_external_store_and_cache(ExternalStore { store, version })
            .await?;
        Ok(nodes)
    }

    async fn auto_delete_free_nodes(&self, cluster_name: String) -> Result<(), MetaStoreError> {
        let ExternalStore { mut store, version } = self.get_external_store().await?;
        store.auto_delete_free_nodes(cluster_name)?;
        self.update_external_store_and_cache(ExternalStore { store, version })
            .await?;
        Ok(())
    }

    async fn migrate_slots_to_scale_down(
        &self,
        cluster_name: String,
        new_node_num: usize,
    ) -> Result<(), MetaStoreError> {
        let ExternalStore { mut store, version } = self.get_external_store().await?;
        store.migrate_slots_to_scale_down(cluster_name, new_node_num)?;
        self.update_external_store_and_cache(ExternalStore { store, version })
            .await?;
        Ok(())
    }

    async fn migrate_slots(&self, cluster_name: String) -> Result<(), MetaStoreError> {
        let ExternalStore { mut store, version } = self.get_external_store().await?;
        store.migrate_slots(cluster_name)?;
        self.update_external_store_and_cache(ExternalStore { store, version })
            .await?;
        Ok(())
    }

    async fn auto_change_node_number(
        &self,
        cluster_name: String,
        expected_num: usize,
    ) -> Result<(ScaleOp, Vec<String>, u64), MetaStoreError> {
        let ExternalStore { mut store, version } = self.get_external_store().await?;
        let data = store.auto_change_node_number(cluster_name, expected_num)?;
        self.update_external_store_and_cache(ExternalStore { store, version })
            .await?;
        Ok(data)
    }

    async fn auto_scale_out_node_number(
        &self,
        cluster_name: String,
        expected_num: usize,
    ) -> Result<(), MetaStoreError> {
        let ExternalStore { mut store, version } = self.get_external_store().await?;
        store.auto_scale_out_node_number(cluster_name, expected_num)?;
        self.update_external_store_and_cache(ExternalStore { store, version })
            .await?;
        Ok(())
    }

    async fn change_config(
        &self,
        cluster_name: String,
        config: HashMap<String, String>,
    ) -> Result<(), MetaStoreError> {
        let ExternalStore { mut store, version } = self.get_external_store().await?;
        store.change_config(cluster_name, config)?;
        self.update_external_store_and_cache(ExternalStore { store, version })
            .await?;
        Ok(())
    }

    async fn balance_masters(&self, cluster_name: String) -> Result<(), MetaStoreError> {
        let ExternalStore { mut store, version } = self.get_external_store().await?;
        store.balance_masters(cluster_name)?;
        self.update_external_store_and_cache(ExternalStore { store, version })
            .await?;
        Ok(())
    }

    async fn add_proxy(
        &self,
        proxy_address: String,
        nodes: [String; NODES_PER_PROXY],
        host: Option<String>,
        index: Option<usize>,
    ) -> Result<(), MetaStoreError> {
        let ExternalStore { mut store, version } = self.get_external_store().await?;
        let res = store.add_proxy(proxy_address, nodes, host, index);
        self.update_external_store_and_cache(ExternalStore { store, version })
            .await?;
        res
    }

    async fn remove_proxy(&self, proxy_address: String) -> Result<(), MetaStoreError> {
        let ExternalStore { mut store, version } = self.get_external_store().await?;
        store.remove_proxy(proxy_address)?;
        self.update_external_store_and_cache(ExternalStore { store, version })
            .await?;
        Ok(())
    }

    async fn get_global_epoch(&self) -> Result<u64, MetaStoreError> {
        let store = self.cached_store.lease();
        let epoch = store.get_global_epoch();
        Ok(epoch)
    }

    async fn recover_epoch(&self, exsting_largest_epoch: u64) -> Result<(), MetaStoreError> {
        let ExternalStore { mut store, version } = self.get_external_store().await?;
        store.recover_epoch(exsting_largest_epoch + 1);
        self.update_external_store_and_cache(ExternalStore { store, version })
            .await?;
        Ok(())
    }

    async fn force_bump_all_epoch(&self, new_epoch: u64) -> Result<(), MetaStoreError> {
        let ExternalStore { mut store, version } = self.get_external_store().await?;
        store.force_bump_all_epoch(new_epoch)?;
        self.update_external_store_and_cache(ExternalStore { store, version })
            .await?;
        Ok(())
    }

    async fn check_metadata(&self) -> Result<Option<MetaStore>, MetaStoreError> {
        // Now this is called frequently so just get data from cache.
        let store = self.cached_store.lease();
        match store.check() {
            Ok(()) => Ok(None),
            Err(store) => Ok(Some(store)),
        }
    }
}
