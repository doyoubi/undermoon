use super::store::{ClusterInfo, MetaStoreError};
use super::store::{MetaStore, NODES_PER_PROXY};
use crate::broker::store::ScaleOp;
use crate::common::cluster::{Cluster, ClusterName, MigrationTaskMeta, Node, Proxy};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[async_trait]
pub trait MetaStorage: Send + Sync + 'static {
    async fn get_all_metadata(&self) -> Result<MetaStore, MetaStoreError>;
    async fn restore_metadata(&self, meta_store: MetaStore) -> Result<(), MetaStoreError>;
    async fn get_cluster_names(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Result<Vec<ClusterName>, MetaStoreError>;
    async fn get_cluster_by_name(
        &self,
        name: &str,
        migration_limit: u64,
    ) -> Result<Option<Cluster>, MetaStoreError>;
    async fn get_proxy_addresses(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Result<Vec<String>, MetaStoreError>;
    async fn get_proxy_by_address(
        &self,
        address: &str,
        migration_limit: u64,
    ) -> Result<Option<Proxy>, MetaStoreError>;
    async fn get_failures(
        &self,
        failure_ttl: chrono::Duration,
        failure_quorum: u64,
    ) -> Result<Vec<String>, MetaStoreError>;
    async fn add_failure(&self, address: String, reporter_id: String)
        -> Result<(), MetaStoreError>;
    async fn replace_failed_proxy(
        &self,
        failed_proxy_address: String,
        migration_limit: u64,
    ) -> Result<Option<Proxy>, MetaStoreError>;
    async fn commit_migration(
        &self,
        task: MigrationTaskMeta,
        clear_free_nodes: bool,
    ) -> Result<(), MetaStoreError>;
    async fn get_failed_proxies(&self) -> Result<Vec<String>, MetaStoreError>;
    async fn get_cluster_info_by_name(
        &self,
        cluster_name: &str,
        migration_limit: u64,
    ) -> Result<Option<ClusterInfo>, MetaStoreError>;
    async fn add_cluster(
        &self,
        cluster_name: String,
        node_num: usize,
    ) -> Result<(), MetaStoreError>;
    async fn remove_cluster(&self, cluster_name: String) -> Result<(), MetaStoreError>;
    async fn auto_add_node(
        &self,
        cluster_name: String,
        node_num: usize,
    ) -> Result<Vec<Node>, MetaStoreError>;
    async fn auto_scale_up_nodes(
        &self,
        cluster_name: String,
        cluster_node_num: usize,
    ) -> Result<Vec<Node>, MetaStoreError>;
    async fn auto_delete_free_nodes(&self, cluster_name: String) -> Result<(), MetaStoreError>;
    async fn migrate_slots_to_scale_down(
        &self,
        cluster_name: String,
        new_node_num: usize,
    ) -> Result<(), MetaStoreError>;
    async fn migrate_slots(&self, cluster_name: String) -> Result<(), MetaStoreError>;
    #[allow(clippy::type_complexity)]
    async fn auto_change_node_number(
        &self,
        cluster_name: String,
        expected_num: usize,
    ) -> Result<(ScaleOp, Vec<String>, u64), MetaStoreError>;
    async fn auto_scale_out_node_number(
        &self,
        cluster_name: String,
        expected_num: usize,
    ) -> Result<(), MetaStoreError>;
    async fn change_config(
        &self,
        cluster_name: String,
        config: HashMap<String, String>,
    ) -> Result<(), MetaStoreError>;
    async fn balance_masters(&self, cluster_name: String) -> Result<(), MetaStoreError>;
    async fn add_proxy(
        &self,
        proxy_address: String,
        nodes: [String; NODES_PER_PROXY],
        host: Option<String>,
        index: Option<usize>,
    ) -> Result<(), MetaStoreError>;
    async fn remove_proxy(&self, proxy_address: String) -> Result<(), MetaStoreError>;
    async fn get_global_epoch(&self) -> Result<u64, MetaStoreError>;
    async fn recover_epoch(&self, exsting_largest_epoch: u64) -> Result<(), MetaStoreError>;
    async fn force_bump_all_epoch(&self, new_epoch: u64) -> Result<(), MetaStoreError>;
    async fn check_metadata(&self) -> Result<(), MetaStore>;
}

pub struct MemoryStorage {
    store: Arc<RwLock<MetaStore>>,
}

impl MemoryStorage {
    pub fn new(store: Arc<RwLock<MetaStore>>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl MetaStorage for MemoryStorage {
    async fn get_all_metadata(&self) -> Result<MetaStore, MetaStoreError> {
        let store = self
            .store
            .read()
            .expect("MemoryStorage::get_all_data")
            .clone();
        Ok(store)
    }

    async fn restore_metadata(&self, meta_store: MetaStore) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemoryStorage::restore_metadata")
            .restore(meta_store)
    }

    async fn get_cluster_names(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Result<Vec<ClusterName>, MetaStoreError> {
        let names = self
            .store
            .read()
            .expect("MemoryStorage::get_cluster_names")
            .get_cluster_names_with_pagination(offset, limit);
        Ok(names)
    }

    async fn get_cluster_by_name(
        &self,
        name: &str,
        migration_limit: u64,
    ) -> Result<Option<Cluster>, MetaStoreError> {
        let cluster = self
            .store
            .read()
            .expect("MemoryStorage::get_cluster_by_name")
            .get_cluster_by_name(name, migration_limit);
        Ok(cluster)
    }

    async fn get_proxy_addresses(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Result<Vec<String>, MetaStoreError> {
        let addresses = self
            .store
            .read()
            .expect("MemoryStorage::get_proxy_addresses")
            .get_proxies_with_pagination(offset, limit);
        Ok(addresses)
    }

    async fn get_proxy_by_address(
        &self,
        address: &str,
        migration_limit: u64,
    ) -> Result<Option<Proxy>, MetaStoreError> {
        let proxy = self
            .store
            .read()
            .expect("MemoryStorage::get_proxy_by_address")
            .get_proxy_by_address(address, migration_limit);
        Ok(proxy)
    }

    async fn get_failures(
        &self,
        failure_ttl: chrono::Duration,
        failure_quorum: u64,
    ) -> Result<Vec<String>, MetaStoreError> {
        let failures = self
            .store
            .write()
            .expect("MemoryStorage::get_failures")
            .get_failures(failure_ttl, failure_quorum);
        Ok(failures)
    }

    async fn add_failure(
        &self,
        address: String,
        reporter_id: String,
    ) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemoryStorage::add_failure")
            .add_failure(address, reporter_id);
        Ok(())
    }

    async fn replace_failed_proxy(
        &self,
        failed_proxy_address: String,
        migration_limit: u64,
    ) -> Result<Option<Proxy>, MetaStoreError> {
        self.store
            .write()
            .expect("MemoryStorage::replace_failed_node")
            .replace_failed_proxy(failed_proxy_address, migration_limit)
    }

    async fn commit_migration(
        &self,
        task: MigrationTaskMeta,
        clear_free_nodes: bool,
    ) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemoryStorage::commit_migration")
            .commit_migration(task, clear_free_nodes)
    }

    async fn get_failed_proxies(&self) -> Result<Vec<String>, MetaStoreError> {
        let failures = self
            .store
            .read()
            .expect("MemoryStorage::get_failed_proxies")
            .get_failed_proxies();
        Ok(failures)
    }

    async fn get_cluster_info_by_name(
        &self,
        cluster_name: &str,
        migration_limit: u64,
    ) -> Result<Option<ClusterInfo>, MetaStoreError> {
        let cluster = self
            .store
            .read()
            .expect("MemoryStorage::get_cluster_info_by_name")
            .get_cluster_info_by_name(cluster_name, migration_limit);
        Ok(cluster)
    }

    async fn add_cluster(
        &self,
        cluster_name: String,
        node_num: usize,
    ) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemoryStorage::add_cluster")
            .add_cluster(cluster_name, node_num)
    }

    async fn remove_cluster(&self, cluster_name: String) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemoryStorage::remove_cluster")
            .remove_cluster(cluster_name)
    }

    async fn auto_add_node(
        &self,
        cluster_name: String,
        node_num: usize,
    ) -> Result<Vec<Node>, MetaStoreError> {
        self.store
            .write()
            .expect("MemoryStorage::auto_add_node")
            .auto_add_nodes(cluster_name, node_num)
    }

    async fn auto_scale_up_nodes(
        &self,
        cluster_name: String,
        cluster_node_num: usize,
    ) -> Result<Vec<Node>, MetaStoreError> {
        self.store
            .write()
            .expect("MemoryStorage::auto_scale_up_nodes")
            .auto_scale_up_nodes(cluster_name, cluster_node_num)
    }

    async fn auto_delete_free_nodes(&self, cluster_name: String) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemoryStorage::auto_delete_free_nodes")
            .auto_delete_free_nodes(cluster_name)
    }

    async fn migrate_slots_to_scale_down(
        &self,
        cluster_name: String,
        new_node_num: usize,
    ) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemoryStorage::migrate_slots_to_scale_down")
            .migrate_slots_to_scale_down(cluster_name, new_node_num)
    }

    async fn migrate_slots(&self, cluster_name: String) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemoryStorage::migrate_slots")
            .migrate_slots(cluster_name)
    }

    #[allow(clippy::type_complexity)]
    async fn auto_change_node_number(
        &self,
        cluster_name: String,
        expected_num: usize,
    ) -> Result<(ScaleOp, Vec<String>, u64), MetaStoreError> {
        self.store
            .write()
            .expect("MemoryStorage::auto_scale_node_number")
            .auto_change_node_number(cluster_name, expected_num)
    }

    async fn auto_scale_out_node_number(
        &self,
        cluster_name: String,
        expected_num: usize,
    ) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemoryStorage::auto_scale_node_number")
            .auto_scale_out_node_number(cluster_name, expected_num)
    }

    async fn change_config(
        &self,
        cluster_name: String,
        config: HashMap<String, String>,
    ) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemoryStorage::change_config")
            .change_config(cluster_name, config)
    }

    async fn balance_masters(&self, cluster_name: String) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemoryStorage::balance_masters")
            .balance_masters(cluster_name)
    }

    async fn add_proxy(
        &self,
        proxy_address: String,
        nodes: [String; NODES_PER_PROXY],
        host: Option<String>,
        index: Option<usize>,
    ) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemoryStorage::add_proxy")
            .add_proxy(proxy_address, nodes, host, index)
    }

    async fn remove_proxy(&self, proxy_address: String) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemoryStorage::remove_proxy")
            .remove_proxy(proxy_address)
    }

    async fn get_global_epoch(&self) -> Result<u64, MetaStoreError> {
        let epoch = self
            .store
            .read()
            .expect("MemoryStorage::get_global_epoch")
            .get_global_epoch();
        Ok(epoch)
    }

    async fn recover_epoch(&self, exsting_largest_epoch: u64) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemoryStorage::recover_epoch")
            .recover_epoch(exsting_largest_epoch + 1);
        Ok(())
    }

    async fn force_bump_all_epoch(&self, new_epoch: u64) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemoryStorage::force_bump_all_epoch")
            .force_bump_all_epoch(new_epoch)
    }

    async fn check_metadata(&self) -> Result<(), MetaStore> {
        self.store
            .read()
            .expect("MemoryStorage::check_metadata")
            .check()
    }
}
