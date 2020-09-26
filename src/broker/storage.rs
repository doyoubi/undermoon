use super::store::{ClusterInfo, MetaStoreError};
use super::store::{MetaStore, NODES_PER_PROXY};
use crate::broker::store::ScaleOp;
use crate::common::cluster::{Cluster, ClusterName, MigrationTaskMeta, Node, Proxy};
use futures::{future, Future};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

pub trait MetaStorage: Send + Sync + 'static {
    fn get_all_metadata<'s>(
        &'s self,
    ) -> Pin<Box<dyn Future<Output = Result<MetaStore, MetaStoreError>> + Send + 's>>;
    fn restore_metadata<'s>(
        &'s self,
        meta_store: MetaStore,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>>;
    fn get_cluster_names<'s>(
        &'s self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ClusterName>, MetaStoreError>> + Send + 's>>;
    fn get_cluster_by_name<'s>(
        &'s self,
        name: &str,
        migration_limit: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Cluster>, MetaStoreError>> + Send + 's>>;
    fn get_proxy_addresses<'s>(
        &'s self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, MetaStoreError>> + Send + 's>>;
    fn get_proxy_by_address<'s>(
        &'s self,
        address: &str,
        migration_limit: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Proxy>, MetaStoreError>> + Send + 's>>;
    fn get_failures<'s>(
        &'s self,
        falure_ttl: chrono::Duration,
        failure_quorum: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, MetaStoreError>> + Send + 's>>;
    fn add_failure<'s>(
        &'s self,
        address: String,
        reporter_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>>;
    fn replace_failed_proxy<'s>(
        &'s self,
        failed_proxy_address: String,
        migration_limit: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Proxy>, MetaStoreError>> + Send + 's>>;
    fn commit_migration<'s>(
        &'s self,
        task: MigrationTaskMeta,
        clear_free_nodes: bool,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>>;
    fn get_failed_proxies<'s>(
        &'s self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, MetaStoreError>> + Send + 's>>;
    fn get_cluster_info_by_name<'s>(
        &'s self,
        cluster_name: &str,
        migration_limit: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ClusterInfo>, MetaStoreError>> + Send + 's>>;
    fn add_cluster<'s>(
        &'s self,
        cluster_name: String,
        node_num: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>>;
    fn remove_cluster<'s>(
        &'s self,
        cluster_name: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>>;
    fn auto_add_node<'s>(
        &'s self,
        cluster_name: String,
        node_num: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Node>, MetaStoreError>> + Send + 's>>;
    fn auto_scale_up_nodes<'s>(
        &'s self,
        cluster_name: String,
        cluster_node_num: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Node>, MetaStoreError>> + Send + 's>>;
    fn auto_delete_free_nodes<'s>(
        &'s self,
        cluster_name: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>>;
    fn migrate_slots_to_scale_down<'s>(
        &'s self,
        cluster_name: String,
        new_node_num: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>>;
    fn migrate_slots<'s>(
        &'s self,
        cluster_name: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>>;
    #[allow(clippy::type_complexity)]
    fn auto_change_node_number<'s>(
        &'s self,
        cluster_name: String,
        expected_num: usize,
    ) -> Pin<
        Box<dyn Future<Output = Result<(ScaleOp, Vec<String>, u64), MetaStoreError>> + Send + 's>,
    >;
    fn auto_scale_out_node_number<'s>(
        &'s self,
        cluster_name: String,
        expected_num: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>>;
    fn change_config<'s>(
        &'s self,
        cluster_name: String,
        config: HashMap<String, String>,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>>;
    fn balance_masters<'s>(
        &'s self,
        cluster_name: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>>;
    fn add_proxy<'s>(
        &'s self,
        proxy_address: String,
        nodes: [String; NODES_PER_PROXY],
        host: Option<String>,
        index: Option<usize>,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>>;
    fn remove_proxy<'s>(
        &'s self,
        proxy_address: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>>;
    fn get_global_epoch<'s>(
        &'s self,
    ) -> Pin<Box<dyn Future<Output = Result<u64, MetaStoreError>> + Send + 's>>;
    fn recover_epoch<'s>(
        &'s self,
        exsting_largest_epoch: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>>;
    fn force_bump_all_epoch<'s>(
        &'s self,
        new_epoch: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>>;
    fn check_metadata<'s>(
        &'s self,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStore>> + Send + 's>>;
}

pub struct MemoryStorage {
    store: Arc<RwLock<MetaStore>>,
}

impl MemoryStorage {
    pub fn new(store: Arc<RwLock<MetaStore>>) -> Self {
        Self { store }
    }
}

impl MetaStorage for MemoryStorage {
    fn get_all_metadata<'s>(
        &'s self,
    ) -> Pin<Box<dyn Future<Output = Result<MetaStore, MetaStoreError>> + Send + 's>> {
        let res = self
            .store
            .read()
            .expect("MemoryStorage::get_all_data")
            .clone();
        Box::pin(future::ok(res))
    }

    fn restore_metadata<'s>(
        &'s self,
        meta_store: MetaStore,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>> {
        let res = self
            .store
            .write()
            .expect("MemoryStorage::restore_metadata")
            .restore(meta_store);
        Box::pin(future::ready(res))
    }

    fn get_cluster_names<'s>(
        &'s self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ClusterName>, MetaStoreError>> + Send + 's>> {
        let names = self
            .store
            .read()
            .expect("MemoryStorage::get_cluster_names")
            .get_cluster_names_with_pagination(offset, limit);
        Box::pin(future::ok(names))
    }

    fn get_cluster_by_name<'s>(
        &'s self,
        name: &str,
        migration_limit: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Cluster>, MetaStoreError>> + Send + 's>> {
        let cluster = self
            .store
            .read()
            .expect("MemoryStorage::get_cluster_by_name")
            .get_cluster_by_name(name, migration_limit);
        Box::pin(future::ok(cluster))
    }

    fn get_proxy_addresses<'s>(
        &'s self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, MetaStoreError>> + Send + 's>> {
        let addresses = self
            .store
            .read()
            .expect("MemoryStorage::get_proxy_addresses")
            .get_proxies_with_pagination(offset, limit);
        Box::pin(future::ok(addresses))
    }

    fn get_proxy_by_address<'s>(
        &'s self,
        address: &str,
        migration_limit: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Proxy>, MetaStoreError>> + Send + 's>> {
        let proxy = self
            .store
            .read()
            .expect("MemoryStorage::get_proxy_by_address")
            .get_proxy_by_address(address, migration_limit);
        Box::pin(future::ok(proxy))
    }

    fn get_failures<'s>(
        &'s self,
        failure_ttl: chrono::Duration,
        failure_quorum: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, MetaStoreError>> + Send + 's>> {
        let failures = self
            .store
            .write()
            .expect("MemoryStorage::get_failures")
            .get_failures(failure_ttl, failure_quorum);
        Box::pin(future::ok(failures))
    }

    fn add_failure<'s>(
        &'s self,
        address: String,
        reporter_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>> {
        self.store
            .write()
            .expect("MemoryStorage::add_failure")
            .add_failure(address, reporter_id);
        Box::pin(future::ok(()))
    }

    fn replace_failed_proxy<'s>(
        &'s self,
        failed_proxy_address: String,
        migration_limit: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Proxy>, MetaStoreError>> + Send + 's>> {
        let res = self
            .store
            .write()
            .expect("MemoryStorage::replace_failed_node")
            .replace_failed_proxy(failed_proxy_address, migration_limit);
        Box::pin(future::ready(res))
    }

    fn commit_migration<'s>(
        &'s self,
        task: MigrationTaskMeta,
        clear_free_nodes: bool,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>> {
        let res = self
            .store
            .write()
            .expect("MemoryStorage::commit_migration")
            .commit_migration(task, clear_free_nodes);
        Box::pin(future::ready(res))
    }

    fn get_failed_proxies<'s>(
        &'s self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, MetaStoreError>> + Send + 's>> {
        let failures = self
            .store
            .read()
            .expect("MemoryStorage::get_failed_proxies")
            .get_failed_proxies();
        Box::pin(future::ok(failures))
    }

    fn get_cluster_info_by_name<'s>(
        &'s self,
        cluster_name: &str,
        migration_limit: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ClusterInfo>, MetaStoreError>> + Send + 's>>
    {
        let cluster = self
            .store
            .read()
            .expect("MemoryStorage::get_cluster_info_by_name")
            .get_cluster_info_by_name(cluster_name, migration_limit);
        Box::pin(future::ok(cluster))
    }

    fn add_cluster<'s>(
        &'s self,
        cluster_name: String,
        node_num: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>> {
        let res = self
            .store
            .write()
            .expect("MemoryStorage::add_cluster")
            .add_cluster(cluster_name, node_num);
        Box::pin(future::ready(res))
    }

    fn remove_cluster<'s>(
        &'s self,
        cluster_name: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>> {
        let res = self
            .store
            .write()
            .expect("MemoryStorage::remove_cluster")
            .remove_cluster(cluster_name);
        Box::pin(future::ready(res))
    }

    fn auto_add_node<'s>(
        &'s self,
        cluster_name: String,
        node_num: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Node>, MetaStoreError>> + Send + 's>> {
        let res = self
            .store
            .write()
            .expect("MemoryStorage::auto_add_node")
            .auto_add_nodes(cluster_name, node_num);
        Box::pin(future::ready(res))
    }

    fn auto_scale_up_nodes<'s>(
        &'s self,
        cluster_name: String,
        cluster_node_num: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Node>, MetaStoreError>> + Send + 's>> {
        let res = self
            .store
            .write()
            .expect("MemoryStorage::auto_scale_up_nodes")
            .auto_scale_up_nodes(cluster_name, cluster_node_num);
        Box::pin(future::ready(res))
    }

    fn auto_delete_free_nodes<'s>(
        &'s self,
        cluster_name: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>> {
        let res = self
            .store
            .write()
            .expect("MemoryStorage::auto_delete_free_nodes")
            .auto_delete_free_nodes(cluster_name);
        Box::pin(future::ready(res))
    }

    fn migrate_slots_to_scale_down<'s>(
        &'s self,
        cluster_name: String,
        new_node_num: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>> {
        let res = self
            .store
            .write()
            .expect("MemoryStorage::migrate_slots_to_scale_down")
            .migrate_slots_to_scale_down(cluster_name, new_node_num);
        Box::pin(future::ready(res))
    }

    fn migrate_slots<'s>(
        &'s self,
        cluster_name: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>> {
        let res = self
            .store
            .write()
            .expect("MemoryStorage::migrate_slots")
            .migrate_slots(cluster_name);
        Box::pin(future::ready(res))
    }

    #[allow(clippy::type_complexity)]
    fn auto_change_node_number<'s>(
        &'s self,
        cluster_name: String,
        expected_num: usize,
    ) -> Pin<
        Box<dyn Future<Output = Result<(ScaleOp, Vec<String>, u64), MetaStoreError>> + Send + 's>,
    > {
        let res = self
            .store
            .write()
            .expect("MemoryStorage::auto_scale_node_number")
            .auto_change_node_number(cluster_name, expected_num);
        Box::pin(future::ready(res))
    }

    fn auto_scale_out_node_number<'s>(
        &'s self,
        cluster_name: String,
        expected_num: usize,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>> {
        let res = self
            .store
            .write()
            .expect("MemoryStorage::auto_scale_node_number")
            .auto_scale_out_node_number(cluster_name, expected_num);
        Box::pin(future::ready(res))
    }

    fn change_config<'s>(
        &'s self,
        cluster_name: String,
        config: HashMap<String, String>,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>> {
        let res = self
            .store
            .write()
            .expect("MemoryStorage::change_config")
            .change_config(cluster_name, config);
        Box::pin(future::ready(res))
    }

    fn balance_masters<'s>(
        &'s self,
        cluster_name: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>> {
        let res = self
            .store
            .write()
            .expect("MemoryStorage::balance_masters")
            .balance_masters(cluster_name);
        Box::pin(future::ready(res))
    }

    fn add_proxy<'s>(
        &'s self,
        proxy_address: String,
        nodes: [String; NODES_PER_PROXY],
        host: Option<String>,
        index: Option<usize>,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>> {
        let res = self
            .store
            .write()
            .expect("MemoryStorage::add_proxy")
            .add_proxy(proxy_address, nodes, host, index);
        Box::pin(future::ready(res))
    }

    fn remove_proxy<'s>(
        &'s self,
        proxy_address: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>> {
        let res = self
            .store
            .write()
            .expect("MemoryStorage::remove_proxy")
            .remove_proxy(proxy_address);
        Box::pin(future::ready(res))
    }

    fn get_global_epoch<'s>(
        &'s self,
    ) -> Pin<Box<dyn Future<Output = Result<u64, MetaStoreError>> + Send + 's>> {
        let epoch = self
            .store
            .read()
            .expect("MemoryStorage::get_global_epoch")
            .get_global_epoch();
        Box::pin(future::ok(epoch))
    }

    fn recover_epoch<'s>(
        &'s self,
        exsting_largest_epoch: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>> {
        self.store
            .write()
            .expect("MemoryStorage::recover_epoch")
            .recover_epoch(exsting_largest_epoch + 1);
        Box::pin(future::ok(()))
    }

    fn force_bump_all_epoch<'s>(
        &'s self,
        new_epoch: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStoreError>> + Send + 's>> {
        let res = self
            .store
            .write()
            .expect("MemoryStorage::force_bump_all_epoch")
            .force_bump_all_epoch(new_epoch);
        Box::pin(future::ready(res))
    }

    fn check_metadata<'s>(
        &'s self,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaStore>> + Send + 's>> {
        let res = self
            .store
            .read()
            .expect("MemoryStorage::check_metadata")
            .check();
        Box::pin(future::ready(res))
    }
}
