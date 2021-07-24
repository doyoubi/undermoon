use super::persistence::MetaPersistence;
use super::replication::MetaReplicator;
use super::resource::ResourceChecker;
use super::storage::{MemoryStorage, MetaStorage};
use super::store::{ClusterInfo, MetaStore, MetaStoreError, ScaleOp, CHUNK_HALF_NODE_NUM};
use crate::broker::epoch::{fetch_max_epoch, wait_for_proxy_epoch, EpochFetchResult};
use crate::broker::external::ExternalHttpStorage;
use crate::common::atomic_lock::AtomicLock;
use crate::common::cluster::{Cluster, ClusterName, MigrationTaskMeta, Node, Proxy};
use crate::common::config::ClusterConfig;
use crate::common::version::UNDERMOON_VERSION;
use crate::coordinator::http_mani_broker::ReplaceProxyResponse;
use crate::coordinator::http_meta_broker::{
    ClusterNamesPayload, ClusterPayload, FailedProxiesPayload, FailuresPayload,
    ProxyAddressesPayload, ProxyPayload,
};
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::convert::Infallible;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;
use warp::{http, Filter};

pub const MEM_BROKER_API_VERSION: &str = "v3";

pub async fn run_server(service: Arc<MemBrokerService>, address: std::net::SocketAddr) {
    let svc = warp::any().map(move || service.clone());
    let logger = warp::log::custom(|info| {
        if *info.method() == http::Method::GET {
            return;
        }
        info!(
            target: "mem_broker",
            "{} \"{} {} {:?}\" {} {:?}",
            info.remote_addr().map(|addr| addr.to_string()).unwrap_or_default(),
            info.method(),
            info.path(),
            info.version(),
            info.status().as_u16(),
            info.elapsed(),
        );
    });

    let get_version_hdl = warp::get().and(warp::path("version")).map(get_version);

    let get_metadata_hdl = warp::get()
        .and(warp::path("metadata"))
        .and(svc.clone())
        .and_then(get_all_metadata)
        .with(warp::compression::gzip());

    let put_metadata_hdl = warp::put()
        .and(warp::path("metadata"))
        .and(warp::body::json())
        .and(svc.clone())
        .and_then(restore_metadata);

    let get_cluster_names_hdl = warp::get()
        .and(warp::path!("clusters" / "names"))
        .and(warp::query::<Pagination>())
        .and(svc.clone())
        .and_then(get_cluster_names);

    let get_cluster_by_name_hdl = warp::get()
        .and(warp::path!("clusters" / "meta" / String))
        .and(svc.clone())
        .and_then(get_cluster_by_name)
        .with(warp::compression::gzip());

    let get_proxy_addresses_hdl = warp::get()
        .and(warp::path!("proxies" / "addresses"))
        .and(warp::query::<Pagination>())
        .and(svc.clone())
        .and_then(get_proxy_addresses);

    let get_proxy_by_address_hdl = warp::get()
        .and(warp::path!("proxies" / "meta" / String))
        .and(svc.clone())
        .and_then(get_proxy_by_address)
        .with(warp::compression::gzip());

    let get_failures_hdl = warp::get()
        .and(warp::path("failures"))
        .and(svc.clone())
        .and_then(get_failures);

    let add_failure_hdl = warp::post()
        .and(warp::path!("failures" / String / String))
        .and(svc.clone())
        .and_then(add_failure);

    let replace_failed_node_hdl = warp::post()
        .and(warp::path!("proxies" / "failover" / String))
        .and(svc.clone())
        .and_then(replace_failed_node);

    let commit_migration_hdl = warp::put()
        .and(warp::path!("clusters" / "migrations"))
        .and(warp::body::json())
        .and(svc.clone())
        .and_then(commit_migration);

    let get_failed_proxies_hdl = warp::get()
        .and(warp::path!("proxies" / "failed" / "addresses"))
        .and(svc.clone())
        .and_then(get_failed_proxies);

    let get_cluster_info_by_name_hdl = warp::get()
        .and(warp::path!("clusters" / "info" / String))
        .and(svc.clone())
        .and_then(get_cluster_info_by_name);

    let add_cluster_hdl = warp::post()
        .and(warp::path!("clusters" / "meta" / String))
        .and(warp::body::json())
        .and(svc.clone())
        .and_then(add_cluster);

    let remove_cluster_hdl = warp::delete()
        .and(warp::path!("clusters" / "meta" / String))
        .and(svc.clone())
        .and_then(remove_cluster);

    let auto_add_nodes_hdl = warp::patch()
        .and(warp::path!("clusters" / "nodes" / String))
        .and(warp::body::json())
        .and(svc.clone())
        .and_then(auto_add_nodes);

    let auto_scale_up_nodes_hdl = warp::put()
        .and(warp::path!("clusters" / "nodes" / String))
        .and(warp::body::json())
        .and(svc.clone())
        .and_then(auto_scale_up_nodes);

    let auto_delete_free_nodes_hdl = warp::delete()
        .and(warp::path!("clusters" / "free_nodes" / String))
        .and(svc.clone())
        .and_then(auto_delete_free_nodes);

    let migrate_slots_to_scale_down_hdl = warp::post()
        .and(warp::path!(
            "clusters" / "migrations" / "shrink" / String / usize
        ))
        .and(svc.clone())
        .and_then(migrate_slots_to_scale_down);

    let migrate_slots_hdl = warp::post()
        .and(warp::path!("clusters" / "migrations" / "expand" / String))
        .and(svc.clone())
        .and_then(migrate_slots);

    let auto_scale_node_number_hdl = warp::post()
        .and(warp::path!(
            "clusters" / "migrations" / "auto" / String / usize
        ))
        .and(svc.clone())
        .and_then(auto_scale_node_number);

    let change_config_hdl = warp::patch()
        .and(warp::path!("clusters" / "config" / String))
        .and(warp::body::json())
        .and(svc.clone())
        .and_then(change_config);

    let balance_masters_hdl = warp::put()
        .and(warp::path!("clusters" / "balance" / String))
        .and(svc.clone())
        .and_then(balance_masters);

    let add_proxy_hdl = warp::post()
        .and(warp::path!("proxies" / "meta"))
        .and(warp::body::json())
        .and(svc.clone())
        .and_then(add_proxy);

    let remove_proxy_hdl = warp::delete()
        .and(warp::path!("proxies" / "meta" / String))
        .and(svc.clone())
        .and_then(remove_proxy);

    let check_resource_for_failures_hdl = warp::post()
        .and(warp::path!("resources" / "failures" / "check"))
        .and(svc.clone())
        .and_then(check_resource_for_failures);

    let change_broker_config_hdl = warp::put()
        .and(warp::path("config"))
        .and(warp::body::json())
        .and(svc.clone())
        .map(change_broker_config);

    let get_broker_config_hdl = warp::get()
        .and(warp::path("config"))
        .and(svc.clone())
        .map(get_broker_config);

    let get_epoch_hdl = warp::get()
        .and(warp::path("epoch"))
        .and(svc.clone())
        .and_then(get_epoch);

    let recover_epoch_hdl = warp::put()
        .and(warp::path!("epoch" / "recovery"))
        .and(svc.clone())
        .and_then(recover_epoch);

    let bump_epoch_hdl = warp::put()
        .and(warp::path!("epoch" / u64))
        .and(svc.clone())
        .and_then(bump_epoch);

    let routes = warp::path("api")
        .and(warp::path(MEM_BROKER_API_VERSION))
        .and(
            get_version_hdl
                .or(get_metadata_hdl)
                .or(put_metadata_hdl)
                // Broker api
                .or(get_cluster_names_hdl)
                .or(get_cluster_by_name_hdl)
                .or(get_proxy_addresses_hdl)
                .or(get_proxy_by_address_hdl)
                .or(get_failures_hdl)
                .or(add_failure_hdl)
                .or(replace_failed_node_hdl)
                .or(commit_migration_hdl)
                .or(get_failed_proxies_hdl)
                // Additional api
                .or(get_cluster_info_by_name_hdl)
                .or(add_cluster_hdl)
                .or(remove_cluster_hdl)
                .or(auto_add_nodes_hdl)
                .or(auto_scale_up_nodes_hdl)
                .or(auto_delete_free_nodes_hdl)
                .or(migrate_slots_to_scale_down_hdl)
                .or(migrate_slots_hdl)
                .or(auto_scale_node_number_hdl)
                .or(change_config_hdl)
                .or(balance_masters_hdl)
                .or(add_proxy_hdl)
                .or(remove_proxy_hdl)
                .or(check_resource_for_failures_hdl)
                .or(change_broker_config_hdl)
                .or(get_broker_config_hdl)
                .or(get_epoch_hdl)
                .or(recover_epoch_hdl)
                .or(bump_epoch_hdl),
        )
        .with(logger);
    warp::serve(routes).run(address).await
}

enum WarpRes<T> {
    Json(T),
    Empty,
}

fn warp_empty_res((): ()) -> WarpRes<()> {
    WarpRes::Empty
}

fn warp_json<T: serde::Serialize>(
    r: Result<WarpRes<T>, MetaStoreError>,
) -> impl warp::reply::Reply {
    let (res, code): (Box<dyn warp::Reply>, _) = match r {
        Ok(WarpRes::Json(t)) => (Box::new(warp::reply::json(&t)), http::StatusCode::OK),
        Ok(WarpRes::Empty) => (Box::new(warp::reply::reply()), http::StatusCode::OK),
        Err(err) => (Box::new(warp::reply::json(&err)), err.status_code()),
    };
    warp::reply::with_status::<Box<dyn warp::Reply>>(res, code)
}

pub type ReplicaAddresses = Arc<ArcSwap<Vec<String>>>;

#[derive(Derivative)]
#[derivative(Debug, Clone)]
pub enum StorageConfig {
    Memory,
    ExternalHttp {
        // This name is used for the external storage
        // to differentiate different undermoon clusters.
        storage_name: String,
        #[derivative(Debug = "ignore")]
        storage_password: String,
        address: String,
        refresh_interval: Duration,
    },
}

#[derive(Debug, Clone)]
pub struct MemBrokerConfig {
    pub address: String,
    pub failure_ttl: u64, // in seconds
    pub failure_quorum: u64,
    pub migration_limit: u64,
    pub recover_from_meta_file: bool,
    pub meta_filename: String,
    pub auto_update_meta_file: bool,
    pub update_meta_file_interval: Option<NonZeroU64>,
    pub replica_addresses: ReplicaAddresses,
    pub sync_meta_interval: Option<NonZeroU64>,
    pub enable_ordered_proxy: bool,
    pub storage: StorageConfig,
    pub debug: bool,
}

impl MemBrokerConfig {
    pub fn update(&self, config_payload: MemBrokerConfigPayload) -> Result<(), MetaStoreError> {
        let MemBrokerConfigPayload { replica_addresses } = config_payload;
        self.replica_addresses.swap(Arc::new(replica_addresses));
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MemBrokerConfigPayload {
    pub replica_addresses: Vec<String>,
}

pub struct MemBrokerService {
    config: MemBrokerConfig,
    default_cluster_config: ClusterConfig,
    storage: Arc<dyn MetaStorage>,
    meta_persistence: Arc<dyn MetaPersistence + Send + Sync + 'static>,
    meta_replicator: Arc<dyn MetaReplicator + Send + Sync + 'static>,
    scale_lock: AtomicLock,
}

impl MemBrokerService {
    pub fn new(
        config: MemBrokerConfig,
        default_cluster_config: ClusterConfig,
        meta_persistence: Arc<dyn MetaPersistence + Send + Sync + 'static>,
        meta_replicator: Arc<dyn MetaReplicator + Send + Sync + 'static>,
        last_meta_store: Option<MetaStore>,
    ) -> Result<Self, MetaStoreError> {
        info!("config: {:?}", config);
        let mut meta_store = MetaStore::new(config.enable_ordered_proxy);
        if let Some(last) = last_meta_store {
            if meta_store.enable_ordered_proxy != last.enable_ordered_proxy {
                error!("The configured enable_ordered_proxy is not the same as the recovered data. Will ignore the configured one.");
            }
            info!("restore metadata");
            meta_store.restore(last)?;
        }

        let storage: Arc<dyn MetaStorage> = match config.storage.clone() {
            StorageConfig::Memory => Arc::new(MemoryStorage::new(Arc::new(
                parking_lot::RwLock::new(meta_store),
            ))),
            StorageConfig::ExternalHttp {
                storage_name,
                storage_password,
                address,
                refresh_interval,
            } => {
                let config_clone = config.clone();
                let http_storage = Arc::new(ExternalHttpStorage::new(
                    storage_name,
                    storage_password,
                    address,
                    config.enable_ordered_proxy,
                ));
                let http_storage_clone = http_storage.clone();
                tokio::spawn(async move {
                    http_storage_clone
                        .keep_refreshing_cache(config_clone, refresh_interval)
                        .await;
                });
                http_storage
            }
        };

        let service = Self {
            config,
            default_cluster_config,
            storage,
            meta_persistence,
            meta_replicator,
            scale_lock: AtomicLock::default(),
        };
        Ok(service)
    }

    async fn trigger_update(&self) -> Result<(), MetaStoreError> {
        if self.config.auto_update_meta_file {
            self.update_meta_file().await?;
        }
        Ok(())
    }

    pub async fn update_meta_file(&self) -> Result<(), MetaStoreError> {
        let store = self.storage.get_all_metadata().await?;
        self.meta_persistence
            .store(store)
            .await
            .map_err(MetaStoreError::SyncError)
    }

    pub async fn sync_meta(&self) -> Result<(), MetaStoreError> {
        if self.config.replica_addresses.lease().is_empty() {
            return Ok(());
        }
        let store = self.storage.get_all_metadata().await?;
        let store = Arc::new(store);
        self.meta_replicator
            .sync_meta(store)
            .await
            .map_err(MetaStoreError::SyncError)
    }

    pub async fn get_all_data(&self) -> Result<MetaStore, MetaStoreError> {
        self.storage.get_all_metadata().await
    }

    pub async fn restore_metadata(&self, meta_store: MetaStore) -> Result<(), MetaStoreError> {
        self.storage.restore_metadata(meta_store).await
    }

    pub async fn get_proxy_addresses(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Result<Vec<String>, MetaStoreError> {
        self.storage.get_proxy_addresses(offset, limit).await
    }

    pub async fn get_proxy_by_address(
        &self,
        address: &str,
    ) -> Result<Option<Proxy>, MetaStoreError> {
        let migration_limit = self.config.migration_limit;
        self.storage
            .get_proxy_by_address(address, migration_limit)
            .await
    }

    pub async fn get_cluster_names(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Result<Vec<ClusterName>, MetaStoreError> {
        self.storage.get_cluster_names(offset, limit).await
    }

    pub async fn get_cluster_by_name(&self, name: &str) -> Result<Option<Cluster>, MetaStoreError> {
        let migration_limit = self.config.migration_limit;
        self.storage
            .get_cluster_by_name(name, migration_limit)
            .await
    }

    pub async fn get_cluster_info_by_name(
        &self,
        name: &str,
    ) -> Result<Option<ClusterInfo>, MetaStoreError> {
        let migration_limit = self.config.migration_limit;
        self.storage
            .get_cluster_info_by_name(name, migration_limit)
            .await
    }

    pub async fn add_proxy(
        &self,
        proxy_resource: ProxyResourcePayload,
    ) -> Result<(), MetaStoreError> {
        let ProxyResourcePayload {
            proxy_address,
            nodes,
            host,
            index,
        } = proxy_resource;
        self.storage
            .add_proxy(proxy_address, nodes, host, index)
            .await
    }

    pub async fn add_cluster(
        &self,
        cluster_name: String,
        node_num: usize,
    ) -> Result<(), MetaStoreError> {
        self.storage
            .add_cluster(cluster_name, node_num, self.default_cluster_config.clone())
            .await
    }

    pub async fn remove_cluster(&self, cluster_name: String) -> Result<(), MetaStoreError> {
        self.storage.remove_cluster(cluster_name).await
    }

    pub async fn auto_add_nodes(
        &self,
        cluster_name: String,
        node_num: usize,
    ) -> Result<Vec<Node>, MetaStoreError> {
        let _guard = self
            .scale_lock
            .lock()
            .ok_or(MetaStoreError::NodeNumberChanging)?;

        self.storage.auto_add_nodes(cluster_name, node_num).await
    }

    pub async fn auto_scale_up_nodes(
        &self,
        cluster_name: String,
        cluster_node_num: usize,
    ) -> Result<Vec<Node>, MetaStoreError> {
        let _guard = self
            .scale_lock
            .lock()
            .ok_or(MetaStoreError::NodeNumberChanging)?;

        self.storage
            .auto_scale_up_nodes(cluster_name, cluster_node_num)
            .await
    }

    pub async fn auto_delete_free_nodes(&self, cluster_name: String) -> Result<(), MetaStoreError> {
        let _guard = self
            .scale_lock
            .lock()
            .ok_or(MetaStoreError::NodeNumberChanging)?;

        self.storage.auto_delete_free_nodes(cluster_name).await
    }

    pub async fn change_config(
        &self,
        cluster_name: String,
        config: HashMap<String, String>,
    ) -> Result<(), MetaStoreError> {
        self.storage.change_config(cluster_name, config).await
    }

    pub async fn balance_masters(&self, cluster_name: String) -> Result<(), MetaStoreError> {
        self.storage.balance_masters(cluster_name).await
    }

    pub async fn remove_proxy(&self, proxy_address: String) -> Result<(), MetaStoreError> {
        self.storage.remove_proxy(proxy_address).await
    }

    pub async fn check_resource_for_failures(&self) -> Result<Vec<String>, MetaStoreError> {
        let migration_limit = self.config.migration_limit;
        let store_copy = self.storage.get_all_metadata().await?;
        let checker = ResourceChecker::new(store_copy);
        checker.check_failure_tolerance(migration_limit)
    }

    pub fn change_broker_config(
        &self,
        config_payload: MemBrokerConfigPayload,
    ) -> Result<(), MetaStoreError> {
        self.config.update(config_payload)?;
        Ok(())
    }

    pub fn get_broker_config(&self) -> Result<MemBrokerConfigPayload, MetaStoreError> {
        let payload = MemBrokerConfigPayload {
            replica_addresses: (*self.config.replica_addresses.load()).clone(),
        };
        Ok(payload)
    }

    pub async fn migrate_slots(&self, cluster_name: String) -> Result<(), MetaStoreError> {
        let _guard = self
            .scale_lock
            .lock()
            .ok_or(MetaStoreError::NodeNumberChanging)?;

        self.storage.migrate_slots(cluster_name).await
    }

    pub async fn migrate_slots_to_scale_down(
        &self,
        cluster_name: String,
        new_node_num: usize,
    ) -> Result<(), MetaStoreError> {
        let _guard = self
            .scale_lock
            .lock()
            .ok_or(MetaStoreError::NodeNumberChanging)?;

        self.storage
            .migrate_slots_to_scale_down(cluster_name, new_node_num)
            .await
    }

    pub async fn auto_scale_node_number(
        &self,
        cluster_name: String,
        new_node_num: usize,
    ) -> Result<(), MetaStoreError> {
        // Since this operation consists of two phrase
        // protected by two locking phase, we need
        // another lock to prevent other scaling operation
        // between them.
        let _guard = self
            .scale_lock
            .lock()
            .ok_or(MetaStoreError::NodeNumberChanging)?;

        let (scale_op, proxy_addresses, cluster_epoch) = self
            .storage
            .auto_change_node_number(cluster_name.clone(), new_node_num)
            .await?;

        if let ScaleOp::NoOp | ScaleOp::ScaleDown = scale_op {
            return Ok(());
        }

        if let Err(failed_proxy) = wait_for_proxy_epoch(proxy_addresses, cluster_epoch).await {
            error!(
                "failed to wait for epoch sync. failed proxy: {}",
                failed_proxy
            );
            return Err(MetaStoreError::ProxyNotSync);
        }

        self.storage
            .auto_scale_out_node_number(cluster_name, new_node_num)
            .await
    }

    pub async fn get_failures(&self) -> Result<Vec<String>, MetaStoreError> {
        let failure_ttl = chrono::Duration::seconds(self.config.failure_ttl as i64);
        let failure_quorum = self.config.failure_quorum;
        self.storage.get_failures(failure_ttl, failure_quorum).await
    }

    pub async fn add_failure(
        &self,
        address: String,
        reporter_id: String,
    ) -> Result<(), MetaStoreError> {
        self.storage.add_failure(address, reporter_id).await
    }

    pub async fn commit_migration(&self, task: MigrationTaskMeta) -> Result<(), MetaStoreError> {
        // TODO: Maybe we need to make `clear_free_nodes` of `commit_migration` configurable.
        self.storage.commit_migration(task, false).await
    }

    pub async fn replace_failed_proxy(
        &self,
        failed_proxy_address: String,
    ) -> Result<Option<Proxy>, MetaStoreError> {
        let migration_limit = self.config.migration_limit;
        self.storage
            .replace_failed_proxy(failed_proxy_address, migration_limit)
            .await
    }

    pub async fn get_failed_proxies(&self) -> Result<Vec<String>, MetaStoreError> {
        self.storage.get_failed_proxies().await
    }

    pub async fn force_bump_all_epoch(&self, new_epoch: u64) -> Result<(), MetaStoreError> {
        self.storage.force_bump_all_epoch(new_epoch).await
    }

    pub async fn get_epoch(&self) -> Result<u64, MetaStoreError> {
        self.storage.get_global_epoch().await
    }

    pub async fn recover_epoch(&self) -> Result<Vec<String>, MetaStoreError> {
        let proxy_addresses = self.storage.get_proxy_addresses(None, None).await?;
        let EpochFetchResult {
            max_epoch,
            failed_addresses,
        } = fetch_max_epoch(proxy_addresses).await;
        info!(
            "Get largest epoch {} with failed addresses: {:?}",
            max_epoch, failed_addresses
        );
        self.storage.recover_epoch(max_epoch + 1).await?;
        Ok(failed_addresses)
    }

    pub async fn check_metadata(&self) -> Result<Option<MetaStore>, MetaStoreError> {
        self.storage.check_metadata().await
    }
}

type ServiceState = Arc<MemBrokerService>;

fn get_version() -> &'static str {
    UNDERMOON_VERSION
}

async fn get_all_metadata(state: ServiceState) -> Result<impl warp::reply::Reply, Infallible> {
    Ok(warp_json(state.get_all_data().await.map(WarpRes::Json)))
}

async fn restore_metadata(
    meta_store: MetaStore,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let res = state.restore_metadata(meta_store).await;
    Ok(warp_json(res.map(warp_empty_res)))
}

#[derive(Deserialize)]
struct Pagination {
    offset: Option<usize>,
    limit: Option<usize>,
}

async fn get_proxy_addresses(
    pagination: Pagination,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let Pagination { offset, limit } = pagination;
    let res = state
        .get_proxy_addresses(offset, limit)
        .await
        .map(|addresses| ProxyAddressesPayload { addresses });
    Ok(warp_json(res.map(WarpRes::Json)))
}

async fn get_proxy_by_address(
    address: String,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let res = state
        .get_proxy_by_address(&address)
        .await
        .map(|proxy| ProxyPayload { proxy });
    Ok(warp_json(res.map(WarpRes::Json)))
}

async fn get_cluster_names(
    pagination: Pagination,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let Pagination { offset, limit } = pagination;
    let res = state
        .get_cluster_names(offset, limit)
        .await
        .map(|names| ClusterNamesPayload { names });
    Ok(warp_json(res.map(WarpRes::Json)))
}

async fn get_cluster_by_name(
    name: String,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let res = state
        .get_cluster_by_name(&name)
        .await
        .map(|cluster| ClusterPayload { cluster });
    Ok(warp_json(res.map(WarpRes::Json)))
}

async fn get_cluster_info_by_name(
    name: String,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let res = match state.get_cluster_info_by_name(&name).await {
        Ok(Some(cluster_info)) => Ok(cluster_info),
        Ok(None) => Err(MetaStoreError::ClusterNotFound),
        Err(err) => Err(err),
    };
    Ok(warp_json(res.map(WarpRes::Json)))
}

async fn get_failures(state: ServiceState) -> Result<impl warp::reply::Reply, Infallible> {
    let res = state
        .get_failures()
        .await
        .map(|addresses| FailuresPayload { addresses });
    Ok(warp_json(res.map(WarpRes::Json)))
}

#[derive(Deserialize, Serialize)]
pub struct ProxyResourcePayload {
    proxy_address: String,
    nodes: [String; CHUNK_HALF_NODE_NUM],
    host: Option<String>,
    index: Option<usize>,
}

async fn add_proxy(
    proxy_resource: ProxyResourcePayload,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let res = async {
        // This may still successfully update the store even on error.
        let res = state.add_proxy(proxy_resource).await;
        state.trigger_update().await?;
        res
    }
    .await;
    Ok(warp_json(res.map(warp_empty_res)))
}

#[derive(Deserialize, Serialize)]
pub struct CreateClusterPayload {
    node_number: usize,
}

async fn add_cluster(
    cluster_name: String,
    payload: CreateClusterPayload,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let CreateClusterPayload { node_number } = payload;
    let res = async {
        state.add_cluster(cluster_name, node_number).await?;
        state.trigger_update().await?;
        Ok(())
    }
    .await;
    Ok(warp_json(res.map(warp_empty_res)))
}

async fn remove_cluster(
    cluster_name: String,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let res = async {
        state.remove_cluster(cluster_name).await?;
        state.trigger_update().await?;
        Ok(())
    }
    .await;
    Ok(warp_json(res.map(warp_empty_res)))
}

#[derive(Deserialize, Serialize)]
pub struct AutoScaleUpNodesPayload {
    cluster_node_number: usize,
}

async fn auto_scale_up_nodes(
    cluster_name: String,
    payload: AutoScaleUpNodesPayload,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let node_num = payload.cluster_node_number;
    let res = async {
        let res = state.auto_scale_up_nodes(cluster_name, node_num).await?;
        state.trigger_update().await?;
        Ok(res)
    }
    .await;
    Ok(warp_json(res.map(WarpRes::Json)))
}

#[derive(Deserialize, Serialize)]
pub struct AutoAddNodesPayload {
    node_number: usize,
}

async fn auto_add_nodes(
    cluster_name: String,
    payload: AutoAddNodesPayload,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let node_num = payload.node_number;
    let res = async {
        let res = state.auto_add_nodes(cluster_name, node_num).await?;
        state.trigger_update().await?;
        Ok(res)
    }
    .await;
    Ok(warp_json(res.map(WarpRes::Json)))
}

async fn auto_delete_free_nodes(
    cluster_name: String,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let res = async {
        state.auto_delete_free_nodes(cluster_name).await?;
        state.trigger_update().await?;
        Ok(())
    }
    .await;
    Ok(warp_json(res.map(warp_empty_res)))
}

async fn change_config(
    cluster_name: String,
    config: HashMap<String, String>,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let res = async {
        state.change_config(cluster_name, config).await?;
        state.trigger_update().await?;
        Ok(())
    }
    .await;
    Ok(warp_json(res.map(warp_empty_res)))
}

async fn balance_masters(
    cluster_name: String,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let res = async {
        state.balance_masters(cluster_name).await?;
        state.trigger_update().await?;
        Ok(())
    }
    .await;
    Ok(warp_json(res.map(warp_empty_res)))
}

async fn bump_epoch(
    new_epoch: u64,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let res = async {
        state.force_bump_all_epoch(new_epoch).await?;
        state.trigger_update().await?;
        Ok(())
    }
    .await;
    Ok(warp_json(res.map(warp_empty_res)))
}

async fn remove_proxy(
    proxy_address: String,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let res = async {
        state.remove_proxy(proxy_address).await?;
        state.trigger_update().await?;
        Ok(())
    }
    .await;
    Ok(warp_json(res.map(warp_empty_res)))
}

#[derive(Deserialize, Serialize)]
pub struct ResourceFailureCheckPayload {
    hosts_cannot_fail: Vec<String>,
}

async fn check_resource_for_failures(
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let res = state
        .check_resource_for_failures()
        .await
        .map(|hosts_cannot_fail| ResourceFailureCheckPayload { hosts_cannot_fail });
    Ok(warp_json(res.map(WarpRes::Json)))
}

fn change_broker_config(
    config_payload: MemBrokerConfigPayload,
    state: ServiceState,
) -> impl warp::reply::Reply {
    let res = state.change_broker_config(config_payload);
    warp_json(res.map(warp_empty_res))
}

fn get_broker_config(state: ServiceState) -> impl warp::reply::Reply {
    let res = state.get_broker_config();
    warp_json(res.map(WarpRes::Json))
}

async fn migrate_slots(
    cluster_name: String,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let res = async {
        state.migrate_slots(cluster_name).await?;
        state.trigger_update().await?;
        Ok(())
    }
    .await;
    Ok(warp_json(res.map(warp_empty_res)))
}

async fn migrate_slots_to_scale_down(
    cluster_name: String,
    new_node_num: usize,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let res = async {
        state
            .migrate_slots_to_scale_down(cluster_name, new_node_num)
            .await?;
        state.trigger_update().await?;
        Ok(())
    }
    .await;
    Ok(warp_json(res.map(warp_empty_res)))
}

async fn auto_scale_node_number(
    cluster_name: String,
    new_node_num: usize,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let res = async {
        state
            .auto_scale_node_number(cluster_name, new_node_num)
            .await?;
        state.trigger_update().await?;
        Ok(())
    }
    .await;
    Ok(warp_json(res.map(warp_empty_res)))
}

async fn add_failure(
    server_proxy_address: String,
    reporter_id: String,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let res = async move {
        state.add_failure(server_proxy_address, reporter_id).await?;
        state.trigger_update().await?;
        Ok(())
    }
    .await;
    Ok(warp_json(res.map(warp_empty_res)))
}

async fn commit_migration(
    task: MigrationTaskMeta,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let res = async move {
        state.commit_migration(task).await?;
        state.trigger_update().await?;
        Ok(())
    }
    .await;
    Ok(warp_json(res.map(warp_empty_res)))
}

async fn replace_failed_node(
    proxy_address: String,
    state: ServiceState,
) -> Result<impl warp::reply::Reply, Infallible> {
    let res = async {
        let res = state
            .replace_failed_proxy(proxy_address)
            .await
            .map(|proxy| ReplaceProxyResponse { proxy });
        let sync_res = state.trigger_update().await;
        let res = res?;
        sync_res?;
        Ok(res)
    }
    .await;
    Ok(warp_json(res.map(WarpRes::Json)))
}

async fn get_failed_proxies(state: ServiceState) -> Result<impl warp::reply::Reply, Infallible> {
    let res = state
        .get_failed_proxies()
        .await
        .map(|addresses| FailedProxiesPayload { addresses });
    Ok(warp_json(res.map(WarpRes::Json)))
}

async fn get_epoch(state: ServiceState) -> Result<impl warp::reply::Reply, Infallible> {
    let res = state.get_epoch().await;
    Ok(warp_json(res.map(WarpRes::Json)))
}

#[derive(Deserialize, Serialize)]
struct RecoverEpochResult {
    failed_addresses: Vec<String>,
}

async fn recover_epoch(state: ServiceState) -> Result<impl warp::reply::Reply, Infallible> {
    let res = state
        .recover_epoch()
        .await
        .map(|failed_addresses| RecoverEpochResult { failed_addresses });
    Ok(warp_json(res.map(WarpRes::Json)))
}

impl MetaStoreError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            MetaStoreError::InUse => http::StatusCode::CONFLICT,
            MetaStoreError::NotInUse => http::StatusCode::CONFLICT,
            MetaStoreError::NoAvailableResource => http::StatusCode::CONFLICT,
            MetaStoreError::ResourceNotBalance => http::StatusCode::CONFLICT,
            MetaStoreError::AlreadyExisted => http::StatusCode::CONFLICT,
            MetaStoreError::ClusterNotFound => http::StatusCode::NOT_FOUND,
            MetaStoreError::FreeNodeNotFound => http::StatusCode::NOT_FOUND,
            MetaStoreError::FreeNodeFound => http::StatusCode::CONFLICT,
            MetaStoreError::ProxyNotFound => http::StatusCode::NOT_FOUND,
            MetaStoreError::InvalidNodeNum => http::StatusCode::BAD_REQUEST,
            MetaStoreError::NodeNumAlreadyEnough => http::StatusCode::CONFLICT,
            MetaStoreError::InvalidClusterName => http::StatusCode::BAD_REQUEST,
            MetaStoreError::InvalidMigrationTask => http::StatusCode::BAD_REQUEST,
            MetaStoreError::InvalidProxyAddress => http::StatusCode::BAD_REQUEST,
            MetaStoreError::MigrationTaskNotFound => http::StatusCode::NOT_FOUND,
            MetaStoreError::MigrationRunning => http::StatusCode::CONFLICT,
            MetaStoreError::InvalidConfig { .. } => http::StatusCode::BAD_REQUEST,
            MetaStoreError::SlotsAlreadyEven => http::StatusCode::BAD_REQUEST,
            MetaStoreError::SyncError(_) => http::StatusCode::INTERNAL_SERVER_ERROR,
            MetaStoreError::InvalidMetaVersion => http::StatusCode::CONFLICT,
            MetaStoreError::SmallEpoch => http::StatusCode::CONFLICT,
            MetaStoreError::MissingIndex => http::StatusCode::BAD_REQUEST,
            MetaStoreError::ProxyResourceOutOfOrder => http::StatusCode::CONFLICT,
            MetaStoreError::OrderedProxyEnabled => http::StatusCode::CONFLICT,
            MetaStoreError::OneClusterAlreadyExisted => http::StatusCode::CONFLICT,
            MetaStoreError::ProxyNotSync => http::StatusCode::INTERNAL_SERVER_ERROR,
            MetaStoreError::NodeNumberChanging => http::StatusCode::CONFLICT,
            MetaStoreError::External => http::StatusCode::INTERNAL_SERVER_ERROR,
            MetaStoreError::Retry => http::StatusCode::CONFLICT,
            MetaStoreError::EmptyExternalVersion => http::StatusCode::INTERNAL_SERVER_ERROR,
            MetaStoreError::ExternalTimeout => http::StatusCode::GATEWAY_TIMEOUT,
        }
    }
}
