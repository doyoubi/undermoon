use super::persistence::MetaPersistence;
use super::replication::MetaReplicator;
use super::resource::ResourceChecker;
use super::storage::{MemoryStorage, MetaStorage};
use super::store::{ClusterInfo, MetaStore, MetaStoreError, ScaleOp, CHUNK_HALF_NODE_NUM};
use crate::broker::epoch::{fetch_max_epoch, wait_for_proxy_epoch, EpochFetchResult};
use crate::broker::external::ExternalHttpStorage;
use crate::common::atomic_lock::AtomicLock;
use crate::common::cluster::{Cluster, ClusterName, MigrationTaskMeta, Node, Proxy};
use crate::common::version::UNDERMOON_VERSION;
use crate::coordinator::http_mani_broker::ReplaceProxyResponse;
use crate::coordinator::http_meta_broker::{
    ClusterNamesPayload, ClusterPayload, FailedProxiesPayload, FailuresPayload,
    ProxyAddressesPayload, ProxyPayload,
};
use actix_http::ResponseBuilder;
use actix_web::dev::Service;
use actix_web::{error, http, web, HttpRequest, HttpResponse};
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::{Arc, RwLock};
use std::time::Duration;

pub const MEM_BROKER_API_VERSION: &str = "/api/v2";

pub fn configure_app(cfg: &mut web::ServiceConfig, service: Arc<MemBrokerService>) {
    let service2 = service.clone();
    cfg.data(service).service(
        web::scope(MEM_BROKER_API_VERSION)
            .wrap_fn(move |req, srv| {
                let method = req.method().clone();
                let peer_addr = match req.peer_addr() {
                    None => "".to_string(),
                    Some(address) => format!("{:?}", address),
                };
                let req_str = format!("{} {} {} {:?} {}", req.method(), req.path(), req.query_string(), req.version(), peer_addr);
                let fut = srv.call(req);

                let service = if service2.config.debug {
                    Some(service2.clone())
                } else {
                    None
                };

                async move {
                    let res = fut.await;
                    // The GET APIs are accessed too frequently so we don't log them.
                    if method != http::Method::GET {
                        match &res {
                            Ok(response) => info!("{} status {}", req_str, response.status()),
                            Err(err) => info!("{} err {}", req_str, err)
                        }
                    } else if let Some(service) = service {
                        match service.check_metadata().await {
                            Err(err) => {
                                error!("failed to check metadata: {:?}", err);
                            }
                            Ok(None) => (),
                            Ok(Some(invalid_meta_store)) => {
                                error!("Invalid meta store: {:?}", invalid_meta_store);
                            }
                        }
                    }
                    res
                }
            })
            .route("/version", web::get().to(get_version))
            .route("/metadata", web::get().to(get_all_metadata))
            .route("/metadata", web::put().to(restore_metadata))
            // Broker api
            .route("/clusters/names", web::get().to(get_cluster_names))
            .route(
                "/clusters/meta/{cluster_name}",
                web::get().to(get_cluster_by_name),
            )
            .route("/proxies/addresses", web::get().to(get_proxy_addresses))
            .route(
                "/proxies/meta/{address}",
                web::get().to(get_proxy_by_address),
            )
            .route("/failures", web::get().to(get_failures))
            .route(
                "/failures/{server_proxy_address}/{reporter_id}",
                web::post().to(add_failure),
            )
            .route(
                "/proxies/failover/{address}",
                web::post().to(replace_failed_node),
            )
            .route("/clusters/migrations", web::put().to(commit_migration))
            .route("/proxies/failed/addresses", web::get().to(get_failed_proxies))

            // Additional api
            .route("/clusters/info/{cluster_name}", web::get().to(get_cluster_info_by_name))
            .route("/clusters/meta/{cluster_name}", web::post().to(add_cluster))
            .route("/clusters/meta/{cluster_name}", web::delete().to(remove_cluster))
            .route(
                "/clusters/nodes/{cluster_name}",
                web::patch().to(auto_add_nodes),
            )
            .route(
                "/clusters/nodes/{cluster_name}",
                web::put().to(auto_scale_up_nodes),
            )
            .route("/clusters/free_nodes/{cluster_name}", web::delete().to(auto_delete_free_nodes))
            .route(
                "/clusters/migrations/shrink/{cluster_name}/{node_number}",
                web::post().to(migrate_slots_to_scale_down),
            )
            .route("/clusters/migrations/expand/{cluster_name}", web::post().to(migrate_slots))
            .route("/clusters/migrations/auto/{cluster_name}/{node_number}", web::post().to(auto_scale_node_number))
            .route("/clusters/config/{cluster_name}", web::patch().to(change_config))
            .route("/clusters/balance/{cluster_name}", web::put().to(balance_masters))

            .route("/proxies/meta", web::post().to(add_proxy))
            .route(
                "/proxies/meta/{proxy_address}",
                web::delete().to(remove_proxy),
            )
            .route("/resources/failures/check", web::post().to(check_resource_for_failures))
            .route("/config", web::put().to(change_broker_config))
            .route("/config", web::get().to(get_broker_config))
            .route("/epoch", web::get().to(get_epoch))
            .route("/epoch/recovery", web::put().to(recover_epoch))
            .route("/epoch/{new_epoch}", web::put().to(bump_epoch)),
    );
}

pub type ReplicaAddresses = Arc<ArcSwap<Vec<String>>>;

#[derive(Derivative)]
#[derivative(Debug, Clone)]
pub enum StorageConfig {
    Memory,
    ExternalHTTP {
        // This name is used for the external storage
        // to differentiate different undermoon clusters.
        storage_name: String,
        #[derivative(Debug="ignore")]
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
    storage: Arc<dyn MetaStorage>,
    meta_persistence: Arc<dyn MetaPersistence + Send + Sync + 'static>,
    meta_replicator: Arc<dyn MetaReplicator + Send + Sync + 'static>,
    scale_lock: AtomicLock,
}

impl MemBrokerService {
    pub fn new(
        config: MemBrokerConfig,
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
            StorageConfig::Memory => {
                Arc::new(MemoryStorage::new(Arc::new(RwLock::new(meta_store))))
            }
            StorageConfig::ExternalHTTP {
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
        self.storage.add_cluster(cluster_name, node_num).await
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
            .ok_or_else(|| MetaStoreError::NodeNumberChanging)?;

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
            .ok_or_else(|| MetaStoreError::NodeNumberChanging)?;

        self.storage
            .auto_scale_up_nodes(cluster_name, cluster_node_num)
            .await
    }

    pub async fn auto_delete_free_nodes(&self, cluster_name: String) -> Result<(), MetaStoreError> {
        let _guard = self
            .scale_lock
            .lock()
            .ok_or_else(|| MetaStoreError::NodeNumberChanging)?;

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
            .ok_or_else(|| MetaStoreError::NodeNumberChanging)?;

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
            .ok_or_else(|| MetaStoreError::NodeNumberChanging)?;

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
            .ok_or_else(|| MetaStoreError::NodeNumberChanging)?;

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

type ServiceState = web::Data<Arc<MemBrokerService>>;

async fn get_version(_req: HttpRequest) -> &'static str {
    UNDERMOON_VERSION
}

async fn get_all_metadata(state: ServiceState) -> Result<web::Json<MetaStore>, MetaStoreError> {
    let metadata = state.get_all_data().await?;
    Ok(web::Json(metadata))
}

async fn restore_metadata(
    (meta_store, state): (web::Json<MetaStore>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    state
        .restore_metadata(meta_store.into_inner())
        .await
        .map(|_| "")
}

#[derive(Deserialize)]
struct Pagination {
    offset: Option<usize>,
    limit: Option<usize>,
}

async fn get_proxy_addresses(
    (web::Query(pagination), state): (web::Query<Pagination>, ServiceState),
) -> Result<web::Json<ProxyAddressesPayload>, MetaStoreError> {
    let Pagination { offset, limit } = pagination;
    let addresses = state.get_proxy_addresses(offset, limit).await?;
    Ok(web::Json(ProxyAddressesPayload { addresses }))
}

async fn get_proxy_by_address(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> Result<web::Json<ProxyPayload>, MetaStoreError> {
    let name = path.into_inner().0;
    let proxy = state.get_proxy_by_address(&name).await?;
    Ok(web::Json(ProxyPayload { proxy }))
}

async fn get_cluster_names(
    (web::Query(pagination), state): (web::Query<Pagination>, ServiceState),
) -> Result<web::Json<ClusterNamesPayload>, MetaStoreError> {
    let Pagination { offset, limit } = pagination;
    let names = state.get_cluster_names(offset, limit).await?;
    Ok(web::Json(ClusterNamesPayload { names }))
}

async fn get_cluster_by_name(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> Result<web::Json<ClusterPayload>, MetaStoreError> {
    let name = path.into_inner().0;
    let cluster = state.get_cluster_by_name(&name).await?;
    Ok(web::Json(ClusterPayload { cluster }))
}

async fn get_cluster_info_by_name(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> Result<web::Json<ClusterInfo>, MetaStoreError> {
    let name = path.into_inner().0;
    match state.get_cluster_info_by_name(&name).await {
        Ok(Some(cluster_info)) => Ok(web::Json(cluster_info)),
        Ok(None) => Err(MetaStoreError::ClusterNotFound),
        Err(err) => Err(err),
    }
}

async fn get_failures(state: ServiceState) -> Result<web::Json<FailuresPayload>, MetaStoreError> {
    let addresses = state.get_failures().await?;
    Ok(web::Json(FailuresPayload { addresses }))
}

#[derive(Deserialize, Serialize)]
pub struct ProxyResourcePayload {
    proxy_address: String,
    nodes: [String; CHUNK_HALF_NODE_NUM],
    host: Option<String>,
    index: Option<usize>,
}

async fn add_proxy(
    (proxy_resource, state): (web::Json<ProxyResourcePayload>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    // This may still successfully update the store even on error.
    let res = state.add_proxy(proxy_resource.into_inner()).await;
    state.trigger_update().await?;
    res.map(|()| "")
}

#[derive(Deserialize, Serialize)]
pub struct CreateClusterPayload {
    node_number: usize,
}

async fn add_cluster(
    (path, payload, state): (
        web::Path<(String,)>,
        web::Json<CreateClusterPayload>,
        ServiceState,
    ),
) -> Result<&'static str, MetaStoreError> {
    let cluster_name = path.into_inner().0;
    let CreateClusterPayload { node_number } = payload.into_inner();
    state.add_cluster(cluster_name, node_number).await?;
    state.trigger_update().await?;
    Ok("")
}

async fn remove_cluster(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let cluster_name = path.into_inner().0;
    state.remove_cluster(cluster_name).await?;
    state.trigger_update().await?;
    Ok("")
}

#[derive(Deserialize, Serialize)]
pub struct AutoScaleUpNodesPayload {
    cluster_node_number: usize,
}

async fn auto_scale_up_nodes(
    (path, payload, state): (
        web::Path<(String,)>,
        web::Json<AutoScaleUpNodesPayload>,
        ServiceState,
    ),
) -> Result<web::Json<Vec<Node>>, MetaStoreError> {
    let cluster_name = path.into_inner().0;
    let node_num = payload.into_inner().cluster_node_number;
    let res = state
        .auto_scale_up_nodes(cluster_name, node_num)
        .await
        .map(web::Json)?;
    state.trigger_update().await?;
    Ok(res)
}

#[derive(Deserialize, Serialize)]
pub struct AutoAddNodesPayload {
    node_number: usize,
}

async fn auto_add_nodes(
    (path, payload, state): (
        web::Path<(String,)>,
        web::Json<AutoAddNodesPayload>,
        ServiceState,
    ),
) -> Result<web::Json<Vec<Node>>, MetaStoreError> {
    let cluster_name = path.into_inner().0;
    let node_num = payload.into_inner().node_number;
    let res = state
        .auto_add_nodes(cluster_name, node_num)
        .await
        .map(web::Json)?;
    state.trigger_update().await?;
    Ok(res)
}

async fn auto_delete_free_nodes(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let cluster_name = path.into_inner().0;
    state.auto_delete_free_nodes(cluster_name).await?;
    state.trigger_update().await?;
    Ok("")
}

async fn change_config(
    (path, config, state): (
        web::Path<(String,)>,
        web::Json<HashMap<String, String>>,
        ServiceState,
    ),
) -> Result<&'static str, MetaStoreError> {
    let cluster_name = path.into_inner().0;
    state
        .change_config(cluster_name, config.into_inner())
        .await?;
    state.trigger_update().await?;
    Ok("")
}

async fn balance_masters(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let cluster_name = path.into_inner().0;
    state.balance_masters(cluster_name).await?;
    state.trigger_update().await?;
    Ok("")
}

async fn bump_epoch(
    (path, state): (web::Path<(u64,)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let new_epoch = path.into_inner().0;
    state.force_bump_all_epoch(new_epoch).await?;
    state.trigger_update().await?;
    Ok("")
}

async fn remove_proxy(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let (proxy_address,) = path.into_inner();
    state.remove_proxy(proxy_address).await?;
    state.trigger_update().await?;
    Ok("")
}

#[derive(Deserialize, Serialize)]
pub struct ResourceFailureCheckPayload {
    hosts_cannot_fail: Vec<String>,
}

async fn check_resource_for_failures(
    state: ServiceState,
) -> Result<web::Json<ResourceFailureCheckPayload>, MetaStoreError> {
    let hosts_cannot_fail = state.check_resource_for_failures().await?;
    Ok(web::Json(ResourceFailureCheckPayload { hosts_cannot_fail }))
}

async fn change_broker_config(
    (state, config_payload): (ServiceState, web::Json<MemBrokerConfigPayload>),
) -> Result<&'static str, MetaStoreError> {
    state.change_broker_config(config_payload.into_inner())?;
    Ok("")
}

async fn get_broker_config(
    state: ServiceState,
) -> Result<web::Json<MemBrokerConfigPayload>, MetaStoreError> {
    let payload = state.get_broker_config()?;
    Ok(web::Json(payload))
}

async fn migrate_slots(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let (cluster_name,) = path.into_inner();
    state.migrate_slots(cluster_name).await?;
    state.trigger_update().await?;
    Ok("")
}

async fn migrate_slots_to_scale_down(
    (path, state): (web::Path<(String, usize)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let (cluster_name, new_node_num) = path.into_inner();
    state
        .migrate_slots_to_scale_down(cluster_name, new_node_num)
        .await?;
    state.trigger_update().await?;
    Ok("")
}

async fn auto_scale_node_number(
    (path, state): (web::Path<(String, usize)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let (cluster, new_node_num) = path.into_inner();
    state.auto_scale_node_number(cluster, new_node_num).await?;
    state.trigger_update().await?;
    Ok("")
}

async fn add_failure(
    (path, state): (web::Path<(String, String)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let (server_proxy_address, reporter_id) = path.into_inner();
    state.add_failure(server_proxy_address, reporter_id).await?;
    state.trigger_update().await?;
    Ok("")
}

async fn commit_migration(
    (task, state): (web::Json<MigrationTaskMeta>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    state.commit_migration(task.into_inner()).await?;
    state.trigger_update().await?;
    Ok("")
}

async fn replace_failed_node(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> Result<web::Json<ReplaceProxyResponse>, MetaStoreError> {
    let (proxy_address,) = path.into_inner();
    let res = state
        .replace_failed_proxy(proxy_address)
        .await
        .map(|proxy| ReplaceProxyResponse { proxy })
        .map(web::Json);
    let sync_res = state.trigger_update().await;
    let res = res?;
    sync_res?;
    Ok(res)
}

async fn get_failed_proxies(
    state: ServiceState,
) -> Result<web::Json<FailedProxiesPayload>, MetaStoreError> {
    let addresses = state.get_failed_proxies().await?;
    Ok(web::Json(FailedProxiesPayload { addresses }))
}

async fn get_epoch(state: ServiceState) -> Result<String, MetaStoreError> {
    state.get_epoch().await.map(|epoch| epoch.to_string())
}

#[derive(Deserialize, Serialize)]
struct RecoverEpochResult {
    failed_addresses: Vec<String>,
}

async fn recover_epoch(
    state: ServiceState,
) -> Result<web::Json<RecoverEpochResult>, MetaStoreError> {
    let failed_addresses = state.recover_epoch().await?;
    let result = RecoverEpochResult { failed_addresses };
    Ok(web::Json(result))
}

impl error::ResponseError for MetaStoreError {
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

    fn error_response(&self) -> HttpResponse {
        ResponseBuilder::new(self.status_code()).json(self)
    }
}
