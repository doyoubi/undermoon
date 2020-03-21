use super::store::{MetaStore, MetaStoreError, CHUNK_HALF_NODE_NUM};
use crate::common::cluster::{Cluster, DBName, MigrationTaskMeta, Node, Proxy};
use crate::common::version::UNDERMOON_VERSION;
use crate::coordinator::http_meta_broker::{
    ClusterNamesPayload, ClusterPayload, FailuresPayload, ProxyAddressesPayload, ProxyPayload,
};
use actix_http::ResponseBuilder;
use actix_web::dev::Service;
use actix_web::{error, http, web, HttpRequest, HttpResponse, Responder};
use chrono;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub const MEM_BROKER_API_VERSION: &str = "/api/v2";

pub fn configure_app(cfg: &mut web::ServiceConfig, service: Arc<MemBrokerService>) {
    cfg.data(service).service(
        web::scope(MEM_BROKER_API_VERSION)
            .wrap_fn(|req, srv| {
                let method = req.method().clone();
                let peer_addr = match req.peer_addr() {
                    None => "".to_string(),
                    Some(address) => format!("{:?}", address),
                };
                let req_str = format!("{} {} {} {:?} {}", req.method(), req.path(), req.query_string(), req.version(), peer_addr);
                let fut = srv.call(req);
                async move {
                    let res = fut.await;
                    // The GET APIs are accessed too frequently so we don't log them.
                    if method != http::Method::GET {
                        match &res {
                            Ok(response) => info!("{} status {}", req_str, response.status()),
                            Err(err) => info!("{} err {}", req_str, err)
                        }
                    }
                    res
                }
            })
            .route("/version", web::get().to(get_version))
            .route("/metadata", web::get().to(get_all_metadata))
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

            // Additional api
            .route("/clusters/meta/{cluster_name}", web::post().to(add_cluster))
            .route("/clusters/meta/{cluster_name}", web::delete().to(remove_cluster))
            .route(
                "/clusters/nodes/{cluster_name}",
                web::patch().to(auto_add_nodes),
            )
            .route("/clusters/free_nodes/{cluster_name}", web::delete().to(audo_delete_free_nodes))
            .route(
                "/clusters/migrations/{cluster_name}",
                web::post().to(migrate_slots),
            )
            .route("clusters/config/{cluster_name}", web::patch().to(change_config))

            .route("/proxies/nodes", web::post().to(add_proxy))
            .route(
                "/proxies/nodes/{proxy_address}",
                web::delete().to(remove_proxy),
            ),
    );
}

#[derive(Debug, Clone)]
pub struct MemBrokerConfig {
    pub address: String,
    pub failure_ttl: u64, // in seconds
    pub failure_quorum: u64,
    pub migration_limit: u64,
}

pub struct MemBrokerService {
    config: MemBrokerConfig,
    store: Arc<RwLock<MetaStore>>,
}

impl MemBrokerService {
    pub fn new(config: MemBrokerConfig) -> Self {
        info!("config: {:?}", config);
        Self {
            config,
            store: Arc::new(RwLock::new(MetaStore::default())),
        }
    }

    pub fn get_all_data(&self) -> MetaStore {
        self.store
            .read()
            .expect("MemBrokerService::get_all_data")
            .clone()
    }

    pub fn get_proxy_addresses(&self) -> Vec<String> {
        self.store
            .read()
            .expect("MemBrokerService::get_proxy_addresses")
            .get_proxies()
    }

    pub fn get_proxy_by_address(&self, address: &str) -> Option<Proxy> {
        let migration_limit = self.config.migration_limit;
        self.store
            .read()
            .expect("MemBrokerService::get_proxy_by_address")
            .get_proxy_by_address(address, migration_limit)
    }

    pub fn get_cluster_names(&self) -> Vec<DBName> {
        self.store
            .read()
            .expect("MemBrokerService::get_cluster_names")
            .get_cluster_names()
    }

    pub fn get_cluster_by_name(&self, name: &str) -> Option<Cluster> {
        let migration_limit = self.config.migration_limit;
        self.store
            .read()
            .expect("MemBrokerService::get_cluster_by_name")
            .get_cluster_by_name(name, migration_limit)
    }

    pub fn add_proxy(&self, proxy_resource: ProxyResource) -> Result<(), MetaStoreError> {
        let ProxyResource {
            proxy_address,
            nodes,
        } = proxy_resource;
        self.store
            .write()
            .expect("MemBrokerService::add_proxy")
            .add_proxy(proxy_address, nodes)
    }

    pub fn add_cluster(&self, cluster_name: String, node_num: usize) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemBrokerService::add_cluster")
            .add_cluster(cluster_name, node_num)
    }

    pub fn remove_cluster(&self, cluster_name: String) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemBrokerService::remove_cluster")
            .remove_cluster(cluster_name)
    }

    pub fn auto_add_node(
        &self,
        cluster_name: String,
        node_num: usize,
    ) -> Result<Vec<Node>, MetaStoreError> {
        self.store
            .write()
            .expect("MemBrokerService::auto_add_node")
            .auto_add_nodes(cluster_name, node_num)
    }

    pub fn audo_delete_free_nodes(&self, cluster_name: String) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemBrokerService::audo_delete_free_nodes")
            .audo_delete_free_nodes(cluster_name)
    }

    pub fn change_config(
        &self,
        cluster_name: String,
        config: HashMap<String, String>,
    ) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemBrokerService::change_config")
            .change_config(cluster_name, config)
    }

    pub fn remove_proxy(&self, proxy_address: String) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemBrokerService::remove_proxy")
            .remove_proxy(proxy_address)
    }

    pub fn migrate_slots(&self, cluster_name: String) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemBrokerService::migrate_slots")
            .migrate_slots(cluster_name)
    }

    pub fn get_failures(&self) -> Vec<String> {
        let failure_ttl = chrono::Duration::seconds(self.config.failure_ttl as i64);
        let failure_quorum = self.config.failure_quorum;
        self.store
            .write()
            .expect("MemBrokerService::get_failures")
            .get_failures(failure_ttl, failure_quorum)
    }

    pub fn add_failure(&self, address: String, reporter_id: String) {
        self.store
            .write()
            .expect("MemBrokerService::add_failure")
            .add_failure(address, reporter_id)
    }

    pub fn commit_migration(&self, task: MigrationTaskMeta) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemBrokerService::commit_migration")
            .commit_migration(task)
    }

    pub fn replace_failed_node(
        &self,
        failed_proxy_address: String,
    ) -> Result<Proxy, MetaStoreError> {
        let migration_limit = self.config.migration_limit;
        self.store
            .write()
            .expect("MemBrokerService::replace_failed_node")
            .replace_failed_proxy(failed_proxy_address, migration_limit)
    }
}

type ServiceState = web::Data<Arc<MemBrokerService>>;

async fn get_version(_req: HttpRequest) -> &'static str {
    UNDERMOON_VERSION
}

async fn get_all_metadata(state: ServiceState) -> impl Responder {
    let metadata = state.get_all_data();
    web::Json(metadata)
}

async fn get_proxy_addresses(state: ServiceState) -> impl Responder {
    let addresses = state.get_proxy_addresses();
    web::Json(ProxyAddressesPayload { addresses })
}

async fn get_proxy_by_address(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> impl Responder {
    let name = path.into_inner().0;
    let proxy = state.get_proxy_by_address(&name);
    web::Json(ProxyPayload { proxy })
}

async fn get_cluster_names(state: ServiceState) -> impl Responder {
    let names = state.get_cluster_names();
    web::Json(ClusterNamesPayload { names })
}

async fn get_cluster_by_name(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> impl Responder {
    let name = path.into_inner().0;
    let cluster = state.get_cluster_by_name(&name);
    web::Json(ClusterPayload { cluster })
}

async fn get_failures(state: ServiceState) -> impl Responder {
    let addresses = state.get_failures();
    web::Json(FailuresPayload { addresses })
}

#[derive(Deserialize, Serialize)]
pub struct ProxyResource {
    proxy_address: String,
    nodes: [String; CHUNK_HALF_NODE_NUM],
}

async fn add_proxy(
    (proxy_resource, state): (web::Json<ProxyResource>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    state.add_proxy(proxy_resource.into_inner()).map(|()| "")
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
    state.add_cluster(cluster_name, node_number).map(|()| "")
}

async fn remove_cluster(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let cluster_name = path.into_inner().0;
    state.remove_cluster(cluster_name).map(|()| "")
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
    state.auto_add_node(cluster_name, node_num).map(web::Json)
}

async fn audo_delete_free_nodes(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let cluster_name = path.into_inner().0;
    state.audo_delete_free_nodes(cluster_name).map(|()| "")
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
        .map(|()| "")
}

async fn remove_proxy(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let (proxy_address,) = path.into_inner();
    state.remove_proxy(proxy_address).map(|()| "")
}

async fn migrate_slots(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let (cluster_name,) = path.into_inner();
    state.migrate_slots(cluster_name).map(|()| "")
}

async fn add_failure((path, state): (web::Path<(String, String)>, ServiceState)) -> &'static str {
    let (server_proxy_address, reporter_id) = path.into_inner();
    state.add_failure(server_proxy_address, reporter_id);
    ""
}

async fn commit_migration(
    (task, state): (web::Json<MigrationTaskMeta>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    state.commit_migration(task.into_inner()).map(|()| "")
}

async fn replace_failed_node(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> Result<web::Json<Proxy>, MetaStoreError> {
    let (proxy_address,) = path.into_inner();
    state.replace_failed_node(proxy_address).map(web::Json)
}

impl error::ResponseError for MetaStoreError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            MetaStoreError::InUse => http::StatusCode::BAD_REQUEST,
            MetaStoreError::NotInUse => http::StatusCode::BAD_REQUEST,
            MetaStoreError::NoAvailableResource => http::StatusCode::CONFLICT,
            MetaStoreError::ResourceNotBalance => http::StatusCode::CONFLICT,
            MetaStoreError::AlreadyExisted => http::StatusCode::BAD_REQUEST,
            MetaStoreError::ClusterNotFound => http::StatusCode::NOT_FOUND,
            MetaStoreError::FreeNodeNotFound => http::StatusCode::NOT_FOUND,
            MetaStoreError::ProxyNotFound => http::StatusCode::NOT_FOUND,
            MetaStoreError::InvalidNodeNum => http::StatusCode::BAD_REQUEST,
            MetaStoreError::InvalidClusterName => http::StatusCode::BAD_REQUEST,
            MetaStoreError::InvalidMigrationTask => http::StatusCode::BAD_REQUEST,
            MetaStoreError::InvalidProxyAddress => http::StatusCode::BAD_REQUEST,
            MetaStoreError::MigrationTaskNotFound => http::StatusCode::NOT_FOUND,
            MetaStoreError::MigrationRunning => http::StatusCode::CONFLICT,
            MetaStoreError::NotSupported => http::StatusCode::BAD_REQUEST,
            MetaStoreError::InvalidConfig { .. } => http::StatusCode::BAD_REQUEST,
            MetaStoreError::SlotsAlreadyEven => http::StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> HttpResponse {
        ResponseBuilder::new(self.status_code()).body(format!("{:?}", self))
    }
}
