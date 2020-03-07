use super::store::{MetaStore, MetaStoreError, MigrationType};
use crate::broker::store::InconsistentError;
use crate::common::cluster::{Cluster, DBName, MigrationTaskMeta, Node, Proxy};
use crate::common::version::UNDERMOON_VERSION;
use crate::coordinator::http_meta_broker::{
    ClusterNamesPayload, ClusterPayload, FailuresPayload, ProxyAddressesPayload, ProxyPayload,
};
use actix_http::ResponseBuilder;
use actix_web::{error, http, web, HttpRequest, HttpResponse, Responder};
use chrono;
use std::error::Error;
use std::sync::{Arc, RwLock};

pub fn configure_app(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api")
            .route("/version", web::get().to(get_version))
            .route("/metadata", web::get().to(get_all_metadata))
            .route("/validation", web::post().to(validate_meta))
            .route("/proxies/addresses", web::get().to(get_host_addresses))
            .route(
                "/proxies/nodes/{proxy_address}",
                web::delete().to(remove_proxy),
            )
            .route("/proxies/nodes", web::put().to(add_host))
            .route(
                "/proxies/failover/{address}",
                web::post().to(replace_failed_node),
            )
            .route(
                "/proxies/meta/{address}",
                web::get().to(get_host_by_address),
            )
            .route("/clusters/migrations", web::put().to(commit_migration))
            .route(
                "/clusters/meta/{cluster_name}",
                web::get().to(get_cluster_by_name),
            )
            .route("/clusters/names", web::get().to(get_cluster_names))
            .route(
                "/clusters/{cluster_name}/nodes/{proxy_address}",
                web::delete().to(remove_proxy_from_cluster),
            )
            .route(
                "/clusters/{cluster_name}/nodes",
                web::post().to(auto_add_nodes),
            )
            .route("/clusters/{cluster_name}", web::post().to(add_cluster))
            .route("/clusters/{cluster_name}", web::delete().to(remove_cluster))
            .route(
                "/failures/{server_proxy_address}/{reporter_id}",
                web::post().to(add_failure),
            )
            .route("/failures", web::get().to(get_failures))
            .route(
                "/clusters/{cluster_name}/migrations/half/{src_node}/{dst_node}",
                web::post().to(migrate_half_slots),
            )
            .route(
                "/clusters/{cluster_name}/migrations/all/{src_node}/{dst_node}",
                web::post().to(migrate_all_slots),
            )
            .route(
                "/clusters/{cluster_name}/migrations/{src_node}/{dst_node}",
                web::delete().to(stop_migrations),
            )
            .route(
                "/clusters/{cluster_name}/replications/{master_node}/{replica_node}",
                web::post().to(assign_replica),
            ),
    );
}

#[derive(Debug, Clone)]
pub struct MemBrokerConfig {
    pub address: String,
    pub failure_ttl: u64, // in seconds
}

pub struct MemBrokerService {
    config: MemBrokerConfig,
    store: Arc<RwLock<MetaStore>>,
}

impl MemBrokerService {
    pub fn new(config: MemBrokerConfig) -> Self {
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

    pub fn get_host_addresses(&self) -> Vec<String> {
        self.store
            .read()
            .expect("MemBrokerService::get_host_addresses")
            .get_hosts()
    }

    pub fn get_host_by_address(&self, address: &str) -> Option<Proxy> {
        self.store
            .read()
            .expect("MemBrokerService::get_host_by_address")
            .get_host_by_address(address)
    }

    pub fn get_cluster_names(&self) -> Vec<DBName> {
        self.store
            .read()
            .expect("MemBrokerService::get_cluster_names")
            .get_cluster_names()
    }

    pub fn get_cluster_by_name(&self, name: &str) -> Option<Cluster> {
        self.store
            .read()
            .expect("MemBrokerService::get_cluster_by_name")
            .get_cluster_by_name(name)
    }

    pub fn add_hosts(&self, host_resource: ProxyResource) -> Result<(), MetaStoreError> {
        let ProxyResource {
            proxy_address,
            nodes,
        } = host_resource;
        self.store
            .write()
            .expect("MemBrokerService::add_hosts")
            .add_hosts(proxy_address, nodes)
    }

    pub fn add_cluster(&self, cluster_name: String) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemBrokerService::add_cluster")
            .add_cluster(cluster_name)
    }

    pub fn remove_cluster(&self, cluster_name: String) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemBrokerService::remove_cluster")
            .remove_cluster(cluster_name)
    }

    pub fn auto_add_node(&self, cluster_name: String) -> Result<Vec<Node>, MetaStoreError> {
        self.store
            .write()
            .expect("MemBrokerService::auto_add_node")
            .auto_add_nodes(cluster_name)
    }

    pub fn remove_proxy_from_cluster(
        &self,
        cluster_name: String,
        proxy_address: String,
    ) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemBrokerService::remove_proxy_from_cluster")
            .remove_proxy_from_cluster(cluster_name, proxy_address)
    }

    pub fn remove_proxy(&self, proxy_address: String) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemBrokerService::remove_proxy")
            .remove_proxy(proxy_address)
    }

    pub fn migrate_slots(
        &self,
        cluster_name: String,
        src_node_address: String,
        dst_node_address: String,
        migration_type: MigrationType,
    ) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemBrokerService::migrate_slots")
            .migrate_slots(
                cluster_name,
                src_node_address,
                dst_node_address,
                migration_type,
            )
    }

    pub fn stop_migrations(
        &self,
        cluster_name: String,
        src_node_address: String,
        dst_node_address: String,
    ) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemBrokerService::stop_migrations")
            .stop_migrations(cluster_name, src_node_address, dst_node_address)
    }

    pub fn assign_replica(
        &self,
        cluster_name: String,
        master_node_address: String,
        replica_node_address: String,
    ) -> Result<(), MetaStoreError> {
        self.store
            .write()
            .expect("MemBrokerService::assign_replica")
            .assign_replica(cluster_name, master_node_address, replica_node_address)
    }

    pub fn get_failures(&self) -> Vec<String> {
        let failure_ttl = chrono::Duration::seconds(self.config.failure_ttl as i64);
        self.store
            .write()
            .expect("MemBrokerService::get_failures")
            .get_failures(failure_ttl)
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
        self.store
            .write()
            .expect("MemBrokerService::replace_failed_node")
            .replace_failed_proxy(failed_proxy_address)
    }

    pub fn validate_meta(&self) -> Result<(), InconsistentError> {
        self.store
            .read()
            .expect("MemBrokerService::validate_meta")
            .validate()
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

async fn get_host_addresses(state: ServiceState) -> impl Responder {
    let addresses = state.get_host_addresses();
    web::Json(ProxyAddressesPayload { addresses })
}

async fn get_host_by_address(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> impl Responder {
    let name = path.into_inner().0;
    let host = state.get_host_by_address(&name);
    web::Json(ProxyPayload { host })
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
    nodes: Vec<String>,
}

async fn add_host(
    (host_resource, state): (web::Json<ProxyResource>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    state.add_hosts(host_resource.into_inner()).map(|()| "")
}

async fn add_cluster(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let cluster_name = path.into_inner().0;
    state.add_cluster(cluster_name).map(|()| "")
}

async fn remove_cluster(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let cluster_name = path.into_inner().0;
    state.remove_cluster(cluster_name).map(|()| "")
}

async fn auto_add_nodes(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> Result<web::Json<Vec<Node>>, MetaStoreError> {
    let cluster_name = path.into_inner().0;
    state.auto_add_node(cluster_name).map(web::Json)
}

async fn remove_proxy_from_cluster(
    (path, state): (web::Path<(String, String)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let (cluster_name, proxy_address) = path.into_inner();
    state
        .remove_proxy_from_cluster(cluster_name, proxy_address)
        .map(|()| "")
}

async fn remove_proxy(
    (path, state): (web::Path<(String,)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let (proxy_address,) = path.into_inner();
    state.remove_proxy(proxy_address).map(|()| "")
}

async fn migrate_half_slots(
    (path, state): (web::Path<(String, String, String)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let (cluster_name, src_node_address, dst_node_address) = path.into_inner();
    state
        .migrate_slots(
            cluster_name,
            src_node_address,
            dst_node_address,
            MigrationType::Half,
        )
        .map(|()| "")
}

async fn migrate_all_slots(
    (path, state): (web::Path<(String, String, String)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let (cluster_name, src_node_address, dst_node_address) = path.into_inner();
    state
        .migrate_slots(
            cluster_name,
            src_node_address,
            dst_node_address,
            MigrationType::All,
        )
        .map(|()| "")
}

async fn stop_migrations(
    (path, state): (web::Path<(String, String, String)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let (cluster_name, src_node_address, dst_node_address) = path.into_inner();
    state
        .stop_migrations(cluster_name, src_node_address, dst_node_address)
        .map(|()| "")
}

async fn assign_replica(
    (path, state): (web::Path<(String, String, String)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let (cluster_name, master_node_address, replica_node_address) = path.into_inner();
    state
        .assign_replica(cluster_name, master_node_address, replica_node_address)
        .map(|()| "")
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

async fn validate_meta(state: ServiceState) -> Result<String, InconsistentError> {
    state.validate_meta().map(|()| "".to_string())
}

impl error::ResponseError for MetaStoreError {
    fn status_code(&self) -> http::StatusCode {
        match self {
            MetaStoreError::NoAvailableResource => http::StatusCode::CONFLICT,
            _ => http::StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> HttpResponse {
        ResponseBuilder::new(self.status_code()).body(self.description().to_string())
    }
}

impl error::ResponseError for InconsistentError {
    fn status_code(&self) -> http::StatusCode {
        http::StatusCode::INTERNAL_SERVER_ERROR
    }

    fn error_response(&self) -> HttpResponse {
        ResponseBuilder::new(self.status_code()).body(format!("{}", self))
    }
}
