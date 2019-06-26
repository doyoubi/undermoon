use super::store::{MetaStore, MetaStoreError, MigrationType};
use ::common::cluster::{Cluster, Host, MigrationTaskMeta, Node};
use ::common::version::UNDERMOON_VERSION;
use ::coordinator::http_mani_broker::ReplaceNodePayload;
use ::coordinator::http_meta_broker::{
    ClusterNamesPayload, ClusterPayload, FailuresPayload, HostAddressesPayload, HostPayload,
};
use actix_web::{
    error, http, middleware, App, HttpRequest, HttpResponse, Json, Path, Responder, State,
};
use chrono;
use std::error::Error;
use std::sync::{Arc, RwLock};

pub fn gen_app(service: Arc<MemBrokerService>) -> App<Arc<MemBrokerService>> {
    App::with_state(service)
        .middleware(middleware::Logger::default())
        .prefix("/api")
        .resource("/version", |r| r.method(http::Method::GET).f(get_version))
        .resource("/metadata", |r| {
            r.method(http::Method::GET).f(get_all_metadata)
        })
        .resource("/hosts/addresses/{address}", |r| {
            r.method(http::Method::GET).with(get_host_by_address)
        })
        .resource("/hosts/addresses", |r| {
            r.method(http::Method::GET).f(get_host_addresses)
        })
        .resource("/clusters/nodes", |r| {
            r.method(http::Method::PUT).with(replace_failed_node)
        })
        .resource("/clusters/{cluster_name}/meta", |r| {
            r.method(http::Method::GET).with(get_cluster_by_name)
        })
        .resource("/clusters/names", |r| {
            r.method(http::Method::GET).f(get_cluster_names)
        })
        .resource("/hosts/nodes/{proxy_address}", |r| {
            r.method(http::Method::DELETE).with(remove_proxy)
        })
        .resource("/hosts/nodes", |r| {
            r.method(http::Method::PUT).with(add_host)
        })
        .resource("/clusters/{cluster_name}/nodes/{proxy_address}", |r| {
            r.method(http::Method::DELETE)
                .with(remove_proxy_from_cluster);
        })
        .resource("/clusters/{cluster_name}/nodes", |r| {
            r.method(http::Method::POST).with(auto_add_nodes)
        })
        .resource("/clusters/{cluster_name}", |r| {
            r.method(http::Method::POST).with(add_cluster);
            r.method(http::Method::DELETE).with(remove_cluster);
        })
        .resource("/failures/{server_proxy_address}/{reporter_id}", |r| {
            r.method(http::Method::POST).with(add_failure)
        })
        .resource("/failures", |r| r.method(http::Method::GET).f(get_failures))
        .resource(
            "/clusters/{cluster_name}/migrations/half/{src_node}/{dst_node}",
            |r| r.method(http::Method::POST).with(migrate_half_slots),
        )
        .resource(
            "/clusters/{cluster_name}/migrations/all/{src_node}/{dst_node}",
            |r| r.method(http::Method::POST).with(migrate_all_slots),
        )
        .resource(
            "/clusters/{cluster_name}/migrations/{src_node}/{dst_node}",
            |r| r.method(http::Method::DELETE).with(stop_migrations),
        )
        .resource(
            "/clusters/{cluster_name}/replications/{master_node}/{replica_node}",
            |r| r.method(http::Method::POST).with(assign_replica),
        )
        .resource("/clusters/migrations", |r| {
            r.method(http::Method::PUT).with(commit_migration)
        })
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

    pub fn get_host_by_address(&self, address: &str) -> Option<Host> {
        self.store
            .read()
            .expect("MemBrokerService::get_host_by_address")
            .get_host_by_address(address)
    }

    pub fn get_cluster_names(&self) -> Vec<String> {
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

    pub fn add_hosts(&self, host_resource: HostResource) -> Result<(), MetaStoreError> {
        let HostResource {
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
        curr_cluster_epoch: u64,
        node: Node,
    ) -> Result<Node, MetaStoreError> {
        self.store
            .write()
            .expect("MemBrokerService::replace_node")
            .replace_failed_node(curr_cluster_epoch, node)
    }
}

fn get_version(_req: &HttpRequest<Arc<MemBrokerService>>) -> &'static str {
    UNDERMOON_VERSION
}

fn get_all_metadata(request: &HttpRequest<Arc<MemBrokerService>>) -> impl Responder {
    let metadata = request.state().get_all_data();
    Json(metadata)
}

fn get_host_addresses(request: &HttpRequest<Arc<MemBrokerService>>) -> impl Responder {
    let addresses = request.state().get_host_addresses();
    Json(HostAddressesPayload { addresses })
}

fn get_host_by_address((path, state): (Path<(String,)>, ServiceState)) -> impl Responder {
    let name = path.into_inner().0;
    let host = state.get_host_by_address(&name);
    Json(HostPayload { host })
}

fn get_cluster_names(request: &HttpRequest<Arc<MemBrokerService>>) -> impl Responder {
    let names = request.state().get_cluster_names();
    Json(ClusterNamesPayload { names })
}

fn get_cluster_by_name((path, state): (Path<(String,)>, ServiceState)) -> impl Responder {
    let name = path.into_inner().0;
    let cluster = state.get_cluster_by_name(&name);
    Json(ClusterPayload { cluster })
}

fn get_failures(request: &HttpRequest<Arc<MemBrokerService>>) -> impl Responder {
    let addresses = request.state().get_failures();
    Json(FailuresPayload { addresses })
}

#[derive(Deserialize, Serialize)]
pub struct HostResource {
    proxy_address: String,
    nodes: Vec<String>,
}

type ServiceState = State<Arc<MemBrokerService>>;

fn add_host(
    (host_resource, state): (Json<HostResource>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    state.add_hosts(host_resource.into_inner()).map(|()| "")
}

fn add_cluster(
    (path, state): (Path<(String,)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let cluster_name = path.into_inner().0;
    state.add_cluster(cluster_name).map(|()| "")
}

fn remove_cluster(
    (path, state): (Path<(String,)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let cluster_name = path.into_inner().0;
    state.remove_cluster(cluster_name).map(|()| "")
}

fn auto_add_nodes(
    (path, state): (Path<(String,)>, ServiceState),
) -> Result<Json<Vec<Node>>, MetaStoreError> {
    let cluster_name = path.into_inner().0;
    state.auto_add_node(cluster_name).map(Json)
}

fn remove_proxy_from_cluster(
    (path, state): (Path<(String, String)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let (cluster_name, proxy_address) = path.into_inner();
    state
        .remove_proxy_from_cluster(cluster_name, proxy_address)
        .map(|()| "")
}

fn remove_proxy(
    (path, state): (Path<(String,)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let (proxy_address,) = path.into_inner();
    state.remove_proxy(proxy_address).map(|()| "")
}

fn migrate_half_slots(
    (path, state): (Path<(String, String, String)>, ServiceState),
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

fn migrate_all_slots(
    (path, state): (Path<(String, String, String)>, ServiceState),
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

fn stop_migrations(
    (path, state): (Path<(String, String, String)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let (cluster_name, src_node_address, dst_node_address) = path.into_inner();
    state
        .stop_migrations(cluster_name, src_node_address, dst_node_address)
        .map(|()| "")
}

fn assign_replica(
    (path, state): (Path<(String, String, String)>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    let (cluster_name, master_node_address, replica_node_address) = path.into_inner();
    state
        .assign_replica(cluster_name, master_node_address, replica_node_address)
        .map(|()| "")
}

fn add_failure((path, state): (Path<(String, String)>, ServiceState)) -> &'static str {
    let (server_proxy_address, reporter_id) = path.into_inner();
    state.add_failure(server_proxy_address, reporter_id);
    ""
}

fn commit_migration(
    (task, state): (Json<MigrationTaskMeta>, ServiceState),
) -> Result<&'static str, MetaStoreError> {
    state.commit_migration(task.into_inner()).map(|()| "")
}

fn replace_failed_node(
    (payload, state): (Json<ReplaceNodePayload>, ServiceState),
) -> Result<Json<Node>, MetaStoreError> {
    let ReplaceNodePayload {
        cluster_epoch,
        node,
    } = payload.into_inner();
    state.replace_failed_node(cluster_epoch, node).map(Json)
}

impl error::ResponseError for MetaStoreError {
    fn error_response(&self) -> HttpResponse {
        let mut response = HttpResponse::new(http::StatusCode::BAD_REQUEST);
        response.set_body(self.description().to_string());
        response
    }
}
