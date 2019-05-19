use super::store::{MetaStore, MetaStoreError, NodeSlot};
use ::common::version::UNDERMOON_VERSION;
use ::common::cluster::Node;
use ::coordinator::http_meta_broker::HostAddressesPayload;
use actix_web::{error, http, App, HttpRequest, Json, Path, Responder, State};
use std::error::Error;
use std::sync::{Arc, RwLock};

pub fn gen_app(service: Arc<MemBrokerService>) -> App<Arc<MemBrokerService>> {
    App::with_state(service)
        .prefix("/api")
        .resource("/version", |r| r.method(http::Method::GET).f(get_version))
        .resource("/metadata", |r| r.method(http::Method::GET).f(get_all_metadata))
        .resource("/hosts/addresses", |r| {
            r.method(http::Method::GET).f(get_host_addresses)
        })
        .resource("/hosts/nodes", |r| {
            r.method(http::Method::PUT).with(add_host)
        })
        .resource("/clusters/{name}/nodes/{proxy_address}/{node_address}", |r| {
            r.method(http::Method::POST).with(add_node);
            r.method(http::Method::DELETE).with(remove_node_from_cluster);
        })
        .resource("/clusters/{name}/nodes", |r| {
            r.method(http::Method::POST).with(auto_add_node)
        })
        .resource("/clusters/{name}", |r| {
            r.method(http::Method::POST).with(add_cluster);
            r.method(http::Method::DELETE).with(remove_cluster);
        })
}

#[derive(Debug, Clone)]
pub struct MemBrokerConfig {
    pub address: String,
}

pub struct MemBrokerService {
    _config: MemBrokerConfig,
    store: Arc<RwLock<MetaStore>>,
}

impl MemBrokerService {
    pub fn new(_config: MemBrokerConfig) -> Self {
        Self {
            _config,
            store: Arc::new(RwLock::new(MetaStore::default())),
        }
    }

    pub fn get_all_data(&self) -> MetaStore {
        self.store.read().expect("MemBrokerService::get_all_data").clone()
    }

    pub fn get_host_addresses(&self) -> Vec<String> {
        self.store
            .read()
            .expect("MemBrokerService::get_host_addresses")
            .get_hosts()
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

    pub fn auto_add_node(&self, cluster_name: String) -> Result<Node, MetaStoreError> {
        self.store.write().expect("MemBrokerService::auto_add_node").auto_add_node(cluster_name)
    }

    pub fn add_node(&self, cluster_name: String, proxy_address: String, node_address: String) -> Result<Node, MetaStoreError> {
        let node_slot = NodeSlot{
            proxy_address,
            node_address,
        };
        self.store.write().expect("MemBrokerService::add_node").add_node(cluster_name, node_slot)
    }

    pub fn remove_node_from_cluster(&self, cluster_name: String, proxy_address: String, node_address: String) -> Result<(), MetaStoreError> {
        let node_slot = NodeSlot {
            proxy_address,
            node_address,
        };
        self.store.write().expect("MemBrokerService::remove_node_from_cluster").remove_node_from_cluster(cluster_name, node_slot)
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

#[derive(Deserialize, Serialize)]
pub struct HostResource {
    proxy_address: String,
    nodes: Vec<String>,
}

fn add_host(
    (host_resource, state): (Json<HostResource>, State<Arc<MemBrokerService>>),
) -> error::Result<&'static str> {
    state
        .add_hosts(host_resource.into_inner())
        .map(|()| "")
        .map_err(|e| error::ErrorBadRequest(e.description().to_string()))
}

fn add_cluster(
    (path, state): (Path<(String,)>, State<Arc<MemBrokerService>>),
) -> error::Result<&'static str> {
    let cluster_name = path.into_inner().0;
    state
        .add_cluster(cluster_name)
        .map(|()| "")
        .map_err(|e| error::ErrorBadRequest(e.description().to_string()))
}

fn remove_cluster(
    (path, state): (Path<(String,)>, State<Arc<MemBrokerService>>),
) -> error::Result<&'static str> {
    let cluster_name = path.into_inner().0;
    state
        .remove_cluster(cluster_name)
        .map(|()| "")
        .map_err(|e| error::ErrorBadRequest(e.description().to_string()))
}

fn auto_add_node(
    (path, state): (Path<(String,)>, State<Arc<MemBrokerService>>),
) -> error::Result<Json<Node>> {
    let cluster_name = path.into_inner().0;
    state.auto_add_node(cluster_name)
        .map(Json)
            .map_err(|e| error::ErrorBadRequest(e.description().to_string()))
}

fn add_node(
    (path, state): (Path<(String, String, String)>, State<Arc<MemBrokerService>>),
) -> error::Result<Json<Node>> {
    let (cluster_name, proxy_address, node_address) = path.into_inner();
    state.add_node(cluster_name, proxy_address, node_address)
        .map(Json)
        .map_err(|e| error::ErrorBadRequest(e.description().to_string()))
}

fn remove_node_from_cluster(
    (path, state): (Path<(String, String, String)>, State<Arc<MemBrokerService>>),
) -> error::Result<&'static str> {
    let (cluster_name, proxy_address, node_address) = path.into_inner();
    state.remove_node_from_cluster(cluster_name, proxy_address, node_address)
        .map(|()| "")
        .map_err(|e| error::ErrorBadRequest(e.description().to_string()))
}
