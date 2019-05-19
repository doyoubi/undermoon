use super::store::{MetaStore, MetaStoreError};
use ::common::version::UNDERMOON_VERSION;
use ::coordinator::http_meta_broker::HostAddressesPayload;
use actix_web::{http, App, State, HttpRequest, Json, Responder, error};
use std::sync::{Arc, RwLock};
use std::error::Error;

pub fn gen_app(service: Arc<MemBrokerService>) -> App<Arc<MemBrokerService>> {
    App::with_state(service)
        .prefix("/api")
        .resource("/version", |r| r.method(http::Method::GET).f(get_version))
        .resource("/hosts/addresses", |r| {
            r.method(http::Method::GET).f(get_host_addresses)
        })
        .resource("/hosts/nodes", |r| r.method(http::Method::PUT).with(add_host))
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

    pub fn get_host_addresses(&self) -> Vec<String> {
        self.store
            .read()
            .expect("MemBrokerService::get_host_addresses")
            .get_hosts()
    }

    pub fn add_hosts(&self, host_resource: HostResource) -> Result<(), MetaStoreError> {
        let HostResource{proxy_address, nodes} = host_resource;
        self.store.write().expect("MemBrokerService::add_hosts").add_hosts(proxy_address, nodes)
    }
}

fn get_version(_req: &HttpRequest<Arc<MemBrokerService>>) -> &'static str {
    UNDERMOON_VERSION
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

fn add_host((host_resource, state): (Json<HostResource>, State<Arc<MemBrokerService>>)) -> error::Result<&'static str>  {
    Ok(
        state.add_hosts(host_resource.into_inner())
            .map(|()| "")
            .map_err(|e| error::ErrorBadRequest(e.description().to_string()))?
    )
}
