use super::broker::{MetaDataBroker, MetaDataBrokerError};
use super::service::BrokerAddresses;
use crate::broker::MEM_BROKER_API_VERSION;
use crate::common::cluster::{Cluster, ClusterName, Proxy};
use crate::common::utils::vec_result_to_stream;
use futures::{future, stream, Future, FutureExt, Stream, StreamExt};
use serde_derive::Deserialize;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};

const PAGE_SIZE: usize = 100;

pub struct HttpMetaBroker {
    broker_addresses: BrokerAddresses,
    broker_index: AtomicUsize,
    client: reqwest::Client,
}

impl HttpMetaBroker {
    pub fn new(broker_addresses: BrokerAddresses, client: reqwest::Client) -> Self {
        HttpMetaBroker {
            broker_addresses,
            broker_index: AtomicUsize::new(0),
            client,
        }
    }
}

impl HttpMetaBroker {
    fn gen_url(&self, path: &str) -> Option<String> {
        let broker_addresses = self.broker_addresses.lease();
        let num = broker_addresses.len();
        let curr_index = self.broker_index.fetch_add(1, Ordering::Relaxed);
        let broker = broker_addresses.get(curr_index % num)?;
        let url = format!("http://{}{}{}", broker, MEM_BROKER_API_VERSION, path);
        Some(url)
    }

    async fn get_cluster_names_impl(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ClusterName>, MetaDataBrokerError> {
        let url = self
            .gen_url("/clusters/names")
            .ok_or_else(|| MetaDataBrokerError::NoBroker)?;
        let url = format!("{}?offset={}&limit={}", url, offset, limit);
        let response = self.client.get(&url).send().await.map_err(|e| {
            error!("failed to get cluster names {:?}", e);
            MetaDataBrokerError::RequestFailed
        })?;
        let ClusterNamesPayload { names } = response.json().await.map_err(|e| {
            error!("failed to get cluster names from json {:?}", e);
            MetaDataBrokerError::InvalidReply
        })?;
        Ok(names)
    }

    async fn get_cluster_impl(
        &self,
        name: ClusterName,
    ) -> Result<Option<Cluster>, MetaDataBrokerError> {
        let url = self
            .gen_url(&format!("/clusters/meta/{}", name))
            .ok_or_else(|| MetaDataBrokerError::NoBroker)?;
        let response = self.client.get(&url).send().await.map_err(|e| {
            error!("failed to get cluster {:?}", e);
            MetaDataBrokerError::RequestFailed
        })?;
        let ClusterPayload { cluster } = response.json().await.map_err(|e| {
            error!("failed to get cluster from json {:?}", e);
            MetaDataBrokerError::InvalidReply
        })?;
        Ok(cluster)
    }

    async fn get_proxy_addresses_impl(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<String>, MetaDataBrokerError> {
        let url = self
            .gen_url("/proxies/addresses")
            .ok_or_else(|| MetaDataBrokerError::NoBroker)?;
        let url = format!("{}?offset={}&limit={}", url, offset, limit);
        let response = self.client.get(&url).send().await.map_err(|e| {
            error!("failed to get proxy addresses {:?}", e);
            MetaDataBrokerError::RequestFailed
        })?;
        let ProxyAddressesPayload { addresses } = response.json().await.map_err(|e| {
            error!("failed to get proxy adddresses from json {:?}", e);
            MetaDataBrokerError::InvalidReply
        })?;
        Ok(addresses)
    }

    async fn get_proxy_impl(&self, address: String) -> Result<Option<Proxy>, MetaDataBrokerError> {
        let url = self
            .gen_url(&format!("/proxies/meta/{}", address))
            .ok_or_else(|| MetaDataBrokerError::NoBroker)?;
        let response = self.client.get(&url).send().await.map_err(|e| {
            error!("failed to get proxy {:?}", e);
            MetaDataBrokerError::RequestFailed
        })?;
        let ProxyPayload { proxy } = response.json().await.map_err(move |e| {
            error!("failed to get proxy {} from json {:?}", address, e);
            MetaDataBrokerError::InvalidReply
        })?;
        Ok(proxy)
    }

    async fn add_failure_impl(
        &self,
        address: String,
        reporter_id: String,
    ) -> Result<(), MetaDataBrokerError> {
        let url = self
            .gen_url(&format!("/failures/{}/{}", address, reporter_id))
            .ok_or_else(|| MetaDataBrokerError::NoBroker)?;
        let response = self.client.post(&url).send().await.map_err(|e| {
            error!("failed to add failures {:?}", e);
            MetaDataBrokerError::RequestFailed
        })?;
        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            let result = response.text().await;
            match result {
                Err(e) => {
                    error!("Failed to get body: {:?}", e);
                    Err(MetaDataBrokerError::InvalidReply)
                }
                Ok(body) => {
                    error!("Error body: {:?}", body);
                    Err(MetaDataBrokerError::InvalidReply)
                }
            }
        }
    }

    async fn get_failures_impl(&self) -> Result<Vec<String>, MetaDataBrokerError> {
        let url = self
            .gen_url("/failures")
            .ok_or_else(|| MetaDataBrokerError::NoBroker)?;
        let response = self.client.get(&url).send().await.map_err(|e| {
            error!("Failed to get failures {:?}", e);
            MetaDataBrokerError::RequestFailed
        })?;
        let FailuresPayload { addresses } = response.json().await.map_err(|e| {
            error!("Failed to get failures from json {:?}", e);
            MetaDataBrokerError::InvalidReply
        })?;
        Ok(addresses)
    }

    async fn get_failed_proxies_impl(&self) -> Result<Vec<String>, MetaDataBrokerError> {
        let url = self
            .gen_url("/proxies/failed/addresses")
            .ok_or_else(|| MetaDataBrokerError::NoBroker)?;
        let response = self.client.get(&url).send().await.map_err(|e| {
            error!("Failed to get failed proxies {:?}", e);
            MetaDataBrokerError::RequestFailed
        })?;
        let FailedProxiesPayload { addresses } = response.json().await.map_err(|e| {
            error!("Failed to get failed proxies from json {:?}", e);
            MetaDataBrokerError::InvalidReply
        })?;
        Ok(addresses)
    }
}

impl MetaDataBroker for HttpMetaBroker {
    fn get_cluster_names<'s>(
        &'s self,
    ) -> Pin<Box<dyn Stream<Item = Result<ClusterName, MetaDataBrokerError>> + Send + 's>> {
        let s = stream::iter(0..)
            .then(move |page| {
                let offset = page * PAGE_SIZE;
                self.get_cluster_names_impl(offset, PAGE_SIZE)
            })
            .take_while(|names_res| match names_res {
                Err(_) => future::ready(true),
                Ok(names) => future::ready(!names.is_empty()),
            })
            .map(vec_result_to_stream)
            .flatten();
        Box::pin(s)
    }

    fn get_cluster<'s>(
        &'s self,
        name: ClusterName,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Cluster>, MetaDataBrokerError>> + Send + 's>>
    {
        Box::pin(self.get_cluster_impl(name))
    }

    fn get_proxy_addresses<'s>(
        &'s self,
    ) -> Pin<Box<dyn Stream<Item = Result<String, MetaDataBrokerError>> + Send + 's>> {
        let s = stream::iter(0..)
            .then(move |page| {
                let offset = page * PAGE_SIZE;
                self.get_proxy_addresses_impl(offset, PAGE_SIZE)
            })
            .take_while(|names_res| match names_res {
                Err(_) => future::ready(true),
                Ok(names) => future::ready(!names.is_empty()),
            })
            .map(vec_result_to_stream)
            .flatten();
        Box::pin(s)
    }

    fn get_proxy<'s>(
        &'s self,
        address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Proxy>, MetaDataBrokerError>> + Send + 's>> {
        Box::pin(self.get_proxy_impl(address))
    }

    fn add_failure<'s>(
        &'s self,
        address: String,
        reporter_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaDataBrokerError>> + Send + 's>> {
        Box::pin(self.add_failure_impl(address, reporter_id))
    }

    fn get_failures<'s>(
        &'s self,
    ) -> Pin<Box<dyn Stream<Item = Result<String, MetaDataBrokerError>> + Send + 's>> {
        Box::pin(
            self.get_failures_impl()
                .map(vec_result_to_stream)
                .flatten_stream(),
        )
    }

    fn get_failed_proxies<'s>(
        &'s self,
    ) -> Pin<Box<dyn Stream<Item = Result<String, MetaDataBrokerError>> + Send + 's>> {
        Box::pin(
            self.get_failed_proxies_impl()
                .map(vec_result_to_stream)
                .flatten_stream(),
        )
    }
}

#[derive(Deserialize, Serialize)]
pub struct ClusterNamesPayload {
    pub names: Vec<ClusterName>,
}

#[derive(Deserialize, Serialize)]
pub struct ClusterPayload {
    pub cluster: Option<Cluster>,
}

#[derive(Deserialize, Serialize)]
pub struct ProxyAddressesPayload {
    pub addresses: Vec<String>,
}

#[derive(Deserialize, Serialize)]
pub struct ProxyPayload {
    pub proxy: Option<Proxy>,
}

#[derive(Deserialize, Serialize)]
pub struct FailuresPayload {
    pub addresses: Vec<String>,
}

#[derive(Deserialize, Serialize)]
pub struct FailedProxiesPayload {
    pub addresses: Vec<String>,
}
