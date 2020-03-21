use super::broker::{MetaDataBroker, MetaDataBrokerError};
use crate::common::cluster::{Cluster, DBName, Proxy};
use crate::common::utils::vec_result_to_stream;
use crate::broker::MEM_BROKER_API_VERSION;
use futures::{Future, FutureExt, Stream};
use reqwest;
use serde_derive::Deserialize;
use std::pin::Pin;

#[derive(Clone)]
pub struct HttpMetaBroker {
    broker_address: String,
    client: reqwest::Client,
}

impl HttpMetaBroker {
    pub fn new(broker_address: String, client: reqwest::Client) -> Self {
        HttpMetaBroker {
            broker_address,
            client,
        }
    }
}

impl HttpMetaBroker {
    fn gen_url(&self, path: &str) -> String {
        format!("http://{}/{}{}", self.broker_address, MEM_BROKER_API_VERSION, path)
    }

    async fn get_cluster_names_impl(&self) -> Result<Vec<DBName>, MetaDataBrokerError> {
        let url = self.gen_url("/clusters/names");
        let response = self.client.get(&url).send().await.map_err(|e| {
            error!("failed to get cluster names {:?}", e);
            MetaDataBrokerError::InvalidReply
        })?;
        let ClusterNamesPayload { names } = response.json().await.map_err(|e| {
            error!("failed to get cluster names from json {:?}", e);
            MetaDataBrokerError::InvalidReply
        })?;
        Ok(names)
    }

    async fn get_cluster_impl(&self, name: DBName) -> Result<Option<Cluster>, MetaDataBrokerError> {
        let url = self.gen_url(&format!("/clusters/meta/{}", name));
        let response = self.client.get(&url).send().await.map_err(|e| {
            error!("failed to get cluster {:?}", e);
            MetaDataBrokerError::InvalidReply
        })?;
        let ClusterPayload { cluster } = response.json().await.map_err(|e| {
            error!("failed to get cluster from json {:?}", e);
            MetaDataBrokerError::InvalidReply
        })?;
        Ok(cluster)
    }

    async fn get_host_addresses_impl(&self) -> Result<Vec<String>, MetaDataBrokerError> {
        let url = self.gen_url("/proxies/addresses");
        let response = self.client.get(&url).send().await.map_err(|e| {
            error!("failed to get host addresses {:?}", e);
            MetaDataBrokerError::InvalidReply
        })?;
        let ProxyAddressesPayload { addresses } = response.json().await.map_err(|e| {
            error!("failed to get host adddresses from json {:?}", e);
            MetaDataBrokerError::InvalidReply
        })?;
        Ok(addresses)
    }

    async fn get_host_impl(&self, address: String) -> Result<Option<Proxy>, MetaDataBrokerError> {
        let url = self.gen_url(&format!("/proxies/meta/{}", address));
        let response = self.client.get(&url).send().await.map_err(|e| {
            error!("failed to get host {:?}", e);
            MetaDataBrokerError::InvalidReply
        })?;
        let ProxyPayload { host } = response.json().await.map_err(move |e| {
            error!("failed to get host {} from json {:?}", address, e);
            MetaDataBrokerError::InvalidReply
        })?;
        Ok(host)
    }

    async fn add_failure_impl(
        &self,
        address: String,
        reporter_id: String,
    ) -> Result<(), MetaDataBrokerError> {
        let url = self.gen_url(&format!("/failures/{}/{}", address, reporter_id));
        let response = self.client.post(&url).send().await.map_err(|e| {
            error!("failed to add failures {:?}", e);
            MetaDataBrokerError::InvalidReply
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
        let url = self.gen_url("/failures");
        let response = self.client.get(&url).send().await.map_err(|e| {
            error!("Failed to get failures {:?}", e);
            MetaDataBrokerError::InvalidReply
        })?;
        let FailuresPayload { addresses } = response.json().await.map_err(|e| {
            error!("Failed to get cluster names from json {:?}", e);
            MetaDataBrokerError::InvalidReply
        })?;
        Ok(addresses)
    }
}

impl MetaDataBroker for HttpMetaBroker {
    fn get_cluster_names<'s>(
        &'s self,
    ) -> Pin<Box<dyn Stream<Item = Result<DBName, MetaDataBrokerError>> + Send + 's>> {
        Box::pin(
            self.get_cluster_names_impl()
                .map(vec_result_to_stream)
                .flatten_stream(),
        )
    }

    fn get_cluster<'s>(
        &'s self,
        name: DBName,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Cluster>, MetaDataBrokerError>> + Send + 's>>
    {
        Box::pin(self.get_cluster_impl(name))
    }

    fn get_host_addresses<'s>(
        &'s self,
    ) -> Pin<Box<dyn Stream<Item = Result<String, MetaDataBrokerError>> + Send + 's>> {
        Box::pin(
            self.get_host_addresses_impl()
                .map(vec_result_to_stream)
                .flatten_stream(),
        )
    }

    fn get_host<'s>(
        &'s self,
        address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Proxy>, MetaDataBrokerError>> + Send + 's>> {
        Box::pin(self.get_host_impl(address))
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
}

#[derive(Deserialize, Serialize)]
pub struct ClusterNamesPayload {
    pub names: Vec<DBName>,
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
    pub host: Option<Proxy>,
}

#[derive(Deserialize, Serialize)]
pub struct FailuresPayload {
    pub addresses: Vec<String>,
}
