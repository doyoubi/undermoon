use super::broker::{MetaDataBroker, MetaDataBrokerError};
use crate::common::cluster::{Cluster, Host};
use crate::common::utils::ThreadSafe;
use futures::{stream, Future, Stream, FutureExt, TryFutureExt};
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

impl ThreadSafe for HttpMetaBroker {}

impl HttpMetaBroker {
    async fn get_cluster_names_impl(
        &self,
    ) -> Result<Vec<String>, MetaDataBrokerError> {
        let url = format!("http://{}/api/clusters/names", self.broker_address);
        let response = self.client.get(&url).send().await.map_err(|e|{
            error!("failed to get cluster names {:?}", e);
            MetaDataBrokerError::InvalidReply
        })?;
        let ClusterNamesPayload { names } = response
            .json().await.map_err(|e| {
            error!("failed to get cluster names from json {:?}", e);
            MetaDataBrokerError::InvalidReply
        })?;
        Ok(names)
    }

    async fn get_cluster_impl(
        &self,
        name: String,
    ) -> Result<Option<Cluster>, MetaDataBrokerError> {
        let url = format!("http://{}/api/clusters/meta/{}", self.broker_address, name);
        let response = self.client.get(&url).send().await.map_err(|e| {
            error!("failed to get cluster {:?}", e);
            MetaDataBrokerError::InvalidReply
        })?;
        let ClusterPayload { cluster } = response
            .json().await
            .map_err(|e| {
                error!("failed to get cluster from json {:?}", e);
                MetaDataBrokerError::InvalidReply
            })?;
        Ok(cluster)
    }

    async fn get_host_addresses_impl(
        &self,
    ) -> Result<Vec<String>, MetaDataBrokerError> {
        let url = format!("http://{}/api/proxies/addresses", self.broker_address);
        let response = self.client.get(&url).send().await.map_err(|e| {
            error!("failed to get host addresses {:?}", e);
            MetaDataBrokerError::InvalidReply
        })?;
        let HostAddressesPayload { addresses } = response
            .json().await
            .map_err(|e| {
                error!("failed to get host adddresses from json {:?}", e);
                MetaDataBrokerError::InvalidReply
            })?;
        Ok(addresses)
    }

    async fn get_host_impl(
        &self,
        address: String,
    ) -> Result<Option<Host>, MetaDataBrokerError> {
        let url = format!(
            "http://{}/api/proxies/meta/{}",
            self.broker_address, address
        );
        let response = self.client.get(&url).send().await.map_err(|e| {
            error!("failed to get host {:?}", e);
            MetaDataBrokerError::InvalidReply
        })?;
        let HostPayload { host } = response
            .json().await
            .map_err(move |e| {
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
        let url = format!(
            "http://{}/api/failures/{}/{}",
            self.broker_address, address, reporter_id
        );
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
        let url = format!("http://{}/api/failures", self.broker_address);
        let response = self.client.get(&url).send().await
            .map_err(|e| {
                error!("Failed to get failures {:?}", e);
                MetaDataBrokerError::InvalidReply
            })?;
        let FailuresPayload { addresses } = response
            .json()
            .await
            .map_err(|e| {
                error!("Failed to get cluster names from json {:?}", e);
                MetaDataBrokerError::InvalidReply
            })?;
        Ok(addresses)
    }
}

impl MetaDataBroker for HttpMetaBroker {
    fn get_cluster_names(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<String, MetaDataBrokerError>> + Send>> {
        Box::pin(self.get_cluster_names_impl().map(|names| stream::iter(names.map(Ok))).flatten_stream())
    }

    fn get_cluster(
        &self,
        name: String,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Cluster>, MetaDataBrokerError>> + Send>> {
        Box::pin(self.get_cluster_impl(name))
    }

    fn get_host_addresses(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<String, MetaDataBrokerError>> + Send>> {
        Box::pin(self.get_host_addresses_impl().map(|addrs| stream::iter(addrs.map(Ok))).flatten_stream())
    }

    fn get_host(
        &self,
        address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Host>, MetaDataBrokerError>> + Send>> {
        Box::pin(self.get_host_impl(address))
    }

    fn add_failure(
        &self,
        address: String,
        reporter_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), MetaDataBrokerError>> + Send>> {
        Box::pin(self.add_failure_impl(address, reporter_id))
    }

    fn get_failures(&self) -> Pin<Box<dyn Stream<Item = Result<String, MetaDataBrokerError>> + Send>> {
        Box::pin(self.get_failures_impl().map(|fs| stream::iter(fs.map(Ok))).flatten_stream())
    }
}

#[derive(Deserialize, Serialize)]
pub struct ClusterNamesPayload {
    pub names: Vec<String>,
}

#[derive(Deserialize, Serialize)]
pub struct ClusterPayload {
    pub cluster: Option<Cluster>,
}

#[derive(Deserialize, Serialize)]
pub struct HostAddressesPayload {
    pub addresses: Vec<String>,
}

#[derive(Deserialize, Serialize)]
pub struct HostPayload {
    pub host: Option<Host>,
}

#[derive(Deserialize, Serialize)]
pub struct FailuresPayload {
    pub addresses: Vec<String>,
}
