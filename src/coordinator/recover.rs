use super::broker::{MetaDataBroker, MetaManipulationBroker};
use super::core::{CoordinateError, ProxyFailure, ProxyFailureHandler, ProxyFailureRetriever};
use futures::{Future, Stream, TryFutureExt, FutureExt};
use std::sync::Arc;
use std::pin::Pin;

pub struct BrokerProxyFailureRetriever<B: MetaDataBroker> {
    broker: Arc<B>,
}

impl<B: MetaDataBroker> BrokerProxyFailureRetriever<B> {
    pub fn new(broker: Arc<B>) -> Self {
        Self { broker }
    }
}

impl<B: MetaDataBroker> ProxyFailureRetriever for BrokerProxyFailureRetriever<B> {
    fn retrieve_proxy_failures(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<String, CoordinateError>> + Send>> {
        Box::new(
            self.broker
                .get_failures()
                .map_err(CoordinateError::MetaData),
        )
    }
}

pub struct ReplaceNodeHandler<DB: MetaDataBroker, MB: MetaManipulationBroker + Clone> {
    _data_broker: Arc<DB>,
    mani_broker: Arc<MB>,
}

impl<DB: MetaDataBroker, MB: MetaManipulationBroker + Clone> ReplaceNodeHandler<DB, MB> {
    pub fn new(_data_broker: Arc<DB>, mani_broker: Arc<MB>) -> Self {
        Self {
            _data_broker,
            mani_broker,
        }
    }
}

impl<DB: MetaDataBroker, MB: MetaManipulationBroker + Clone> ProxyFailureHandler
    for ReplaceNodeHandler<DB, MB>
{
    fn handle_proxy_failure(
        &self,
        proxy_failure: ProxyFailure,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoordinateError>> + Send>> {
        Box::new(
            self.mani_broker
                .replace_proxy(proxy_failure)
                .map_err(|e| {
                    error!("failed to replace proxy {:?}", e);
                    CoordinateError::MetaMani(e)
                })
                .map(|new_host| {
                    info!("successfully replace it with new host {:?}", new_host);
                }),
        )
    }
}
