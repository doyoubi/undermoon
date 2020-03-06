use super::broker::{MetaDataBroker, MetaManipulationBroker};
use super::core::{CoordinateError, ProxyFailure, ProxyFailureHandler, ProxyFailureRetriever};
use futures::{Future, Stream, TryFutureExt, TryStreamExt};
use std::pin::Pin;
use std::sync::Arc;

pub struct BrokerProxyFailureRetriever<B: MetaDataBroker> {
    broker: Arc<B>,
}

impl<B: MetaDataBroker> BrokerProxyFailureRetriever<B> {
    pub fn new(broker: Arc<B>) -> Self {
        Self { broker }
    }
}

impl<B: MetaDataBroker> ProxyFailureRetriever for BrokerProxyFailureRetriever<B> {
    fn retrieve_proxy_failures<'s>(
        &'s self,
    ) -> Pin<Box<dyn Stream<Item = Result<String, CoordinateError>> + Send + 's>> {
        Box::pin(
            self.broker
                .get_failures()
                .map_err(CoordinateError::MetaData),
        )
    }
}

pub struct ReplaceNodeHandler<MB: MetaManipulationBroker> {
    mani_broker: Arc<MB>,
}

impl<MB: MetaManipulationBroker> ReplaceNodeHandler<MB> {
    pub fn new(mani_broker: Arc<MB>) -> Self {
        Self { mani_broker }
    }
}

impl<MB: MetaManipulationBroker> ProxyFailureHandler for ReplaceNodeHandler<MB> {
    fn handle_proxy_failure<'s>(
        &'s self,
        proxy_failure: ProxyFailure,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoordinateError>> + Send + 's>> {
        let proxy_failure2 = proxy_failure.clone();
        Box::pin(
            self.mani_broker
                .replace_proxy(proxy_failure.clone())
                .map_err(move |e| {
                    error!("failed to replace proxy {} {:?}", proxy_failure2, e);
                    CoordinateError::MetaMani(e)
                })
                .map_ok(move |new_host| {
                    info!(
                        "successfully replace {} with new host {:?}",
                        proxy_failure, new_host
                    );
                }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::super::broker::{MockMetaDataBroker, MockMetaManipulationBroker};
    use super::super::core::ParFailureHandler;
    use super::*;
    use crate::common::cluster::Proxy;
    use crate::coordinator::core::FailureHandler;
    use futures::{stream, StreamExt};
    use std::collections::HashMap;
    use tokio;

    fn gen_testing_dummy_proxy() -> Proxy {
        Proxy::new(
            "127.0.0.1:6000".to_string(),
            7799,
            vec![],
            vec![],
            vec![],
            HashMap::new(),
        )
    }

    #[tokio::test]
    async fn test_failure_retriever() {
        let mut mock_broker = MockMetaDataBroker::new();
        mock_broker
            .expect_get_failures()
            .times(1)
            .returning(move || Box::pin(stream::iter(vec![Ok("127.0.0.1:6000".to_string())])));
        let mock_broker = Arc::new(mock_broker);

        let retriever = BrokerProxyFailureRetriever::new(mock_broker);
        let s: Vec<_> = retriever.retrieve_proxy_failures().collect().await;
        assert_eq!(s.len(), 1);
    }

    #[tokio::test]
    async fn test_handler() {
        let mut mock_broker = MockMetaManipulationBroker::new();
        let failure = "127.0.0.1:6000";
        let failure2 = failure;
        mock_broker
            .expect_replace_proxy()
            .withf(move |f| f == failure2)
            .times(1)
            .returning(move |_| Box::pin(async { Ok(gen_testing_dummy_proxy()) }));
        let mock_broker = Arc::new(mock_broker);

        let handler = ReplaceNodeHandler::new(mock_broker);
        let res = handler.handle_proxy_failure(failure.to_string()).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_failure_handler() {
        let mut mock_data_broker = MockMetaDataBroker::new();
        mock_data_broker
            .expect_get_failures()
            .times(1)
            .returning(move || Box::pin(stream::iter(vec![Ok("127.0.0.1:6000".to_string())])));
        let mock_data_broker = Arc::new(mock_data_broker);

        let mut mock_mani_broker = MockMetaManipulationBroker::new();
        let failure = "127.0.0.1:6000";
        let failure2 = failure;
        mock_mani_broker
            .expect_replace_proxy()
            .withf(move |f| f == failure2)
            .times(1)
            .returning(move |_| Box::pin(async { Ok(gen_testing_dummy_proxy()) }));
        let mock_mani_broker = Arc::new(mock_mani_broker);

        let retriever = BrokerProxyFailureRetriever::new(mock_data_broker);
        let handler = ReplaceNodeHandler::new(mock_mani_broker);
        let failure_handler = ParFailureHandler::new(retriever, handler);
        let res: Vec<_> = failure_handler.run().collect().await;
        assert_eq!(res.len(), 1);
        assert!(res[0].is_ok());
    }
}
