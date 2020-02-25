use super::broker::MetaDataBroker;
use super::core::{CoordinateError, FailureChecker, FailureReporter, ProxiesRetriever};
use crate::protocol::{RedisClient, RedisClientFactory};
use futures::{Future, Stream, TryFutureExt, TryStreamExt};
use std::pin::Pin;
use std::sync::Arc;

// TODO: Remove this retriever if there's not logic inside it.
pub struct BrokerProxiesRetriever<B: MetaDataBroker> {
    meta_data_broker: Arc<B>,
}

impl<B: MetaDataBroker> BrokerProxiesRetriever<B> {
    pub fn new(meta_data_broker: Arc<B>) -> Self {
        Self { meta_data_broker }
    }
}

impl<B: MetaDataBroker> ProxiesRetriever for BrokerProxiesRetriever<B> {
    fn retrieve_proxies<'s>(
        &'s self,
    ) -> Pin<Box<dyn Stream<Item = Result<String, CoordinateError>> + Send + 's>> {
        Box::pin(
            self.meta_data_broker
                .get_host_addresses()
                .map_err(CoordinateError::MetaData),
        )
    }
}

pub struct PingFailureDetector<F: RedisClientFactory> {
    client_factory: Arc<F>,
}

impl<F: RedisClientFactory> PingFailureDetector<F> {
    pub fn new(client_factory: Arc<F>) -> Self {
        Self { client_factory }
    }

    async fn ping(&self, address: String) -> Result<Option<String>, CoordinateError> {
        let mut client = match self.client_factory.create_client(address.clone()).await {
            Ok(client) => client,
            Err(err) => {
                error!("PingFailureDetector::check failed to connect: {:?}", err);
                return Ok(Some(address));
            }
        };

        // The connection pool might get a stale connection.
        // Return err instead for retry.
        let ping_command = vec!["PING".to_string().into_bytes()];
        match client.execute_single(ping_command).await {
            Ok(_) => Ok(None),
            Err(err) => {
                error!("PingFailureDetector::check failed to send PING: {:?}", err);
                Err(CoordinateError::Redis(err))
            }
        }
    }

    async fn check_impl(&self, address: String) -> Result<Option<String>, CoordinateError> {
        const RETRY: usize = 3;
        for i in 1..=RETRY {
            match self.ping(address.clone()).await {
                Ok(None) => return Ok(None),
                _ if i == RETRY => return Ok(Some(address)),
                _ => continue,
            }
        }
        Ok(Some(address))
    }
}

impl<F: RedisClientFactory> FailureChecker for PingFailureDetector<F> {
    fn check<'s>(
        &'s self,
        address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, CoordinateError>> + Send + 's>> {
        Box::pin(self.check_impl(address))
    }
}

pub struct BrokerFailureReporter<B: MetaDataBroker> {
    reporter_id: String,
    meta_data_broker: Arc<B>,
}

impl<B: MetaDataBroker> BrokerFailureReporter<B> {
    pub fn new(reporter_id: String, meta_data_broker: Arc<B>) -> Self {
        Self {
            reporter_id,
            meta_data_broker,
        }
    }
}

impl<B: MetaDataBroker> FailureReporter for BrokerFailureReporter<B> {
    fn report<'s>(
        &'s self,
        address: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoordinateError>> + Send + 's>> {
        Box::pin(
            self.meta_data_broker
                .add_failure(address, self.reporter_id.clone())
                .map_err(CoordinateError::MetaData),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::super::broker::{MetaDataBrokerError, MockMetaDataBroker};
    use super::super::core::{FailureDetector, SeqFailureDetector};
    use super::*;
    use crate::protocol::{
        Array, BinSafeStr, OptionalMulti, RedisClient, RedisClientError, Resp, RespVec,
    };
    use futures::{future, stream, StreamExt};
    use std::pin::Pin;
    use std::sync::Arc;
    use tokio;

    const NODE1: &'static str = "127.0.0.1:7000";
    const NODE2: &'static str = "127.0.0.1:7001";

    #[derive(Debug)]
    struct DummyClient {
        address: String,
    }

    impl RedisClient for DummyClient {
        fn execute<'s>(
            &'s mut self,
            _command: OptionalMulti<Vec<BinSafeStr>>,
        ) -> Pin<
            Box<dyn Future<Output = Result<OptionalMulti<RespVec>, RedisClientError>> + Send + 's>,
        > {
            if self.address == NODE1 {
                // only works for single command
                Box::pin(future::ok(OptionalMulti::Single(
                    Resp::Arr(Array::Nil).into(),
                )))
            } else {
                Box::pin(future::err(RedisClientError::InvalidReply))
            }
        }
    }

    struct DummyClientFactory;

    impl RedisClientFactory for DummyClientFactory {
        type Client = DummyClient;

        fn create_client(
            &self,
            address: String,
        ) -> Pin<Box<dyn Future<Output = Result<Self::Client, RedisClientError>> + Send>> {
            Box::pin(future::ok(DummyClient { address }))
        }
    }

    #[tokio::test]
    async fn test_detector() {
        let mut mock_broker = MockMetaDataBroker::new();

        let addresses: Vec<String> = vec![NODE1, NODE2]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let addresses_clone = addresses.clone();
        mock_broker
            .expect_get_host_addresses()
            .returning(move || Box::pin(stream::iter(addresses_clone.clone().into_iter().map(Ok))));
        mock_broker
            .expect_add_failure()
            .withf(|address: &String, _| address == NODE2)
            .times(1)
            .returning(|_, _| Box::pin(future::ok(())));

        let broker = Arc::new(mock_broker);
        let retriever = BrokerProxiesRetriever::new(broker.clone());
        let checker = PingFailureDetector::new(Arc::new(DummyClientFactory {}));
        let reporter = BrokerFailureReporter::new("test_id".to_string(), broker.clone());
        let detector = SeqFailureDetector::new(retriever, checker, reporter);

        let res = detector.run().into_future().await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_detector_partial_error() {
        let mut mock_broker = MockMetaDataBroker::new();

        mock_broker.expect_get_host_addresses().returning(move || {
            let results = vec![
                Err(MetaDataBrokerError::InvalidReply),
                Ok(NODE1.to_string()),
                Ok(NODE2.to_string()),
            ];
            Box::pin(stream::iter(results))
        });
        mock_broker
            .expect_add_failure()
            .withf(|address: &String, _| address == NODE2)
            .times(1)
            .returning(|_, _| Box::pin(future::ok(())));

        let broker = Arc::new(mock_broker);
        let retriever = BrokerProxiesRetriever::new(broker.clone());
        let checker = PingFailureDetector::new(Arc::new(DummyClientFactory {}));
        let reporter = BrokerFailureReporter::new("test_id".to_string(), broker.clone());
        let detector = SeqFailureDetector::new(retriever, checker, reporter);

        let res = detector.run().into_future().await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_proxy_retriever() {
        let mut mock_broker = MockMetaDataBroker::new();
        let addresses: Vec<String> = vec!["host1:port1", "host2:port2"]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let addresses_clone = addresses.clone();
        mock_broker
            .expect_get_host_addresses()
            .returning(move || Box::pin(stream::iter(addresses_clone.clone().into_iter().map(Ok))));
        let broker = Arc::new(mock_broker);
        let retriever = BrokerProxiesRetriever::new(broker);
        let addrs: Vec<Result<String, CoordinateError>> =
            retriever.retrieve_proxies().collect().await;
        let addrs: Vec<String> = addrs.into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(addrs, addresses);
    }
}
