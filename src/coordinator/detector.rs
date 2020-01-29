use super::broker::MetaDataBroker;
use super::core::{CoordinateError, FailureChecker, FailureReporter, ProxiesRetriever};
use crate::protocol::{RedisClient, RedisClientFactory};
use futures::{Future, Stream, TryStreamExt, TryFutureExt};
use std::sync::Arc;
use std::pin::Pin;

pub struct BrokerProxiesRetriever<B: MetaDataBroker> {
    meta_data_broker: Arc<B>,
}

impl<B: MetaDataBroker> BrokerProxiesRetriever<B> {
    pub fn new(meta_data_broker: Arc<B>) -> Self {
        Self { meta_data_broker }
    }
}

impl<B: MetaDataBroker> ProxiesRetriever for BrokerProxiesRetriever<B> {
    fn retrieve_proxies<'s>(&'s self) -> Pin<Box<dyn Stream<Item = Result<String, CoordinateError>> + Send + 's>> {
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

        let ping_command = vec!["PING".to_string().into_bytes()];
        match client.execute(ping_command).await {
            Ok(_) => Ok(None),
            Err(err) => {
                error!("PingFailureDetector::check failed to send PING: {:?}", err);
                Ok(Some(address))
            }
        }
    }

    async fn check_impl(
        &self,
        address: String,
    ) -> Result<Option<String>, CoordinateError> {
        const RETRY: usize = 3;
        for i in 1..=RETRY {
            match self.ping(address.clone()).await {
                Ok(None) => return Ok(None),
                res if i == RETRY => return res,
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
    use super::super::broker::{MetaDataBroker, MetaDataBrokerError};
    use super::super::core::{FailureDetector, SeqFailureDetector};
    use super::*;
    use crate::common::cluster::{Cluster, Host};
    use crate::common::utils::ThreadSafe;
    use crate::protocol::{Array, BinSafeStr, RedisClient, RedisClientError, Resp, RespVec};
    use futures::{future, stream};
    use tokio::runtime::Runtime;
    use std::sync::{Arc, Mutex};
    use std::pin::Pin;

    const NODE1: &'static str = "127.0.0.1:7000";
    const NODE2: &'static str = "127.0.0.1:7001";

    #[derive(Debug)]
    struct DummyClient {
        address: String,
    }

    impl ThreadSafe for DummyClient {}

    impl RedisClient for DummyClient {
        fn execute(
            &mut self,
            _command: Vec<BinSafeStr>,
        ) -> Pin<Box<dyn Future<Output = Result<RespVec, RedisClientError>> + Send>> {
            if self.address == NODE1 {
                Box::pin(future::ok(Resp::Arr(Array::Nil)))
            } else {
                Box::pin(future::err(RedisClientError::InvalidReply))
            }
        }
    }

    struct DummyClientFactory;

    impl ThreadSafe for DummyClientFactory {}

    impl RedisClientFactory for DummyClientFactory {
        type Client = DummyClient;

        fn create_client(
            &self,
            address: String,
        ) -> Pin<Box<dyn Future<Output = Result<Self::Client, RedisClientError>> + Send>> {
            Box::pin(future::ok(DummyClient { address }))
        }
    }

    #[derive(Clone)]
    struct DummyMetaBroker {
        reported_failures: Arc<Mutex<Vec<String>>>,
    }

    impl DummyMetaBroker {
        fn new() -> Self {
            Self {
                reported_failures: Arc::new(Mutex::new(vec![])),
            }
        }
    }

    impl ThreadSafe for DummyMetaBroker {}

    impl MetaDataBroker for DummyMetaBroker {
        fn get_cluster_names<'s>(
            &'s self,
        ) -> Pin<Box<dyn Stream<Item = Result<String, MetaDataBrokerError>> + Send +'s>> {
            Box::pin(stream::iter(vec![]))
        }
        fn get_cluster<'s>(
            &'s self,
            _name: String,
        ) -> Pin<Box<dyn Future<Output = Result<Option<Cluster>, MetaDataBrokerError>> + Send + 's>> {
            Box::pin(future::ok(None))
        }
        fn get_host_addresses<'s>(
            &'s self,
        ) -> Pin<Box<dyn Stream<Item = Result<String, MetaDataBrokerError>> + Send + 's>> {
            Box::pin(stream::iter(vec![Ok(NODE1.to_string()), Ok(NODE2.to_string())]))
        }
        fn get_host<'s>(
            &'s self,
            _address: String,
        ) -> Pin<Box<dyn Future<Output = Result<Option<Host>, MetaDataBrokerError>> + Send + 's>> {
            Box::pin(future::ok(None))
        }
        fn add_failure<'s>(
            &'s self,
            address: String,
            _reporter_id: String,
        ) -> Pin<Box<dyn Future<Output = Result<(), MetaDataBrokerError>> + Send + 's>> {
            self.reported_failures
                .lock()
                .expect("dummy_add_failure")
                .push(address);
            Box::pin(future::ok(()))
        }
        fn get_failures<'s>(
            &'s self,
        ) -> Pin<Box<dyn Stream<Item = Result<String, MetaDataBrokerError>> + Send + 's>> {
            Box::pin(stream::iter(vec![]))
        }
    }

    #[test]
    fn test_detector() {
        let broker = Arc::new(DummyMetaBroker::new());
        let retriever = BrokerProxiesRetriever::new(broker.clone());
        let checker = PingFailureDetector::new(Arc::new(DummyClientFactory {}));
        let reporter = BrokerFailureReporter::new("test_id".to_string(), broker.clone());
        let detector = SeqFailureDetector::new(retriever, checker, reporter);

        let mut rt = Runtime::new().expect("test_detector");
        let res = rt.block_on(detector.run().into_future());
        assert!(res.is_ok());
        let failed_nodes = broker
            .reported_failures
            .lock()
            .expect("test_detector")
            .clone();
        assert_eq!(1, failed_nodes.len());
        assert_eq!(NODE2, failed_nodes[0]);
    }
}
