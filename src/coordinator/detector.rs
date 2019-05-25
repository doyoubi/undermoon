use super::broker::MetaDataBroker;
use super::core::{CoordinateError, FailureChecker, FailureReporter, ProxiesRetriever};
use futures::{future, Future, Stream};
use protocol::{RedisClient, RedisClientFactory};
use std::sync::Arc;

pub struct BrokerProxiesRetriever<B: MetaDataBroker> {
    meta_data_broker: Arc<B>,
}

impl<B: MetaDataBroker> BrokerProxiesRetriever<B> {
    pub fn new(meta_data_broker: Arc<B>) -> Self {
        Self { meta_data_broker }
    }
}

impl<B: MetaDataBroker> ProxiesRetriever for BrokerProxiesRetriever<B> {
    fn retrieve_proxies(&self) -> Box<dyn Stream<Item = String, Error = CoordinateError> + Send> {
        Box::new(
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
}

impl<F: RedisClientFactory> FailureChecker for PingFailureDetector<F> {
    fn check(
        &self,
        address: String,
    ) -> Box<dyn Future<Item = Option<String>, Error = CoordinateError> + Send + 'static> {
        let address_clone = address.clone();
        let client_fut = self.client_factory.create_client(address.clone());
        Box::new(
            client_fut
                .and_then(|client| {
                    let ping_command = vec!["ping".to_string().into_bytes()];
                    client
                        .execute(ping_command)
                        .then(move |result| match result {
                            Ok(_) => future::ok(None),
                            Err(_) => future::ok(Some(address)),
                        })
                })
                .or_else(move |_err| {
                    Ok(Some(address_clone))
                }),
        )
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
    fn report(
        &self,
        address: String,
    ) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        Box::new(
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
    use common::cluster::{Cluster, Host};
    use common::utils::ThreadSafe;
    use futures::stream;
    use protocol::{Array, BinSafeStr, RedisClient, RedisClientError, Resp};
    use std::sync::{Arc, Mutex};

    const NODE1: &'static str = "127.0.0.1:7000";
    const NODE2: &'static str = "127.0.0.1:7001";

    #[derive(Debug)]
    struct DummyClient {
        address: String,
    }

    impl ThreadSafe for DummyClient {}

    impl RedisClient for DummyClient {
        fn execute(
            self,
            _command: Vec<BinSafeStr>,
        ) -> Box<dyn Future<Item = (Self, Resp), Error = RedisClientError> + Send> {
            if self.address == NODE1 {
                Box::new(future::ok((self, Resp::Arr(Array::Nil))))
            } else {
                Box::new(future::err(RedisClientError::InvalidReply))
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
        ) -> Box<dyn Future<Item = Self::Client, Error = RedisClientError> + Send> {
            Box::new(future::ok(DummyClient { address }))
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
        fn get_cluster_names(
            &self,
        ) -> Box<dyn Stream<Item = String, Error = MetaDataBrokerError> + Send> {
            Box::new(stream::empty())
        }
        fn get_cluster(
            &self,
            _name: String,
        ) -> Box<dyn Future<Item = Option<Cluster>, Error = MetaDataBrokerError> + Send> {
            Box::new(future::ok(None))
        }
        fn get_host_addresses(
            &self,
        ) -> Box<dyn Stream<Item = String, Error = MetaDataBrokerError> + Send> {
            Box::new(stream::iter_ok(vec![NODE1.to_string(), NODE2.to_string()]))
        }
        fn get_host(
            &self,
            _address: String,
        ) -> Box<dyn Future<Item = Option<Host>, Error = MetaDataBrokerError> + Send> {
            Box::new(future::ok(None))
        }
        fn get_peer(
            &self,
            _address: String,
        ) -> Box<dyn Future<Item = Option<Host>, Error = MetaDataBrokerError> + Send> {
            Box::new(future::ok(None))
        }
        fn add_failure(
            &self,
            address: String,
            _reporter_id: String,
        ) -> Box<dyn Future<Item = (), Error = MetaDataBrokerError> + Send> {
            self.reported_failures
                .lock()
                .expect("dummy_add_failure")
                .push(address);
            Box::new(future::ok(()))
        }
        fn get_failures(
            &self,
        ) -> Box<dyn Stream<Item = String, Error = MetaDataBrokerError> + Send> {
            Box::new(stream::iter_ok(vec![]))
        }
    }

    #[test]
    fn test_detector() {
        let broker = Arc::new(DummyMetaBroker::new());
        let retriever = BrokerProxiesRetriever::new(broker.clone());
        let checker = PingFailureDetector::new(Arc::new(DummyClientFactory {}));
        let reporter = BrokerFailureReporter::new("test_id".to_string(), broker.clone());
        let detector = SeqFailureDetector::new(retriever, checker, reporter);
        let res = detector.run().into_future().wait();
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
