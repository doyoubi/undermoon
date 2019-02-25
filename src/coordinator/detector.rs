use futures::{Future, Stream, future, stream};
use protocol::{RedisClient, SimpleRedisClient, RedisClientError};
use ::common::cluster::{Host, Node, Cluster};
use super::cluster::FullMetaData;
use super::broker::{MetaDataBroker, MetaDataBrokerError};
use super::core::{ProxyFailure, ProxiesRetriever, FailureChecker, FailureReporter, CoordinateError, SeqFailureDetector, FailureDetector};

pub struct BrokerProxiesRetriever<B: MetaDataBroker> {
    meta_data_broker: B
}

impl<B: MetaDataBroker> BrokerProxiesRetriever<B> {
    pub fn new(meta_data_broker: B) -> Self {
        Self{ meta_data_broker }
    }
}

impl<B: MetaDataBroker> ProxiesRetriever for BrokerProxiesRetriever<B> {
    fn retrieve_proxies(&self) -> Box<dyn Stream<Item = String, Error = CoordinateError> + Send> {
        Box::new(
            self.meta_data_broker.get_host_addresses().map_err(|e| CoordinateError::MetaData(e))
        )
    }
}

pub struct PingFailureDetector<C: RedisClient + Sync + Send + 'static> {
    client: C
}

impl<C: RedisClient + Sync + Send + 'static> PingFailureDetector<C> {
    pub fn new(client: C) -> Self {
        Self{ client }
    }
}

impl<C: RedisClient + Sync + Send + 'static> FailureChecker for PingFailureDetector<C> {
    fn check(&self, address: String) -> Box<dyn Future<Item = Option<String>, Error = CoordinateError> + Send> {
        let ping_command = vec!["ping".to_string().into_bytes()];
        Box::new(
            self.client.execute(address.clone(), ping_command).then(move |result| {
                match result {
                    Ok(_) => future::ok(None),
                    Err(_) => future::ok(Some(address)),
                }
            })
        )
    }
}

pub struct BrokerFailureReporter<B: MetaDataBroker> {
    reporter_id: String,
    meta_data_broker: B
}

impl<B: MetaDataBroker> BrokerFailureReporter<B> {
    pub fn new(reporter_id: String, meta_data_broker: B) -> Self {
        Self{ reporter_id, meta_data_broker }
    }
}

impl<B: MetaDataBroker> FailureReporter for BrokerFailureReporter<B> {
    fn report(&self, address: String) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        Box::new(
            self.meta_data_broker.add_failure(address, self.reporter_id.clone())
                .map_err(|e| CoordinateError::MetaData(e))
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use ::common::utils::ThreadSafe;
    use ::protocol::{RedisClient, BinSafeStr, Resp, Array};
    use super::*;

    const NODE1: &'static str = "127.0.0.1:7000";
    const NODE2: &'static str = "127.0.0.1:7001";

    #[derive(Clone)]
    struct DummyClient;

    impl ThreadSafe for DummyClient {}

    impl RedisClient for DummyClient {
        fn execute(&self, address: String, command: Vec<BinSafeStr>) -> Box<dyn Future<Item = Resp, Error =RedisClientError> + Send> {
            if address == NODE1 {
                Box::new(future::ok(Resp::Arr(Array::Nil)))
            } else {
                Box::new(future::err(RedisClientError::InvalidReply))
            }
        }
    }

    #[derive(Clone)]
    struct DummyMetaBroker {
        reported_failures: Arc<Mutex<Vec<String>>>,
    }

    impl DummyMetaBroker {
        fn new() -> Self {
            Self { reported_failures: Arc::new(Mutex::new(vec![])) }
        }
    }

    impl ThreadSafe for DummyMetaBroker {}

    impl MetaDataBroker for DummyMetaBroker {
        fn get_cluster_names(&self) -> Box<dyn Stream<Item = String, Error = MetaDataBrokerError> + Send> {
            Box::new(stream::empty())
        }
        fn get_cluster(&self, name: String) -> Box<dyn Future<Item = Option<Cluster>, Error = MetaDataBrokerError> + Send> {
            Box::new(future::ok(None))
        }
        fn get_host_addresses(&self) -> Box<dyn Stream<Item = String, Error = MetaDataBrokerError> + Send> {
            Box::new(stream::iter(vec![
                Ok::<String, MetaDataBrokerError>(NODE1.to_string()),
                Ok::<String, MetaDataBrokerError>(NODE2.to_string()),
            ]))
        }
        fn get_host(&self, address: String) -> Box<dyn Future<Item = Option<Host>, Error = MetaDataBrokerError> + Send> {
            Box::new(future::ok(None))
        }
        fn get_peer(&self, address: String) -> Box<dyn Future<Item = Option<Host>, Error = MetaDataBrokerError> + Send> {
            Box::new(future::ok(None))
        }
        fn add_failure(&self, address: String, reporter_id: String) -> Box<dyn Future<Item = (), Error = MetaDataBrokerError> + Send> {
            self.reported_failures.lock().unwrap().push(address);
            Box::new(future::ok(()))
        }
    }

    #[test]
    fn test_detector() {
        let broker = DummyMetaBroker::new();
        let retriever = BrokerProxiesRetriever::new(broker.clone());
        let checker = PingFailureDetector::new(DummyClient{});
        let reporter = BrokerFailureReporter::new("test_id".to_string(), broker.clone());
        let detector = SeqFailureDetector::new(retriever, checker, reporter);
        detector.run().into_future().wait();
        let failed_nodes = broker.reported_failures.lock().unwrap().clone();
        assert_eq!(1, failed_nodes.len());
        assert_eq!(NODE2, failed_nodes[0]);
    }
}
