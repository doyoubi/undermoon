use std::iter;
use futures::{Future, stream, Stream};
use ::common::utils::ThreadSafe;
use ::protocol::{RedisClient, SimpleRedisClient};
use super::broker::MetaDataBroker;
use super::core::{FailureDetector, SeqFailureDetector, CoordinateError};
use super::detector::{BrokerProxiesRetriever, PingFailureDetector, BrokerFailureReporter};

#[derive(Clone)]
pub struct CoordinatorService<B: MetaDataBroker + ThreadSafe + Clone, C: RedisClient + ThreadSafe + Clone> {
    reporter_id: String,
    broker: B,
    client: C,
}

impl<B: MetaDataBroker + ThreadSafe + Clone, C: RedisClient + ThreadSafe + Clone> CoordinatorService<B, C> {
    pub fn new(reporter_id: String, broker: B, client: C) -> Self {
        Self{
            reporter_id,
            broker,
            client,
        }
    }
}

impl<B: MetaDataBroker + ThreadSafe + Clone, C: RedisClient + ThreadSafe + Clone> CoordinatorService<B, C> {
    pub fn run(&self) -> impl Future<Item = (), Error = CoordinateError> {
        self.loop_detect()
    }

    fn gen_detector(&self) -> impl FailureDetector {
        let retriever = BrokerProxiesRetriever::new(self.broker.clone());
        let checker = PingFailureDetector::new(self.client.clone());
        let reporter = BrokerFailureReporter::new("test_id".to_string(), self.broker.clone());
        SeqFailureDetector::new(retriever, checker, reporter)
    }

    fn loop_detect(&self) -> impl Future<Item = (), Error = CoordinateError> {
        let s = stream::iter_ok(iter::repeat(()));
        s.fold(self.clone(), |service, ()| {
            println!("start detecting failures");
            service.gen_detector().run().collect().map(move |_| service)
        }).map(|_| ())
    }
}
