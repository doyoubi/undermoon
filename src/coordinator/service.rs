use std::iter;
use futures::{Future, stream, Stream};
use futures::future::join_all;
use ::common::utils::ThreadSafe;
use ::protocol::{RedisClient, SimpleRedisClient};
use super::broker::MetaDataBroker;
use super::core::{FailureDetector, SeqFailureDetector, CoordinateError, HostMetaSynchronizer, HostMetaRespSynchronizer};
use super::detector::{BrokerProxiesRetriever, PingFailureDetector, BrokerFailureReporter};
use super::sync::HostMetaRespSender;

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
        self.loop_detect().select(self.loop_sync())
            .map(|_| error!("service exited"))
            .map_err(|(e, _another)| {
                error!("service exited with error {:?}", e);
                e
            })
    }

    fn gen_detector(&self) -> impl FailureDetector {
        let retriever = BrokerProxiesRetriever::new(self.broker.clone());
        let checker = PingFailureDetector::new(self.client.clone());
        let reporter = BrokerFailureReporter::new("test_id".to_string(), self.broker.clone());
        SeqFailureDetector::new(retriever, checker, reporter)
    }

    fn gen_synchronizer(&self) -> impl HostMetaSynchronizer {
        let retriever = BrokerProxiesRetriever::new(self.broker.clone());
        let sender = HostMetaRespSender::new(self.client.clone());
        let broker = self.broker.clone();
        HostMetaRespSynchronizer::new(retriever, sender, broker)
    }

    fn loop_detect(&self) -> impl Future<Item = (), Error = CoordinateError> {
        let s = stream::iter_ok(iter::repeat(()));
        s.fold(self.clone(), |service, ()| {
            debug!("start detecting failures");
            defer!(debug!("detecting finished a round"));
            service.gen_detector().run().collect().map(move |_| service)
        }).map(|_| debug!("loop_detect stopped"))
    }

    fn loop_sync(&self) -> impl Future<Item = (), Error = CoordinateError> {
        let s = stream::iter_ok(iter::repeat(()));
        s.fold(self.clone(), |service, ()| {
            debug!("start sync meta data");
            defer!(debug!("sync finished a round"));
            service.gen_synchronizer().run().collect().map(move |_| service)
        }).map(|_| debug!("loop_sync stopped"))
    }
}
