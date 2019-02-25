use std::iter;
use futures::{Future, stream, Stream};
use futures::future::select_all;
use ::common::utils::ThreadSafe;
use ::protocol::{RedisClient, SimpleRedisClient};
use super::broker::MetaDataBroker;
use super::core::{FailureDetector, SeqFailureDetector, CoordinateError, HostMetaSynchronizer, HostMetaRespSynchronizer};
use super::detector::{BrokerProxiesRetriever, PingFailureDetector, BrokerFailureReporter};
use super::sync::{HostMetaRespSender, LocalMetaRetriever, PeerMetaRetriever};

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
        select_all(vec![
            self.loop_detect(),
            self.loop_local_sync(),
            self.loop_peer_sync(),
        ])
            .map(|_| error!("service stopped"))
            .map_err(|(e, _idx, _others)| {error!("service stopped: {:?}", e); e})
    }

    fn gen_detector(&self) -> impl FailureDetector {
        let retriever = BrokerProxiesRetriever::new(self.broker.clone());
        let checker = PingFailureDetector::new(self.client.clone());
        let reporter = BrokerFailureReporter::new("test_id".to_string(), self.broker.clone());
        SeqFailureDetector::new(retriever, checker, reporter)
    }

    fn gen_local_meta_synchronizer(&self) -> impl HostMetaSynchronizer {
        let proxy_retriever = BrokerProxiesRetriever::new(self.broker.clone());
        let meta_retriever = LocalMetaRetriever::new(self.broker.clone());
        let sender = HostMetaRespSender::new(self.client.clone());
        HostMetaRespSynchronizer::new(proxy_retriever, meta_retriever, sender)
    }

    fn gen_peer_meta_synchronizer(&self) -> impl HostMetaSynchronizer {
        let proxy_retriever = BrokerProxiesRetriever::new(self.broker.clone());
        let meta_retriever = PeerMetaRetriever::new(self.broker.clone());
        let sender = HostMetaRespSender::new(self.client.clone());
        HostMetaRespSynchronizer::new(proxy_retriever, meta_retriever, sender)
    }

    fn loop_detect(&self) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        let s = stream::iter_ok(iter::repeat(()));
        Box::new(
            s.fold(self.clone(), |service, ()| {
                debug!("start detecting failures");
                defer!(debug!("detecting finished a round"));
                service.gen_detector().run().collect().map(move |_| service)
            }).map(|_| debug!("loop_detect stopped"))
        )
    }

    fn loop_local_sync(&self) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        let s = stream::iter_ok(iter::repeat(()));
        Box::new(
            s.fold(self.clone(), |service, ()| {
                debug!("start sync meta data");
                defer!(debug!("sync finished a round"));
                service.gen_local_meta_synchronizer().run().collect().map(move |_| service)
            }).map(|_| debug!("loop_sync stopped"))
        )
    }

    fn loop_peer_sync(&self) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        let s = stream::iter_ok(iter::repeat(()));
        Box::new(
            s.fold(self.clone(), |service, ()| {
                debug!("start sync meta data");
                defer!(debug!("sync finished a round"));
                service.gen_peer_meta_synchronizer().run().collect().map(move |_| service)
            }).map(|_| debug!("loop_sync stopped"))
        )
    }
}
