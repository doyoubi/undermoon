use std::iter;
use futures::{Future, stream, Stream};
use futures::future::select_all;
use ::common::utils::ThreadSafe;
use ::protocol::{RedisClient, SimpleRedisClient};
use super::broker::{MetaDataBroker, MetaManipulationBroker};
use super::core::{FailureDetector, SeqFailureDetector, CoordinateError, HostMetaSynchronizer, HostMetaRespSynchronizer,
                  FailureHandler, SeqFailureHandler};
use super::detector::{BrokerProxiesRetriever, PingFailureDetector, BrokerFailureReporter};
use super::sync::{HostMetaRespSender, PeerMetaRespSender, LocalMetaRetriever, PeerMetaRetriever};
use super::recover::{BrokerProxyFailureRetriever, BrokerNodeFailureRetriever, ReplaceNodeHandler};

#[derive(Clone)]
pub struct CoordinatorService<
        DB: MetaDataBroker + ThreadSafe + Clone,
        MB: MetaManipulationBroker + Clone,
        C: RedisClient + ThreadSafe + Clone> {
    reporter_id: String,
    data_broker: DB,
    mani_broker: MB,
    client: C,
}

impl<DB: MetaDataBroker + ThreadSafe + Clone,
    MB: MetaManipulationBroker + Clone,
    C: RedisClient + ThreadSafe + Clone> CoordinatorService<DB, MB, C> {
    pub fn new(reporter_id: String, data_broker: DB, mani_broker: MB, client: C) -> Self {
        Self{
            reporter_id,
            data_broker,
            mani_broker,
            client,
        }
    }
}

impl<DB: MetaDataBroker + ThreadSafe + Clone,
    MB: MetaManipulationBroker + Clone,
    C: RedisClient + ThreadSafe + Clone> CoordinatorService<DB, MB, C> {
    pub fn run(&self) -> impl Future<Item = (), Error = CoordinateError> {
        select_all(vec![
            self.loop_detect(),
            self.loop_local_sync(),
            self.loop_peer_sync(),
            self.loop_failure_handler(),
        ])
            .map(|_| error!("service stopped"))
            .map_err(|(e, _idx, _others)| {error!("service stopped: {:?}", e); e})
    }

    fn gen_detector(&self) -> impl FailureDetector {
        let retriever = BrokerProxiesRetriever::new(self.data_broker.clone());
        let checker = PingFailureDetector::new(self.client.clone());
        let reporter = BrokerFailureReporter::new("test_id".to_string(), self.data_broker.clone());
        SeqFailureDetector::new(retriever, checker, reporter)
    }

    fn gen_local_meta_synchronizer(&self) -> impl HostMetaSynchronizer {
        let proxy_retriever = BrokerProxiesRetriever::new(self.data_broker.clone());
        let meta_retriever = LocalMetaRetriever::new(self.data_broker.clone());
        let sender = HostMetaRespSender::new(self.client.clone());
        HostMetaRespSynchronizer::new(proxy_retriever, meta_retriever, sender)
    }

    fn gen_peer_meta_synchronizer(&self) -> impl HostMetaSynchronizer {
        let proxy_retriever = BrokerProxiesRetriever::new(self.data_broker.clone());
        let meta_retriever = PeerMetaRetriever::new(self.data_broker.clone());
        let sender = PeerMetaRespSender::new(self.client.clone());
        HostMetaRespSynchronizer::new(proxy_retriever, meta_retriever, sender)
    }

    fn gen_failure_handler(&self) -> impl FailureHandler {
        let proxy_retriever = BrokerProxyFailureRetriever::new(self.data_broker.clone());
        let node_retriever = BrokerNodeFailureRetriever::new(self.data_broker.clone());
        let handler = ReplaceNodeHandler::new(self.data_broker.clone(), self.mani_broker.clone());
        SeqFailureHandler::new(proxy_retriever, node_retriever, handler)
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
                debug!("start sync local meta data");
                defer!(debug!("local sync finished a round"));
                service.gen_local_meta_synchronizer().run().collect().map(move |_| service)
            }).map(|_| debug!("loop_sync stopped"))
        )
    }

    fn loop_peer_sync(&self) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        let s = stream::iter_ok(iter::repeat(()));
        Box::new(
            s.fold(self.clone(), |service, ()| {
                debug!("start sync peer meta data");
                defer!(debug!("peer sync finished a round"));
                service.gen_peer_meta_synchronizer().run().collect().map(move |_| service)
            }).map(|_| debug!("loop_sync stopped"))
        )
    }

    fn loop_failure_handler(&self) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        let s = stream::iter_ok(iter::repeat(()));
        Box::new(
            s.fold(self.clone(), |service, ()| {
                debug!("start handling failures");
                defer!(debug!("handling finished a round"));
                service.gen_failure_handler().run().collect().map(move |_| service)
            }).map(|_| debug!("loop_failure_handler stopped"))
        )
    }
}
