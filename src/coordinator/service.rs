use std::iter;
use std::time::Duration;
use futures::{future, Future, stream, Stream};
use futures::future::select_all;
use futures_timer::Delay;
use ::common::utils::ThreadSafe;
use ::protocol::{RedisClient, SimpleRedisClient};
use super::broker::{MetaDataBroker, MetaManipulationBroker};
use super::core::{FailureDetector, SeqFailureDetector, CoordinateError, HostMetaSynchronizer, HostMetaRespSynchronizer,
                  FailureHandler, SeqFailureHandler};
use super::detector::{BrokerProxiesRetriever, PingFailureDetector, BrokerFailureReporter};
use super::sync::{HostMetaRespSender, PeerMetaRespSender, LocalMetaRetriever, PeerMetaRetriever};
use super::recover::{BrokerProxyFailureRetriever, BrokerNodeFailureRetriever, ReplaceNodeHandler};

#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    pub broker_address: String,
    pub reporter_id: String,
}

#[derive(Clone)]
pub struct CoordinatorService<
        DB: MetaDataBroker + ThreadSafe + Clone,
        MB: MetaManipulationBroker + Clone,
        C: RedisClient + ThreadSafe + Clone> {
    config: CoordinatorConfig,
    data_broker: DB,
    mani_broker: MB,
    client: C,
}

impl<DB: MetaDataBroker + ThreadSafe + Clone,
    MB: MetaManipulationBroker + Clone,
    C: RedisClient + ThreadSafe + Clone> CoordinatorService<DB, MB, C> {

    pub fn new(config: CoordinatorConfig, data_broker: DB, mani_broker: MB, client: C) -> Self {
        Self{
            config,
            data_broker,
            mani_broker,
            client,
        }
    }

    pub fn run(&self) -> impl Future<Item = (), Error = CoordinateError> {
        info!("coordinator config: {:?}", self.config);

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
        let reporter = BrokerFailureReporter::new(self.config.reporter_id.clone(), self.data_broker.clone());
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
                let delay = Delay::new(Duration::from_secs(1))
                    .map_err(CoordinateError::Io);
                service.gen_detector().run().collect()
                    .then(move |res| {
                        if let Err(e) = res {
                            error!("detector stream err {:?}", e)
                        }
                        future::ok(())
                    })
                    .join(delay)
                    .then(move |_| future::ok(service))
            }).map(|_| debug!("loop_detect stopped"))
        )
    }

    fn loop_local_sync(&self) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        let s = stream::iter_ok(iter::repeat(()));
        Box::new(
            s.fold(self.clone(), |service, ()| {
                debug!("start sync local meta data");
                defer!(debug!("local sync finished a round"));
                let delay = Delay::new(Duration::from_secs(1))
                    .map_err(CoordinateError::Io);
                service.gen_local_meta_synchronizer().run().collect()
                    .then(move |res| {
                        if let Err(e) = res {
                            error!("sync stream err {:?}", e)
                        }
                        future::ok(())
                    })
                    .join(delay)
                    .then(move |_| future::ok(service))
            }).map(|_| debug!("loop_sync stopped"))
        )
    }

    fn loop_peer_sync(&self) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        let s = stream::iter_ok(iter::repeat(()));
        Box::new(
            s.fold(self.clone(), |service, ()| {
                debug!("start sync peer meta data");
                defer!(debug!("peer sync finished a round"));
                let delay = Delay::new(Duration::from_secs(1))
                    .map_err(CoordinateError::Io);
                service.gen_peer_meta_synchronizer().run().collect()
                    .then(|res| {
                        if let Err(e) = res {
                            error!("peer sync stream err {:?}", e)
                        }
                        future::ok(())
                    })
                    .join(delay)
                    .then(move |_| future::ok(service))
            }).map(|_| debug!("loop_sync stopped"))
        )
    }

    fn loop_failure_handler(&self) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        let s = stream::iter_ok(iter::repeat(()));
        Box::new(
            s.fold(self.clone(), |service, ()| {
                debug!("start handling failures");
                defer!(debug!("handling finished a round"));
                let delay = Delay::new(Duration::from_secs(1))
                    .map_err(CoordinateError::Io);
                service.gen_failure_handler().run().collect()
                    .then(|res| {
                        if let Err(e) = res {
                            error!("failure handler stream err {:?}", e)
                        }
                        future::ok(())
                    })
                    .join(delay)
                    .then(move |_| future::ok(service))
            }).map(|_| debug!("loop_failure_handler stopped"))
        )
    }
}
