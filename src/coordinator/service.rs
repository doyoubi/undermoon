use super::broker::{MetaDataBroker, MetaManipulationBroker};
use super::core::{
    CoordinateError, FailureDetector, FailureHandler, HostMetaRespSynchronizer,
    HostMetaSynchronizer, MigrationStateSynchronizer, SeqFailureDetector, SeqFailureHandler,
    SeqMigrationStateSynchronizer,
};
use super::detector::{BrokerFailureReporter, BrokerProxiesRetriever, PingFailureDetector};
use super::migration::{BrokerMigrationCommitter, MigrationStateRespChecker};
use super::recover::{BrokerNodeFailureRetriever, BrokerProxyFailureRetriever, ReplaceNodeHandler};
use super::sync::{HostMetaRespSender, LocalMetaRetriever, PeerMetaRespSender, PeerMetaRetriever};
use common::utils::ThreadSafe;
use futures::future::select_all;
use futures::{future, stream, Future, Stream};
use futures_timer::Delay;
use protocol::RedisClientFactory;
use std::iter;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    pub broker_address: String,
    pub reporter_id: String,
}

pub struct CoordinatorService<
    DB: MetaDataBroker + ThreadSafe,
    MB: MetaManipulationBroker,
    F: RedisClientFactory,
> {
    config: CoordinatorConfig,
    data_broker: Arc<DB>,
    mani_broker: Arc<MB>,
    client_factory: Arc<F>,
}

impl<
        DB: MetaDataBroker + ThreadSafe + Clone,
        MB: MetaManipulationBroker + Clone,
        F: RedisClientFactory,
    > CoordinatorService<DB, MB, F>
{
    pub fn new(
        config: CoordinatorConfig,
        data_broker: Arc<DB>,
        mani_broker: Arc<MB>,
        client_factory: F,
    ) -> Self {
        Self {
            config,
            data_broker,
            mani_broker,
            client_factory: Arc::new(client_factory),
        }
    }

    pub fn run(&self) -> impl Future<Item = (), Error = CoordinateError> {
        info!("coordinator config: {:?}", self.config);

        select_all(vec![
            self.loop_detect(),
            self.loop_local_sync(),
            self.loop_peer_sync(),
            self.loop_failure_handler(),
            self.loop_migration_sync(),
        ])
        .map(|_| error!("service stopped"))
        .map_err(|(e, _idx, _others)| {
            error!("service stopped: {:?}", e);
            e
        })
    }

    fn gen_detector(
        reporter_id: String,
        data_broker: Arc<DB>,
        client_factory: Arc<F>,
    ) -> impl FailureDetector {
        let retriever = BrokerProxiesRetriever::new(data_broker.clone());
        let checker = PingFailureDetector::new(client_factory);
        let reporter = BrokerFailureReporter::new(reporter_id, data_broker);
        SeqFailureDetector::new(retriever, checker, reporter)
    }

    fn gen_local_meta_synchronizer(
        data_broker: Arc<DB>,
        client_factory: Arc<F>,
    ) -> impl HostMetaSynchronizer {
        let proxy_retriever = BrokerProxiesRetriever::new(data_broker.clone());
        let meta_retriever = LocalMetaRetriever::new(data_broker);
        let sender = HostMetaRespSender::new(client_factory);
        HostMetaRespSynchronizer::new(proxy_retriever, meta_retriever, sender)
    }

    fn gen_peer_meta_synchronizer(
        data_broker: Arc<DB>,
        client_factory: Arc<F>,
    ) -> impl HostMetaSynchronizer {
        let proxy_retriever = BrokerProxiesRetriever::new(data_broker.clone());
        let meta_retriever = PeerMetaRetriever::new(data_broker);
        let sender = PeerMetaRespSender::new(client_factory);
        HostMetaRespSynchronizer::new(proxy_retriever, meta_retriever, sender)
    }

    fn gen_failure_handler(data_broker: Arc<DB>, mani_broker: Arc<MB>) -> impl FailureHandler {
        let proxy_retriever = BrokerProxyFailureRetriever::new(data_broker.clone());
        let node_retriever = BrokerNodeFailureRetriever::new(data_broker.clone());
        let handler = ReplaceNodeHandler::new(data_broker, mani_broker);
        SeqFailureHandler::new(proxy_retriever, node_retriever, handler)
    }

    fn gen_migration_state_synchronizer(
        data_broker: Arc<DB>,
        mani_broker: Arc<MB>,
        client_factory: Arc<F>,
    ) -> impl MigrationStateSynchronizer {
        let proxy_retriever = BrokerProxiesRetriever::new(data_broker.clone());
        let checker = MigrationStateRespChecker::new(client_factory);
        let committer = BrokerMigrationCommitter::new(mani_broker);
        SeqMigrationStateSynchronizer::new(proxy_retriever, checker, committer)
    }

    fn loop_detect(&self) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        let s = stream::iter_ok(iter::repeat(()));
        let data_broker = self.data_broker.clone();
        let client_factory = self.client_factory.clone();
        let reporter_id = self.config.reporter_id.clone();
        Box::new(
            s.fold(
                (reporter_id, data_broker, client_factory),
                |(reporter_id, data_broker, client_factory), ()| {
                    debug!("start detecting failures");
                    defer!(debug!("detecting finished a round"));
                    let delay = Delay::new(Duration::from_secs(1)).map_err(CoordinateError::Io);
                    Self::gen_detector(
                        reporter_id.clone(),
                        data_broker.clone(),
                        client_factory.clone(),
                    )
                    .run()
                    .collect()
                    .then(move |res| {
                        if let Err(e) = res {
                            error!("detector stream err {:?}", e)
                        }
                        future::ok(())
                    })
                    .join(delay)
                    .then(move |_| future::ok((reporter_id, data_broker, client_factory)))
                },
            )
            .map(|_| debug!("loop_detect stopped")),
        )
    }

    fn loop_local_sync(&self) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        let s = stream::iter_ok(iter::repeat(()));
        let data_broker = self.data_broker.clone();
        let client_factory = self.client_factory.clone();
        Box::new(
            s.fold(
                (data_broker, client_factory),
                |(data_broker, client_factory), ()| {
                    debug!("start sync local meta data");
                    defer!(debug!("local sync finished a round"));
                    let delay = Delay::new(Duration::from_secs(1)).map_err(CoordinateError::Io);
                    Self::gen_local_meta_synchronizer(data_broker.clone(), client_factory.clone())
                        .run()
                        .collect()
                        .then(move |res| {
                            if let Err(e) = res {
                                error!("sync stream err {:?}", e)
                            }
                            future::ok(())
                        })
                        .join(delay)
                        .then(move |_| future::ok((data_broker, client_factory)))
                },
            )
            .map(|_| debug!("loop_sync stopped")),
        )
    }

    fn loop_peer_sync(&self) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        let s = stream::iter_ok(iter::repeat(()));
        let data_broker = self.data_broker.clone();
        let client_factory = self.client_factory.clone();
        Box::new(
            s.fold(
                (data_broker, client_factory),
                |(data_broker, client_factory), ()| {
                    debug!("start sync peer meta data");
                    defer!(debug!("peer sync finished a round"));
                    let delay = Delay::new(Duration::from_secs(1)).map_err(CoordinateError::Io);
                    Self::gen_peer_meta_synchronizer(data_broker.clone(), client_factory.clone())
                        .run()
                        .collect()
                        .then(|res| {
                            if let Err(e) = res {
                                error!("peer sync stream err {:?}", e)
                            }
                            future::ok(())
                        })
                        .join(delay)
                        .then(move |_| future::ok((data_broker, client_factory)))
                },
            )
            .map(|_| debug!("loop_sync stopped")),
        )
    }

    fn loop_failure_handler(&self) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        let s = stream::iter_ok(iter::repeat(()));
        let data_broker = self.data_broker.clone();
        let mani_broker = self.mani_broker.clone();
        Box::new(
            s.fold(
                (data_broker, mani_broker),
                |(data_broker, mani_broker), ()| {
                    debug!("start handling failures");
                    defer!(debug!("handling failures finished a round"));
                    let delay = Delay::new(Duration::from_secs(1)).map_err(CoordinateError::Io);
                    Self::gen_failure_handler(data_broker.clone(), mani_broker.clone())
                        .run()
                        .collect()
                        .then(|res| {
                            if let Err(e) = res {
                                error!("failure handler stream err {:?}", e)
                            }
                            future::ok(())
                        })
                        .join(delay)
                        .then(move |_| future::ok((data_broker, mani_broker)))
                },
            )
            .map(|_| debug!("loop_failure_handler stopped")),
        )
    }

    fn loop_migration_sync(&self) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        let s = stream::iter_ok(iter::repeat(()));
        let data_broker = self.data_broker.clone();
        let mani_broker = self.mani_broker.clone();
        let client_factory = self.client_factory.clone();
        Box::new(
            s.fold(
                (data_broker, mani_broker, client_factory),
                |(data_broker, mani_broker, client_factory), ()| {
                    debug!("start handling migration sync");
                    defer!(debug!("handling migration finished a round"));
                    let delay = Delay::new(Duration::from_secs(1)).map_err(CoordinateError::Io);
                    Self::gen_migration_state_synchronizer(
                        data_broker.clone(),
                        mani_broker.clone(),
                        client_factory.clone(),
                    )
                    .run()
                    .collect()
                    .then(|res| {
                        if let Err(e) = res {
                            error!("migration sync stream err {:?}", e)
                        }
                        future::ok(())
                    })
                    .join(delay)
                    .then(move |_| future::ok((data_broker, mani_broker, client_factory)))
                },
            )
            .map(|_| debug!("loop_migration_sync stopped")),
        )
    }
}
