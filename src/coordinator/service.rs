use super::api::ApiService;
use super::broker::{MetaDataBroker, MetaManipulationBroker};
use super::core::{
    CoordinateError, FailureDetector, FailureHandler, MigrationStateSynchronizer,
    ParFailureDetector, ParFailureHandler, ParMigrationStateSynchronizer,
    ProxyMetaRespSynchronizer, ProxyMetaSynchronizer,
};
use super::detector::{
    BrokerFailureReporter, BrokerOrderedProxiesRetriever, BrokerProxiesRetriever,
    PingFailureDetector,
};
use super::migration::{BrokerMigrationCommitter, MigrationStateRespChecker};
use super::recover::{BrokerProxyFailureRetriever, ReplaceNodeHandler};
use super::sync::{BrokerMetaRetriever, ProxyMetaRespSender};
use crate::common::utils::ThreadSafe;
use crate::protocol::RedisClientFactory;
use arc_swap::ArcSwap;
use futures::future::select_all;
use futures::{Future, StreamExt};
use futures_timer::Delay;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

pub type BrokerAddresses = Arc<ArcSwap<Vec<String>>>;

#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    pub address: String,
    pub broker_addresses: BrokerAddresses,
    pub reporter_id: String,
    pub thread_number: usize,
    pub proxy_timeout: usize,
    pub enable_compression: bool,
}

impl CoordinatorConfig {
    pub fn get_broker_addresses(&self) -> Vec<String> {
        self.broker_addresses.lease().clone()
    }

    pub fn set_broker_addresses(&self, addresses: Vec<String>) {
        self.broker_addresses.store(Arc::new(addresses))
    }
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
    api_service: Arc<ApiService>,
}

type CoordResult = Result<(), CoordinateError>;

impl<DB: MetaDataBroker + ThreadSafe, MB: MetaManipulationBroker, F: RedisClientFactory>
    CoordinatorService<DB, MB, F>
{
    pub fn new(
        config: CoordinatorConfig,
        data_broker: Arc<DB>,
        mani_broker: Arc<MB>,
        client_factory: F,
    ) -> Self {
        let api_service = Arc::new(ApiService::new(Arc::new(config.clone())));
        Self {
            config,
            data_broker,
            mani_broker,
            client_factory: Arc::new(client_factory),
            api_service,
        }
    }

    pub async fn run(&self) -> Result<(), CoordinateError> {
        info!("coordinator config: {:?}", self.config);

        let futs: Vec<Pin<Box<dyn Future<Output = CoordResult> + Send>>> = vec![
            Box::pin(self.loop_detect()),
            Box::pin(self.loop_proxy_sync()),
            Box::pin(self.loop_failure_handler()),
            Box::pin(self.loop_migration_sync()),
            Box::pin(self.api_service.run()),
        ];

        let (res, _, _) = select_all(futs).await;
        error!("service stopped: {:?}", res);
        res.map(|_| ())
    }

    fn gen_detector(
        reporter_id: String,
        data_broker: Arc<DB>,
        client_factory: Arc<F>,
    ) -> impl FailureDetector {
        let retriever = BrokerProxiesRetriever::new(data_broker.clone());
        let checker = PingFailureDetector::new(client_factory);
        let reporter = BrokerFailureReporter::new(reporter_id, data_broker);
        ParFailureDetector::new(retriever, checker, reporter)
    }

    fn gen_proxy_meta_synchronizer(
        data_broker: Arc<DB>,
        client_factory: Arc<F>,
        enable_compression: bool,
    ) -> impl ProxyMetaSynchronizer {
        let proxy_retriever = BrokerOrderedProxiesRetriever::new(data_broker.clone());
        let meta_retriever = BrokerMetaRetriever::new(data_broker);
        let sender = ProxyMetaRespSender::new(client_factory, enable_compression);
        ProxyMetaRespSynchronizer::new(proxy_retriever, meta_retriever, sender)
    }

    fn gen_failure_handler(data_broker: Arc<DB>, mani_broker: Arc<MB>) -> impl FailureHandler {
        let proxy_retriever = BrokerProxyFailureRetriever::new(data_broker);
        let handler = ReplaceNodeHandler::new(mani_broker);
        ParFailureHandler::new(proxy_retriever, handler)
    }

    fn gen_migration_state_synchronizer(
        data_broker: Arc<DB>,
        mani_broker: Arc<MB>,
        client_factory: Arc<F>,
        enable_compression: bool,
    ) -> impl MigrationStateSynchronizer {
        let proxy_retriever = BrokerProxiesRetriever::new(data_broker.clone());
        let checker = MigrationStateRespChecker::new(client_factory.clone());
        let committer = BrokerMigrationCommitter::new(mani_broker);
        let meta_retriever = BrokerMetaRetriever::new(data_broker);
        let sender = ProxyMetaRespSender::new(client_factory, enable_compression);
        ParMigrationStateSynchronizer::new(
            proxy_retriever,
            checker,
            committer,
            meta_retriever,
            sender,
        )
    }

    async fn loop_detect(&self) -> Result<(), CoordinateError> {
        let data_broker = self.data_broker.clone();
        let client_factory = self.client_factory.clone();
        let reporter_id = self.config.reporter_id.clone();
        loop {
            trace!("start detecting failures");
            defer!(trace!("detecting finished a round"));
            if let Err(e) = Self::gen_detector(
                reporter_id.clone(),
                data_broker.clone(),
                client_factory.clone(),
            )
            .run()
            .await
            {
                error!("detector stream err {:?}", e);
            }
            Delay::new(Duration::from_secs(1)).await;
        }
    }

    async fn loop_proxy_sync(&self) -> Result<(), CoordinateError> {
        let data_broker = self.data_broker.clone();
        let client_factory = self.client_factory.clone();
        loop {
            trace!("start sync proxy meta data");
            defer!(trace!("proxy meta sync finished a round"));
            let sync = Self::gen_proxy_meta_synchronizer(
                data_broker.clone(),
                client_factory.clone(),
                self.config.enable_compression,
            );
            let mut s = sync.run();
            while let Some(r) = s.next().await {
                if let Err(e) = r {
                    error!("sync stream err {:?}", e);
                }
            }
            Delay::new(Duration::from_secs(1)).await;
        }
    }

    async fn loop_failure_handler(&self) -> Result<(), CoordinateError> {
        let data_broker = self.data_broker.clone();
        let mani_broker = self.mani_broker.clone();
        loop {
            trace!("start handling failures");
            defer!(trace!("handling failures finished a round"));
            let handler = Self::gen_failure_handler(data_broker.clone(), mani_broker.clone());
            let mut s = handler.run();
            while let Some(r) = s.next().await {
                if let Err(e) = r {
                    error!("failure handler stream err {:?}", e)
                }
            }
            Delay::new(Duration::from_secs(1)).await;
        }
    }

    async fn loop_migration_sync(&self) -> Result<(), CoordinateError> {
        let data_broker = self.data_broker.clone();
        let mani_broker = self.mani_broker.clone();
        let client_factory = self.client_factory.clone();
        loop {
            trace!("start handling migration sync");
            defer!(trace!("handling migration finished a round"));
            let sync = Self::gen_migration_state_synchronizer(
                data_broker.clone(),
                mani_broker.clone(),
                client_factory.clone(),
                self.config.enable_compression,
            );
            let mut s = sync.run();
            while let Some(r) = s.next().await {
                if let Err(e) = r {
                    error!("migration sync stream err {:?}", e)
                }
            }
            Delay::new(Duration::from_secs(1)).await;
        }
    }
}
