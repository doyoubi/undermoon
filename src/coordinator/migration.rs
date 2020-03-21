use super::broker::MetaManipulationBroker;
use super::core::{CoordinateError, MigrationCommitter, MigrationStateChecker};
use crate::common::cluster::MigrationTaskMeta;
use crate::common::utils::vec_result_to_stream;
use crate::protocol::{Array, BulkStr, Resp};
use crate::protocol::{RedisClient, RedisClientFactory, RespVec};
use futures::{Future, FutureExt, Stream, TryFutureExt};
use std::pin::Pin;
use std::str;
use std::sync::Arc;

pub struct MigrationStateRespChecker<F: RedisClientFactory> {
    client_factory: Arc<F>,
}

impl<F: RedisClientFactory> MigrationStateRespChecker<F> {
    pub fn new(client_factory: Arc<F>) -> Self {
        Self { client_factory }
    }

    fn parse_migration_task_meta(element: &RespVec) -> Option<MigrationTaskMeta> {
        match element {
            Resp::Bulk(BulkStr::Str(s)) => {
                let data = str::from_utf8(&s).ok()?;
                let mut it = data
                    .split(' ')
                    .map(ToString::to_string)
                    .collect::<Vec<String>>()
                    .into_iter()
                    .peekable();
                MigrationTaskMeta::from_strings(&mut it)
            }
            others => {
                error!("invalid migration task meta {:?}", others);
                None
            }
        }
    }
}

impl<F: RedisClientFactory> MigrationStateRespChecker<F> {
    async fn check_impl(&self, address: String) -> Result<Vec<MigrationTaskMeta>, CoordinateError> {
        let mut client = self
            .client_factory
            .create_client(address.clone())
            .await
            .map_err(CoordinateError::Redis)?;
        let cmd = vec!["UMCTL".to_string(), "INFOMGR".to_string()]
            .into_iter()
            .map(String::into_bytes)
            .collect();
        let resp = client
            .execute_single(cmd)
            .await
            .map_err(CoordinateError::Redis)?;

        match resp {
            Resp::Arr(Array::Arr(arr)) => {
                let mut metadata = vec![];
                for element in arr.into_iter() {
                    match Self::parse_migration_task_meta(&element) {
                        Some(meta) => metadata.push(meta),
                        None => {
                            error!("failed to parse migration task meta data {:?}", element);
                            return Err(CoordinateError::InvalidReply);
                        }
                    };
                }
                Ok(metadata)
            }
            reply => {
                error!("failed to send meta, invalid reply {:?}", reply);
                Err(CoordinateError::InvalidReply)
            }
        }
    }
}

impl<F: RedisClientFactory> MigrationStateChecker for MigrationStateRespChecker<F> {
    fn check<'s>(
        &'s self,
        address: String,
    ) -> Pin<Box<dyn Stream<Item = Result<MigrationTaskMeta, CoordinateError>> + Send + 's>> {
        Box::pin(
            self.check_impl(address)
                .map(vec_result_to_stream)
                .flatten_stream(),
        )
    }
}

pub struct BrokerMigrationCommitter<MB: MetaManipulationBroker> {
    mani_broker: Arc<MB>,
}

impl<MB: MetaManipulationBroker> BrokerMigrationCommitter<MB> {
    pub fn new(mani_broker: Arc<MB>) -> Self {
        Self { mani_broker }
    }
}

impl<MB: MetaManipulationBroker> MigrationCommitter for BrokerMigrationCommitter<MB> {
    fn commit<'s>(
        &'s self,
        meta: MigrationTaskMeta,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoordinateError>> + Send + 's>> {
        let meta_clone = meta.clone();
        Box::pin(
            self.mani_broker
                .commit_migration(meta.clone())
                .map_err(move |e| {
                    error!("failed to commit migration {:?} {:?}", meta, e);
                    CoordinateError::MetaMani(e)
                })
                .map_ok(move |()| {
                    info!("successfully commit the migration {:?}", meta_clone);
                }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::super::broker::{MockMetaDataBroker, MockMetaManipulationBroker};
    use super::super::core::{MigrationStateSynchronizer, ParMigrationStateSynchronizer};
    use super::super::detector::BrokerProxiesRetriever;
    use super::super::sync::BrokerMetaRetriever;
    use super::*;
    use crate::common::cluster::{
        ClusterName, MigrationMeta, Proxy, RangeList, SlotRange, SlotRangeTag,
    };
    use crate::coordinator::core::MockProxyMetaSender;
    use crate::protocol::{BinSafeStr, DummyRedisClientFactory, MockRedisClient};
    use futures::{stream, StreamExt};
    use std::collections::HashMap;
    use std::convert::TryFrom;
    use tokio;

    fn gen_testing_dummy_proxy(addr: &str) -> Proxy {
        Proxy::new(
            addr.to_string(),
            7799,
            vec![],
            vec![],
            vec![],
            HashMap::new(),
        )
    }

    fn create_client_func() -> impl RedisClient {
        let mut mock_client = MockRedisClient::new();

        let info_mgr_cmd = vec![b"UMCTL".to_vec(), b"INFOMGR".to_vec()];
        mock_client
            .expect_execute_single()
            .withf(move |command: &Vec<BinSafeStr>| {
                command.eq(&info_mgr_cmd)
            })
            .times(1)
            .returning(|_| {
                let reply = b"mydb MIGRATING 1 233-666 7799 127.0.0.1:6000 127.0.0.1:7000 127.0.0.1:6001 127.0.0.1:7001".to_vec();
                let resp = Resp::Arr(Array::Arr(vec![Resp::Bulk(BulkStr::Str(reply))]));
                Box::pin(async { Ok(resp) })
            });

        mock_client
    }

    #[tokio::test]
    async fn test_migration_state_checker() {
        let factory = DummyRedisClientFactory::new(create_client_func);
        let checker = MigrationStateRespChecker::new(Arc::new(factory));
        let res: Vec<_> = checker.check("127.0.0.1:6000".to_string()).collect().await;
        assert_eq!(res.len(), 1);
        let meta = res[0].as_ref().unwrap();
        assert_eq!(meta.cluster_name.to_string(), "mydb");
        let tag = SlotRangeTag::Migrating(MigrationMeta {
            epoch: 7799,
            src_proxy_address: "127.0.0.1:6000".to_string(),
            src_node_address: "127.0.0.1:7000".to_string(),
            dst_proxy_address: "127.0.0.1:6001".to_string(),
            dst_node_address: "127.0.0.1:7001".to_string(),
        });
        let slot_range = SlotRange {
            range_list: RangeList::try_from("1 233-666").unwrap(),
            tag,
        };
        assert_eq!(meta.slot_range, slot_range);
    }

    fn gen_testing_migration_task_meta() -> MigrationTaskMeta {
        let tag = SlotRangeTag::Migrating(MigrationMeta {
            epoch: 7799,
            src_proxy_address: "127.0.0.1:6000".to_string(),
            src_node_address: "127.0.0.1:7000".to_string(),
            dst_proxy_address: "127.0.0.1:6001".to_string(),
            dst_node_address: "127.0.0.1:7001".to_string(),
        });
        let slot_range = SlotRange {
            range_list: RangeList::try_from("1 233-666").unwrap(),
            tag,
        };
        MigrationTaskMeta {
            cluster_name: ClusterName::from("mydb").unwrap(),
            slot_range,
        }
    }

    #[tokio::test]
    async fn test_migration_committer() {
        let mut mock_broker = MockMetaManipulationBroker::new();

        let meta = gen_testing_migration_task_meta();
        let meta2 = meta.clone();

        mock_broker
            .expect_commit_migration()
            .withf(move |m| m == &meta2)
            .returning(move |_| Box::pin(async { Ok(()) }));
        let mock_broker = Arc::new(mock_broker);

        let committer = BrokerMigrationCommitter::new(mock_broker);
        let res = committer.commit(meta).await;
        assert!(res.is_ok());
    }

    // Integrate together.
    #[tokio::test]
    async fn test_migration_state_sync() {
        let factory = Arc::new(DummyRedisClientFactory::new(create_client_func));
        let checker = MigrationStateRespChecker::new(factory);

        let mut mock_mani_broker = MockMetaManipulationBroker::new();
        let meta = gen_testing_migration_task_meta();
        let meta2 = meta.clone();
        mock_mani_broker
            .expect_commit_migration()
            .withf(move |m| m == &meta2)
            .returning(move |_| Box::pin(async { Ok(()) }));
        let mock_mani_broker = Arc::new(mock_mani_broker);

        let mut mock_data_broker = MockMetaDataBroker::new();
        mock_data_broker
            .expect_get_proxy_addresses()
            .returning(move || {
                let results = vec![Ok("127.0.0.1:6000".to_string())];
                Box::pin(stream::iter(results))
            });
        mock_data_broker
            .expect_get_proxy()
            .withf(|proxy_addr| proxy_addr == "127.0.0.1:6000")
            .returning(|_| Box::pin(async { Ok(Some(gen_testing_dummy_proxy("127.0.0.1:6000"))) }));
        mock_data_broker
            .expect_get_proxy()
            .withf(|proxy_addr| proxy_addr == "127.0.0.1:6001")
            .returning(|_| Box::pin(async { Ok(Some(gen_testing_dummy_proxy("127.0.0.1:6001"))) }));
        let mock_data_broker = Arc::new(mock_data_broker);

        let proxies_retriever = BrokerProxiesRetriever::new(mock_data_broker.clone());

        let committer = BrokerMigrationCommitter::new(mock_mani_broker.clone());
        let meta_retriever = BrokerMetaRetriever::new(mock_data_broker);

        let mut mock_meta_sender = MockProxyMetaSender::new();
        mock_meta_sender
            .expect_send_meta()
            .withf(|proxy| proxy.get_address() == "127.0.0.1:6000")
            .times(1)
            .returning(|_| Box::pin(async { Ok(()) }));
        mock_meta_sender
            .expect_send_meta()
            .withf(|proxy| proxy.get_address() == "127.0.0.1:6001")
            .times(1)
            .returning(|_| Box::pin(async { Ok(()) }));

        let sync = ParMigrationStateSynchronizer::new(
            proxies_retriever,
            checker,
            committer,
            meta_retriever,
            mock_meta_sender,
        );
        let res: Vec<_> = sync.run().collect().await;
        assert_eq!(res.len(), 1);
        res[0].as_ref().unwrap();
    }
}
