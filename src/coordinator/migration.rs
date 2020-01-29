use super::broker::MetaManipulationBroker;
use super::core::{CoordinateError, MigrationCommitter, MigrationStateChecker};
use crate::common::cluster::MigrationTaskMeta;
use crate::protocol::{Array, BulkStr, Resp};
use crate::protocol::{RedisClient, RedisClientFactory, RespVec};
use futures::{Future, FutureExt, TryFutureExt, stream, Stream};
use std::str;
use std::pin::Pin;
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
                    .into_iter();
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
    async fn check_impl(
        &self,
        address: String,
    ) -> Result<Vec<MigrationTaskMeta>, CoordinateError> {
        let mut client = self.client_factory.create_client(address.clone()).await.map_err(CoordinateError::Redis)?;
        let cmd = vec!["UMCTL".to_string(), "INFOMGR".to_string()].into_iter().map(String::into_bytes).collect();
        let resp = client.execute(cmd).await.map_err(CoordinateError::Redis)?;

        match resp {
            Resp::Arr(Array::Arr(arr)) => {
                let mut metadata = vec![];
                for element in arr.into_iter() {
                    match Self::parse_migration_task_meta(&element) {
                        Some(meta) => metadata.push(meta),
                        None => {
                            error!(
                                "failed to parse migration task meta data {:?}",
                                element
                            );
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
                .map(|addrs_res| {
                    let elements = match addrs_res {
                        Ok(addrs) => addrs.into_iter().map(Ok).collect(),
                        Err(err) => vec![Err(err)],
                    };
                    stream::iter(elements)
                })
                .flatten_stream()
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
