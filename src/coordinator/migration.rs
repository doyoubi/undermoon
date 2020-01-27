use super::broker::MetaManipulationBroker;
use super::core::{CoordinateError, MigrationCommitter, MigrationStateChecker};
use ::common::cluster::MigrationTaskMeta;
use ::protocol::{RedisClient, RedisClientFactory, RespVec};
use futures::{future, stream, Future, Stream};
use protocol::{Array, BulkStr, Resp};
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

impl<F: RedisClientFactory> MigrationStateChecker for MigrationStateRespChecker<F> {
    fn check(
        &self,
        address: String,
    ) -> Box<dyn Stream<Item = MigrationTaskMeta, Error = CoordinateError> + Send> {
        let client_fut = self
            .client_factory
            .create_client(address.clone())
            .map_err(CoordinateError::Redis);
        Box::new(
            client_fut
                .and_then(move |client| {
                    let cmd = vec!["UMCTL".to_string(), "INFOMGR".to_string()];
                    debug!("sending UMCTL INFOMGR to {}", address);
                    let address_clone = address.clone();
                    client
                        .execute(cmd.into_iter().map(String::into_bytes).collect())
                        .map_err(move |e| {
                            error!(
                                "failed to check the migration state {} {:?}",
                                address_clone, e
                            );
                            CoordinateError::Redis(e)
                        })
                        .and_then(move |(_client, resp)| match resp {
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
                                            return future::err(CoordinateError::InvalidReply);
                                        }
                                    };
                                }
                                future::ok(stream::iter_ok(metadata))
                            }
                            reply => {
                                error!("failed to send meta, invalid reply {:?}", reply);
                                future::err(CoordinateError::InvalidReply)
                            }
                        })
                })
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
    fn commit(
        &self,
        meta: MigrationTaskMeta,
    ) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        let meta_clone = meta.clone();
        Box::new(
            self.mani_broker
                .commit_migration(meta.clone())
                .map_err(move |e| {
                    error!("failed to commit migration {:?} {:?}", meta, e);
                    CoordinateError::MetaMani(e)
                })
                .map(move |()| {
                    info!("successfully commit the migration {:?}", meta_clone);
                }),
        )
    }
}
