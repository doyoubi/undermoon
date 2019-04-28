use super::task::{ImportingTask, MigratingTask, MigrationTaskKey};
use ::common::cluster::{SlotRange, SlotRangeTag};
use ::protocol::RedisClientFactory;
use ::proxy::session::CmdCtx;
use ::replication::manager::ReplicatorManager;
use itertools::Either;
use std::collections::HashMap;
use std::sync::atomic;
use std::sync::{Arc, RwLock};

type TaskRecord = Either<Arc<MigratingTask<Task = CmdCtx>>, Arc<ImportingTask>>;

pub struct MigrationManager<F: RedisClientFactory> {
    updating_epoch: atomic::AtomicU64,
    tasks: RwLock<(u64, HashMap<MigrationTaskKey, TaskRecord>)>,
    client_factory: Arc<F>,
    replicator_manager: ReplicatorManager<F>,
}

impl<F: RedisClientFactory> MigrationManager<F> {
    pub fn new(client_factory: Arc<F>) -> Self {
        let client_factory_clone = client_factory.clone();
        Self {
            updating_epoch: atomic::AtomicU64::new(0),
            tasks: RwLock::new((0, HashMap::new())),
            client_factory,
            replicator_manager: ReplicatorManager::new(client_factory_clone),
        }
    }
}
