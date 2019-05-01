use super::redis_task::{RedisImportingTask, RedisMigratingTask};
use super::task::{ImportingTask, MigratingTask, MigrationTaskMeta};
use ::common::cluster::{SlotRange, SlotRangeTag};
use ::common::db::HostDBMap;
use ::common::utils::{get_key, ThreadSafe, SLOT_NUM};
use ::protocol::RedisClientFactory;
use ::protocol::Resp;
use ::proxy::backend::{CmdTask, CmdTaskSender, CmdTaskSenderFactory};
use ::proxy::database::{DBError, DBSendError, DBTag};
use ::replication::manager::ReplicatorManager;
use crc16::{State, XMODEM};
use futures::Future;
use itertools::Either;
use std::collections::HashMap;
use std::sync::atomic;
use std::sync::{Arc, RwLock};

type TaskRecord<T: CmdTask> = Either<Arc<MigratingTask<Task = T>>, Arc<ImportingTask>>;
type DBTask<T: CmdTask> = HashMap<MigrationTaskMeta, TaskRecord<T>>;

pub struct MigrationManager<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe>
where
    <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task: DBTag,
{
    updating_epoch: atomic::AtomicU64,
    dbs: RwLock<(
        u64,
        HashMap<String, DBTask<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>>,
    )>,
    client_factory: Arc<RCF>,
    sender_factory: Arc<TSF>,
    replicator_manager: ReplicatorManager<RCF>,
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> MigrationManager<RCF, TSF>
where
    <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task: DBTag,
{
    pub fn new(client_factory: Arc<RCF>, sender_factory: Arc<TSF>) -> Self {
        let client_factory_clone = client_factory.clone();
        Self {
            updating_epoch: atomic::AtomicU64::new(0),
            dbs: RwLock::new((0, HashMap::new())),
            client_factory,
            sender_factory,
            replicator_manager: ReplicatorManager::new(client_factory_clone),
        }
    }

    pub fn send(
        &self,
        cmd_task: <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task,
    ) -> Result<(), DBSendError<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>>
    {
        let db_name = cmd_task.get_db_name();
        match self
            .dbs
            .read()
            .expect("MigrationManager::send lock error")
            .1
            .get(&db_name)
        {
            Some(tasks) => {
                let key = match get_key(cmd_task.get_resp()) {
                    Some(key) => key,
                    None => {
                        let resp = Resp::Error("missing key".to_string().into_bytes());
                        cmd_task.set_resp_result(Ok(resp));
                        return Err(DBSendError::MissingKey);
                    }
                };

                let slot = State::<XMODEM>::calculate(&key) as usize % SLOT_NUM;

                for (meta, record) in tasks.iter() {
                    let slot_range_start = meta.slot_range.start;
                    let slot_range_end = meta.slot_range.end;
                    if slot < slot_range_start || slot > slot_range_end {
                        continue;
                    }

                    if let Either::Left(migrating_task) = record {
                        return migrating_task.send(cmd_task);
                    }
                }

                Err(DBSendError::SlotNotFound(cmd_task))
            }
            None => Err(DBSendError::SlotNotFound(cmd_task)),
        }
    }

    pub fn update(&self, host_map: HostDBMap) -> Result<(), DBError> {
        let epoch = host_map.get_epoch();
        let flags = host_map.get_flags();
        let db_map = host_map.into_map();

        let force = flags.force;
        if !force && self.updating_epoch.load(atomic::Ordering::SeqCst) >= epoch {
            return Err(DBError::OldEpoch);
        }

        // The computation below might take a long time.
        // Set epoch first to let later requests fail fast.
        // We can't update the epoch inside the lock here.
        // Because when we get the info inside it, it may be partially updated and inconsistent.
        self.updating_epoch.store(epoch, atomic::Ordering::SeqCst);
        // After this, other threads might accidentally change `updating_epoch` to a lower epoch,
        // we will correct his later.

        let mut migration_dbs = HashMap::new();

        // Race condition here.
        // epoch 1 < epoch 2 < epoch 3
        // Suppose when epoch 3 starts to modify data in epoch 1 and reuse all the tasks of epoch 1,
        // epoch 2 try to write at the same time and create a new tasks.
        // If the write operation of epoch 2 goes first, the thread of epoch 3 may not be able to
        // see the changes of epoch 2 and recreate the new tasks itself.
        // TODO: test whether a big write lock would be expensive and reimplement it.
        for (db_name, node_map) in db_map.iter() {
            for (_node, slot_ranges) in node_map.iter() {
                for slot_range in slot_ranges.iter() {
                    match slot_range.tag {
                        SlotRangeTag::Migrating(ref _meta) => {
                            let migration_meta = MigrationTaskMeta {
                                db_name: db_name.clone(),
                                slot_range: slot_range.clone(),
                            };
                            if let Some(Either::Left(migrating_task)) = self
                                .dbs
                                .read()
                                .expect("MigrationManager::update reuse migrating")
                                .1
                                .get(db_name)
                                .and_then(|tasks| tasks.get(&migration_meta))
                            {
                                let tasks = migration_dbs
                                    .entry(db_name.clone())
                                    .or_insert(HashMap::new());
                                tasks.insert(migration_meta, Either::Left(migrating_task.clone()));
                            }
                        }
                        SlotRangeTag::Importing(ref _meta) => {
                            let migration_meta = MigrationTaskMeta {
                                db_name: db_name.clone(),
                                slot_range: slot_range.clone(),
                            };
                            if let Some(Either::Right(importing_task)) = self
                                .dbs
                                .read()
                                .expect("MigrationManager::update reuse importing")
                                .1
                                .get(db_name)
                                .and_then(|tasks| tasks.get(&migration_meta))
                            {
                                let tasks = migration_dbs
                                    .entry(db_name.clone())
                                    .or_insert(HashMap::new());
                                tasks.insert(migration_meta, Either::Right(importing_task.clone()));
                            }
                        }
                        SlotRangeTag::None => continue,
                    }
                }
            }
        }

        let mut new_migrating_tasks = Vec::new();
        let mut new_importing_tasks = Vec::new();

        for (db_name, node_map) in db_map.into_iter() {
            for (_node, slot_ranges) in node_map.into_iter() {
                for slot_range in slot_ranges {
                    let start = slot_range.start;
                    let end = slot_range.end;
                    match slot_range.tag {
                        SlotRangeTag::Migrating(meta) => {
                            let epoch = meta.epoch;
                            let migration_meta = MigrationTaskMeta {
                                db_name: db_name.clone(),
                                slot_range: SlotRange {
                                    start,
                                    end,
                                    tag: SlotRangeTag::Migrating(meta),
                                },
                            };

                            if Some(true)
                                == migration_dbs
                                    .get(&db_name)
                                    .map(|tasks| tasks.contains_key(&migration_meta))
                            {
                                continue;
                            }

                            let task = Arc::new(RedisMigratingTask::new(
                                migration_meta.clone(),
                                self.client_factory.clone(),
                                self.sender_factory.clone(),
                            ));
                            new_migrating_tasks.push((
                                db_name.clone(),
                                epoch,
                                slot_range.start,
                                slot_range.end,
                                task.clone(),
                            ));
                            let tasks = migration_dbs
                                .entry(db_name.clone())
                                .or_insert(HashMap::new());
                            tasks.insert(migration_meta, Either::Left(task));
                        }
                        SlotRangeTag::Importing(meta) => {
                            let epoch = meta.epoch;
                            let migration_meta = MigrationTaskMeta {
                                db_name: db_name.clone(),
                                slot_range: SlotRange {
                                    start,
                                    end,
                                    tag: SlotRangeTag::Importing(meta),
                                },
                            };

                            if Some(true)
                                == migration_dbs
                                    .get(&db_name)
                                    .map(|tasks| tasks.contains_key(&migration_meta))
                            {
                                continue;
                            }

                            let task = Arc::new(RedisImportingTask::new(
                                migration_meta.clone(),
                                self.client_factory.clone(),
                            ));
                            new_importing_tasks.push((
                                db_name.clone(),
                                epoch,
                                slot_range.start,
                                slot_range.end,
                                task.clone(),
                            ));
                            let tasks = migration_dbs
                                .entry(db_name.clone())
                                .or_insert(HashMap::new());
                            tasks.insert(migration_meta, Either::Right(task));
                        }
                        SlotRangeTag::None => continue,
                    }
                }
            }
        }

        {
            let mut dbs = self.dbs.write().unwrap();
            if !force && epoch <= dbs.0 {
                // We're fooled by the `updating_epoch`, update it.
                self.updating_epoch.store(dbs.0, atomic::Ordering::SeqCst);
                return Err(DBError::OldEpoch);
            }
            for (db_name, epoch, start, end, migrating_task) in new_migrating_tasks.into_iter() {
                debug!(
                    "spawn slot migrating task {} {} {} {}",
                    db_name, epoch, start, end
                );
                tokio::spawn(migrating_task.start().map_err(move |e| {
                    error!(
                        "master slot task {} {} {} {} exit {:?}",
                        db_name, epoch, start, end, e
                    )
                }));
            }
            for (db_name, epoch, start, end, importing_task) in new_importing_tasks.into_iter() {
                debug!(
                    "spawn slot importing replica {} {} {} {}",
                    db_name, epoch, start, end
                );
                tokio::spawn(importing_task.start().map_err(move |e| {
                    error!(
                        "replica slot task {} {} {} {} exit {:?}",
                        db_name, epoch, start, end, e
                    )
                }));
            }
            *dbs = (epoch, migration_dbs);
        }

        Ok(())
    }
}
