use super::redis_task::{RedisImportingTask, RedisMigratingTask};
use super::task::{ImportingTask, MigratingTask, MigrationTaskMeta};
use ::common::cluster::{SlotRange, SlotRangeTag};
use ::common::db::HostDBMap;
use ::common::utils::{get_key, SLOT_NUM};
use ::protocol::RedisClientFactory;
use ::proxy::backend::{CmdTask, CmdTaskSender, CmdTaskSenderFactory};
use ::proxy::database::{DBError, DBSendError};
use ::proxy::session::CmdCtx;
use ::replication::manager::ReplicatorManager;
use crc16::{State, XMODEM};
use itertools::Either;
use std::collections::HashMap;
use std::sync::atomic;
use std::sync::{Arc, RwLock};

type TaskRecord = Either<Arc<MigratingTask<Task = CmdCtx>>, Arc<ImportingTask>>;

pub struct MigrationManager<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory> {
    updating_epoch: atomic::AtomicU64,
    tasks: RwLock<(u64, HashMap<MigrationTaskMeta, TaskRecord>)>,
    client_factory: Arc<RCF>,
    sender_factory: Arc<TSF>,
    replicator_manager: ReplicatorManager<RCF>,
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory> MigrationManager<RCF, TSF> {
    pub fn new(client_factory: Arc<RCF>) -> Self {
        let client_factory_clone = client_factory.clone();
        Self {
            updating_epoch: atomic::AtomicU64::new(0),
            tasks: RwLock::new((0, HashMap::new())),
            client_factory,
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
            Some(db) => {
                let key = match get_key(cmd_task.get_resp()) {
                    Some(key) => key,
                    None => {
                        return Err(DBSendError::MissingKey);
                    }
                };

                let slot = State::<XMODEM>::calculate(&key) as usize % SLOT_NUM;

                for (key, record) in self.tasks.iter() {
                    let MigrationTaskMeta {
                        slot_range_start,
                        slot_range_end,
                    } = key;
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
        let db_map = host_map.into_db_map();

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

        let mut migration_tasks = HashMap::new();

        // Race condition here.
        // epoch 1 < epoch 2 < epoch 3
        // Suppose when epoch 3 starts to modify data in epoch 1 and reuse all the tasks of epoch 1,
        // epoch 2 try to write at the same time and create a new tasks.
        // If the write operation of epoch 2 goes first, the thread of epoch 3 may not be able to
        // see the changes of epoch 2 and recreate the new tasks itself.
        // TODO: test whether a big write lock would be expensive and reimplement it.
        for (db_name, node_map) in db_map.iter() {
            for (node, slot_ranges) in node_map.iter() {
                for slot_range in slot_ranges.iter() {
                    match slot_range.tag {
                        SlotRangeTag::Migrating(meta) => {
                            let migration_meta = MigrationTaskMeta {
                                db_name: db_name.clone(),
                                slot_range: slot_range.clone(),
                            };
                            if let Some(Either::Left(migrating_task)) =
                                self.tasks.read().1.get(&migration_meta)
                            {
                                migration_tasks
                                    .insert(migration_meta, Either::Left(migrating_task.clone()));
                            }
                        }
                        SlotRangeTag::Importing(meta) => {
                            let migration_meta = MigrationTaskMeta {
                                db_name: db_name.clone(),
                                slot_range: slot_range.clone(),
                            };
                            if let Some(Either::Right(importing_task)) =
                                self.tasks.read().1.get(&migration_meta)
                            {
                                migration_tasks
                                    .insert(migration_meta, Either::Right(importing_task.clone()));
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
            for (node, slot_ranges) in node_map.into_iter() {
                for slot_range in slot_ranges {
                    match slot_range.tag {
                        SlotRangeTag::Migrating(meta) => {
                            let migration_meta = MigrationTaskMeta {
                                db_name: db_name.clone(),
                                slot_range,
                            };

                            if migration_tasks.contains_key(&migration_meta) {
                                continue;
                            }

                            let task = Arc::new(RedisMigratingTask::new(
                                migration_meta.clone(),
                                self.client_factory.clone(),
                                self.sender_factory.clone(),
                            ));
                            new_migrating_tasks.push((
                                db_name.clone(),
                                meta.epoch,
                                slot_range.start,
                                slot_range.end,
                                task.clone(),
                            ));
                            migration_tasks.insert(migration_meta, Either::Left(task));
                        }
                        SlotRangeTag::Importing(meta) => {
                            let migration_meta = MigrationTaskMeta {
                                db_name: db_name.clone(),
                                slot_range,
                            };

                            if migration_tasks.contains_key(&migration_meta) {
                                continue;
                            }

                            let task = Arc::new(RedisImportingTask::new(
                                migration_meta.clone(),
                                self.client_factory.clone(),
                            ));
                            new_importing_tasks.push((
                                db_name.clone(),
                                meta.epoch,
                                slot_range.start,
                                slot_range.end,
                                task.clone(),
                            ));
                            migration_tasks.insert(migration_meta, Either::Right(task));
                        }
                        SlotRangeTag::None => continue,
                    }
                }
            }
        }

        {
            let mut tasks = self.tasks.write().unwrap();
            if !force && epoch <= tasks.0 {
                // We're fooled by the `updating_epoch`, update it.
                self.updating_epoch.store(tasks.0, atomic::Ordering::SeqCst);
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
            *tasks = (epoch, migration_tasks);
        }

        Ok(())
    }
}
