use super::redis_task::{RedisImportingTask, RedisMigratingTask};
use super::task::{ImportingTask, MigratingTask, MigrationConfig};
use ::common::cluster::{MigrationTaskMeta, SlotRange, SlotRangeTag};
use ::common::db::HostDBMap;
use ::common::utils::{get_key, get_slot, ThreadSafe};
use ::protocol::RedisClientFactory;
use ::protocol::Resp;
use ::proxy::backend::{CmdTask, CmdTaskSender, CmdTaskSenderFactory};
use ::proxy::database::{DBError, DBSendError, DBTag};
use arc_swap::ArcSwap;
use futures::Future;
use itertools::Either;
use migration::task::{MigrationError, MigrationState, SwitchArg};
use std::collections::HashMap;
use std::sync::atomic;
use std::sync::{Arc, RwLock};

type TaskRecord<T> = Either<Arc<MigratingTask<Task = T>>, Arc<ImportingTask<Task = T>>>;
type DBTask<T> = HashMap<MigrationTaskMeta, TaskRecord<T>>;
type TaskMap<T> = HashMap<String, DBTask<T>>;

pub struct MigrationManager<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe>
where
    <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task: DBTag,
{
    config: Arc<MigrationConfig>,
    empty: atomic::AtomicBool,
    updating_epoch: atomic::AtomicU64,
    dbs: RwLock<(
        u64,
        TaskMap<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>,
    )>,
    tmp_dbs: ArcSwap<(
        u64,
        TaskMap<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>,
    )>,
    client_factory: Arc<RCF>,
    sender_factory: Arc<TSF>,
    local_meta_epoch: atomic::AtomicU64,
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> MigrationManager<RCF, TSF>
where
    <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task: DBTag,
{
    pub fn new(
        config: Arc<MigrationConfig>,
        client_factory: Arc<RCF>,
        sender_factory: Arc<TSF>,
    ) -> Self {
        Self {
            config,
            empty: atomic::AtomicBool::new(true),
            updating_epoch: atomic::AtomicU64::new(0),
            dbs: RwLock::new((0, HashMap::new())),
            tmp_dbs: ArcSwap::new(Arc::new((0, HashMap::new()))),
            client_factory,
            sender_factory,
            local_meta_epoch: atomic::AtomicU64::new(0),
        }
    }

    pub fn send(
        &self,
        cmd_task: <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task,
    ) -> Result<(), DBSendError<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>>
    {
        let cmd_task = match self.send_to_db(cmd_task) {
            Ok(()) => return Ok(()),
            Err(DBSendError::SlotNotFound(cmd_task)) => cmd_task,
            errs => return errs,
        };
        self.send_to_tmp_db(cmd_task)
    }

    pub fn send_to_db(
        &self,
        cmd_task: <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task,
    ) -> Result<(), DBSendError<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>>
    {
        // Optimization for not having any migration.
        if self.empty.load(atomic::Ordering::SeqCst) {
            return Err(DBSendError::SlotNotFound(cmd_task));
        }

        Self::send_helper(
            &self
                .dbs
                .read()
                .expect("MigrationManager::send lock error")
                .1,
            cmd_task,
        )
    }

    pub fn send_to_tmp_db(
        &self,
        cmd_task: <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task,
    ) -> Result<(), DBSendError<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>>
    {
        Self::send_helper(&self.tmp_dbs.lease().1, cmd_task)
    }

    fn send_helper(
        task_map: &TaskMap<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>,
        cmd_task: <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task,
    ) -> Result<(), DBSendError<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>>
    {
        let db_name = cmd_task.get_db_name();
        match task_map.get(&db_name) {
            Some(tasks) => {
                let key = match get_key(cmd_task.get_resp()) {
                    Some(key) => key,
                    None => {
                        let resp = Resp::Error("missing key".to_string().into_bytes());
                        cmd_task.set_resp_result(Ok(resp));
                        return Err(DBSendError::MissingKey);
                    }
                };

                let slot = get_slot(&key);

                for (meta, record) in tasks.iter() {
                    let slot_range_start = meta.slot_range.start;
                    let slot_range_end = meta.slot_range.end;
                    if slot < slot_range_start || slot > slot_range_end {
                        continue;
                    }

                    match record {
                        Either::Left(migrating_task) => return migrating_task.send(cmd_task),
                        Either::Right(importing_task) => return importing_task.send(cmd_task),
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

        let force = flags.force;
        if !force && self.updating_epoch.load(atomic::Ordering::SeqCst) >= epoch {
            return Err(DBError::OldEpoch);
        }

        let (old_epoch, task_map) = self
            .dbs
            .read()
            .expect("MigrationManager::update reuse migrating")
            .clone();
        if !force && old_epoch >= epoch {
            return Err(DBError::OldEpoch);
        }

        let db_map = host_map.into_map();

        let mut migration_dbs = HashMap::new();

        for (db_name, node_map) in db_map.iter() {
            for (_node, slot_ranges) in node_map.iter() {
                for slot_range in slot_ranges.iter() {
                    match slot_range.tag {
                        SlotRangeTag::Migrating(ref _meta) => {
                            let migration_meta = MigrationTaskMeta {
                                db_name: db_name.clone(),
                                slot_range: slot_range.clone(),
                            };
                            if let Some(Either::Left(migrating_task)) = task_map
                                .get(db_name)
                                .and_then(|tasks| tasks.get(&migration_meta))
                            {
                                let tasks = migration_dbs
                                    .entry(db_name.clone())
                                    .or_insert_with(HashMap::new);
                                tasks.insert(migration_meta, Either::Left(migrating_task.clone()));
                            }
                        }
                        SlotRangeTag::Importing(ref _meta) => {
                            let migration_meta = MigrationTaskMeta {
                                db_name: db_name.clone(),
                                slot_range: slot_range.clone(),
                            };
                            if let Some(Either::Right(importing_task)) = task_map
                                .get(db_name)
                                .and_then(|tasks| tasks.get(&migration_meta))
                            {
                                let tasks = migration_dbs
                                    .entry(db_name.clone())
                                    .or_insert_with(HashMap::new);
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
                                    tag: SlotRangeTag::Migrating(meta.clone()),
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
                                self.config.clone(),
                                db_name.clone(),
                                (slot_range.start, slot_range.end),
                                meta,
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
                                .or_insert_with(HashMap::new);
                            tasks.insert(migration_meta, Either::Left(task));
                        }
                        SlotRangeTag::Importing(meta) => {
                            let epoch = meta.epoch;
                            let migration_meta = MigrationTaskMeta {
                                db_name: db_name.clone(),
                                slot_range: SlotRange {
                                    start,
                                    end,
                                    tag: SlotRangeTag::Importing(meta.clone()),
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
                                self.config.clone(),
                                meta,
                                self.client_factory.clone(),
                                self.sender_factory.clone(),
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
                                .or_insert_with(HashMap::new);
                            tasks.insert(migration_meta, Either::Right(task));
                        }
                        SlotRangeTag::None => continue,
                    }
                }
            }
        }

        let mut removed_tasks = HashMap::new();
        for (db_name, tasks) in task_map.into_iter() {
            for (meta, task) in tasks.into_iter() {
                if migration_dbs
                    .get(&db_name)
                    .and_then(|new_tasks| new_tasks.get(&meta))
                    .is_some()
                {
                    continue;
                }
                removed_tasks
                    .entry(db_name.clone())
                    .or_insert_with(HashMap::new)
                    .insert(meta, task);
            }
        }

        let empty = migration_dbs.is_empty();

        {
            let mut dbs = self.dbs.write().unwrap();
            if !force && dbs.0 != old_epoch {
                return Err(DBError::TryAgain);
            }
            if !force && epoch <= dbs.0 {
                return Err(DBError::OldEpoch);
            }
            for (db_name, epoch, start, end, migrating_task) in new_migrating_tasks.into_iter() {
                info!(
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
                info!(
                    "spawn slot importing replica {} {} {}-{}",
                    db_name, epoch, start, end
                );
                tokio::spawn(importing_task.start().map_err(move |e| {
                    error!(
                        "replica slot task {} {} {}-{} exit {:?}",
                        db_name, epoch, start, end, e
                    )
                }));
            }
            *dbs = (epoch, migration_dbs);
            self.updating_epoch.store(epoch, atomic::Ordering::SeqCst);
            self.empty.store(empty, atomic::Ordering::SeqCst);
            self.tmp_dbs.store(Arc::new((epoch, removed_tasks)));
        }

        Ok(())
    }

    pub fn commit_importing(&self, switch_arg: SwitchArg) -> Result<(), SwitchError> {
        let mut task_meta = switch_arg.meta.clone();

        let arg_epoch = match switch_arg.meta.slot_range.tag {
            SlotRangeTag::None => return Err(SwitchError::InvalidArg),
            SlotRangeTag::Migrating(ref meta) => {
                let epoch = meta.epoch;
                task_meta.slot_range.tag = SlotRangeTag::Importing(meta.clone());
                epoch
            }
            SlotRangeTag::Importing(ref meta) => meta.epoch,
        };
        if self.local_meta_epoch.load(atomic::Ordering::SeqCst) < arg_epoch {
            return Err(SwitchError::NotReady);
        }

        if let Some(tasks) = self
            .dbs
            .read()
            .expect("MigrationManager::commit_importing lock error")
            .1
            .get(&switch_arg.meta.db_name)
        {
            debug!(
                "found tasks for db {} {}",
                switch_arg.meta.db_name,
                tasks.len()
            );

            if let Some(record) = tasks.get(&task_meta) {
                debug!("found record for db {}", switch_arg.meta.db_name);
                match record {
                    Either::Left(_migrating_task) => {
                        error!(
                            "Received switch request when migrating {:?}",
                            switch_arg.meta
                        );
                        return Err(SwitchError::PeerMigrating);
                    }
                    Either::Right(importing_task) => {
                        return importing_task
                            .commit(switch_arg)
                            .map_err(SwitchError::MgrErr);
                    }
                }
            }
        }
        warn!("No corresponding task found {:?}", switch_arg.meta);
        Err(SwitchError::TaskNotFound)
    }

    pub fn get_finished_tasks(&self) -> Vec<MigrationTaskMeta> {
        let mut metadata = vec![];
        {
            for (_db_name, tasks) in self
                .dbs
                .read()
                .expect("Migration::get_finished_tasks")
                .1
                .iter()
            {
                for (meta, task) in tasks.iter() {
                    if let Either::Left(migrating_task) = task {
                        if migrating_task.get_state() == MigrationState::SwitchCommitted {
                            metadata.push(meta.clone());
                        }
                    }
                }
            }
        }
        metadata
    }

    pub fn update_local_meta_epoch(&self, epoch: u64) {
        loop {
            let current = self.local_meta_epoch.load(atomic::Ordering::SeqCst);
            if current >= epoch {
                break;
            }
            self.local_meta_epoch
                .compare_and_swap(current, epoch, atomic::Ordering::SeqCst);
        }
        debug!("Successfully update local meta epoch in migration manager");
    }

    pub fn clear_tmp_dbs(&self, epoch: u64) {
        let tmp_dbs = self.tmp_dbs.lease();
        if tmp_dbs.0 == epoch {
            self.tmp_dbs
                .compare_and_swap(tmp_dbs, Arc::new((epoch, HashMap::new())));
        }
    }
}

#[derive(Debug)]
pub enum SwitchError {
    InvalidArg,
    TaskNotFound,
    PeerMigrating,
    NotReady,
    MgrErr(MigrationError),
}
