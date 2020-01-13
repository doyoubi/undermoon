use super::redis_task::{RedisImportingTask, RedisMigratingTask};
use super::task::{ImportingTask, MigratingTask};
use ::common::cluster::{MigrationTaskMeta, SlotRange, SlotRangeTag};
use ::common::config::AtomicMigrationConfig;
use ::common::db::HostDBMap;
use ::common::utils::{get_slot, ThreadSafe};
use ::protocol::RedisClientFactory;
use ::protocol::Resp;
use ::proxy::backend::{CmdTask, CmdTaskSender, CmdTaskSenderFactory};
use ::proxy::database::{DBSendError, DBTag};
use ::proxy::slowlog::TaskEvent;
use futures::Future;
use itertools::Either;
use migration::delete_keys::{DeleteKeysTask, DeleteKeysTaskMap};
use migration::task::{MigrationError, MigrationState, SwitchArg};
use std::collections::HashMap;
use std::sync::Arc;

type TaskRecord<T> = Either<Arc<dyn MigratingTask<Task = T>>, Arc<dyn ImportingTask<Task = T>>>;
type DBTask<T> = HashMap<MigrationTaskMeta, TaskRecord<T>>;
type TaskMap<T> = HashMap<String, DBTask<T>>;
type NewMigrationTuple<TSF> = (
    MigrationMap<TSF>,
    Vec<NewTask<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>>,
);

pub struct NewTask<T: CmdTask> {
    db_name: String,
    epoch: u64,
    slot_range_start: usize,
    slot_range_end: usize,
    task: TaskRecord<T>,
}

pub struct MigrationManager<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe>
where
    <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task: DBTag,
{
    config: Arc<AtomicMigrationConfig>,
    client_factory: Arc<RCF>,
    sender_factory: Arc<TSF>,
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> MigrationManager<RCF, TSF>
where
    <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task: DBTag,
{
    pub fn new(
        config: Arc<AtomicMigrationConfig>,
        client_factory: Arc<RCF>,
        sender_factory: Arc<TSF>,
    ) -> Self {
        Self {
            config,
            client_factory,
            sender_factory,
        }
    }

    pub fn create_new_migration_map(
        &self,
        old_migration_map: &MigrationMap<TSF>,
        local_db_map: &HostDBMap,
    ) -> NewMigrationTuple<TSF> {
        old_migration_map.update_from_old_task_map(
            local_db_map,
            self.config.clone(),
            self.client_factory.clone(),
            self.sender_factory.clone(),
        )
    }

    pub fn run_tasks(
        &self,
        new_tasks: Vec<NewTask<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>>,
    ) {
        if new_tasks.is_empty() {
            return;
        }

        for NewTask {
            db_name,
            epoch,
            slot_range_start,
            slot_range_end,
            task,
        } in new_tasks.into_iter()
        {
            match task {
                Either::Left(migrating_task) => {
                    info!(
                        "spawn slot migrating task {} {} {} {}",
                        db_name, epoch, slot_range_start, slot_range_end
                    );
                    tokio::spawn(migrating_task.start().map_err(move |e| {
                        error!(
                            "master slot task {} {} {} {} exit {:?}",
                            db_name, epoch, slot_range_start, slot_range_end, e
                        )
                    }));
                }
                Either::Right(importing_task) => {
                    info!(
                        "spawn slot importing replica {} {} {}-{}",
                        db_name, epoch, slot_range_start, slot_range_end
                    );
                    tokio::spawn(importing_task.start().map_err(move |e| {
                        error!(
                            "replica slot task {} {} {}-{} exit {:?}",
                            db_name, epoch, slot_range_start, slot_range_end, e
                        )
                    }));
                }
            }
        }
        info!("spawn finished");
    }

    pub fn create_new_deleting_task_map(
        &self,
        old_deleting_task_map: &DeleteKeysTaskMap,
        local_db_map: &HostDBMap,
        left_slots_after_change: HashMap<String, HashMap<String, Vec<SlotRange>>>,
    ) -> (DeleteKeysTaskMap, Vec<Arc<DeleteKeysTask>>) {
        old_deleting_task_map.update_from_old_task_map(
            local_db_map,
            left_slots_after_change,
            self.config.clone(),
            self.client_factory.clone(),
        )
    }

    pub fn run_deleting_tasks(&self, new_tasks: Vec<Arc<DeleteKeysTask>>) {
        if new_tasks.is_empty() {
            return;
        }

        for task in new_tasks.into_iter() {
            if let Some(fut) = task.start() {
                let address = task.get_address();
                tokio::spawn(
                    fut.map(move |()| info!("deleting keys for {} stopped", address))
                        .map_err(move |e| match e {
                            MigrationError::Canceled => {
                                info!("task for deleting keys get canceled")
                            }
                            _ => error!("task for deleting keys exit with error {:?}", e),
                        }),
                );
            }
        }
        info!("spawn finished for deleting keys");
    }
}

pub struct MigrationMap<TSF: CmdTaskSenderFactory + ThreadSafe>
where
    <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task: DBTag,
{
    empty: bool,
    task_map: TaskMap<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>,
}

impl<TSF: CmdTaskSenderFactory + ThreadSafe> MigrationMap<TSF>
where
    <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task: DBTag,
{
    pub fn new() -> Self {
        Self {
            empty: true,
            task_map: HashMap::new(),
        }
    }

    pub fn info(&self) -> String {
        self.task_map
            .iter()
            .map(|(db_name, tasks)| {
                let mut lines = vec![format!("name: {}", db_name)];
                for task_meta in tasks.keys() {
                    if let Some(migration_meta) = task_meta.slot_range.tag.get_migration_meta() {
                        lines.push(format!(
                            "{}-{} {} -> {}",
                            task_meta.slot_range.start,
                            task_meta.slot_range.end,
                            migration_meta.src_node_address,
                            migration_meta.dst_node_address
                        ));
                    } else {
                        error!("invalid slot range migration meta");
                    }
                }
                lines.join("\n")
            })
            .collect::<Vec<String>>()
            .join("\r\n")
    }

    pub fn send(
        &self,
        cmd_task: <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task,
    ) -> Result<(), DBSendError<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>>
    {
        cmd_task
            .get_slowlog()
            .log_event(TaskEvent::SentToMigrationDB);
        self.send_to_db(cmd_task)
    }

    pub fn send_to_db(
        &self,
        cmd_task: <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task,
    ) -> Result<(), DBSendError<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>>
    {
        // Optimization for not having any migration.
        if self.empty {
            return Err(DBSendError::SlotNotFound(cmd_task));
        }

        Self::send_helper(&self.task_map, cmd_task)
    }

    fn send_helper(
        task_map: &TaskMap<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>,
        cmd_task: <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task,
    ) -> Result<(), DBSendError<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>>
    {
        let db_name = cmd_task.get_db_name();
        match task_map.get(&db_name) {
            Some(tasks) => {
                let key = match cmd_task.get_key() {
                    Some(key) => key,
                    None => {
                        let resp = Resp::Error("missing key".to_string().into_bytes());
                        cmd_task.set_resp_result(Ok(resp));
                        return Err(DBSendError::MissingKey);
                    }
                };

                let slot = get_slot(key);

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

    pub fn get_left_slots_after_change(
        &self,
        new_migration_map: &Self,
        new_db_map: &HostDBMap,
    ) -> HashMap<String, HashMap<String, Vec<SlotRange>>> {
        let mut left_slots = HashMap::new();
        for (dbname, db) in self.task_map.iter() {
            let nodes = match new_db_map.get_map().get(dbname) {
                Some(nodes) => nodes,
                None => continue,
            };

            for task_meta in db.keys() {
                if new_migration_map
                    .task_map
                    .get(dbname)
                    .and_then(|db_task_map| db_task_map.get(task_meta))
                    .is_some()
                {
                    // task is still running, ignore it.
                    continue;
                }

                let tag = &task_meta.slot_range.tag;
                let address = match tag {
                    SlotRangeTag::None => continue,
                    SlotRangeTag::Importing(meta) => &meta.dst_node_address,
                    SlotRangeTag::Migrating(meta) => &meta.src_node_address,
                };
                let slots = match nodes.get(address) {
                    Some(slots) => slots,
                    None => continue,
                };
                left_slots
                    .entry(dbname.clone())
                    .or_insert_with(HashMap::new)
                    .insert(address.clone(), slots.clone());
            }
        }
        left_slots
    }

    pub fn update_from_old_task_map<RCF>(
        &self,
        local_db_map: &HostDBMap,
        config: Arc<AtomicMigrationConfig>,
        client_factory: Arc<RCF>,
        sender_factory: Arc<TSF>,
    ) -> (
        Self,
        Vec<NewTask<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>>,
    )
    where
        RCF: RedisClientFactory,
    {
        let old_task_map = &self.task_map;

        let new_db_map = local_db_map.get_map();

        let mut migration_dbs = HashMap::new();

        for (db_name, node_map) in new_db_map.iter() {
            for (_node, slot_ranges) in node_map.iter() {
                for slot_range in slot_ranges.iter() {
                    match slot_range.tag {
                        SlotRangeTag::Migrating(ref _meta) => {
                            let migration_meta = MigrationTaskMeta {
                                db_name: db_name.clone(),
                                slot_range: slot_range.clone(),
                            };
                            if let Some(Either::Left(migrating_task)) = old_task_map
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
                            if let Some(Either::Right(importing_task)) = old_task_map
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

        let mut new_tasks = Vec::new();

        for (db_name, node_map) in new_db_map.iter() {
            for (_node, slot_ranges) in node_map.iter() {
                for slot_range in slot_ranges {
                    let start = slot_range.start;
                    let end = slot_range.end;
                    match slot_range.tag {
                        SlotRangeTag::Migrating(ref meta) => {
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
                                    .get(db_name)
                                    .map(|tasks| tasks.contains_key(&migration_meta))
                            {
                                continue;
                            }

                            let task = Arc::new(RedisMigratingTask::new(
                                config.clone(),
                                db_name.clone(),
                                (slot_range.start, slot_range.end),
                                meta.clone(),
                                client_factory.clone(),
                                sender_factory.clone(),
                            ));
                            new_tasks.push(NewTask {
                                db_name: db_name.clone(),
                                epoch,
                                slot_range_start: slot_range.start,
                                slot_range_end: slot_range.end,
                                task: Either::Left(task.clone()),
                            });
                            let tasks = migration_dbs
                                .entry(db_name.clone())
                                .or_insert_with(HashMap::new);
                            tasks.insert(migration_meta, Either::Left(task));
                        }
                        SlotRangeTag::Importing(ref meta) => {
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
                                    .get(db_name)
                                    .map(|tasks| tasks.contains_key(&migration_meta))
                            {
                                continue;
                            }

                            let task = Arc::new(RedisImportingTask::new(
                                config.clone(),
                                db_name.clone(),
                                meta.clone(),
                                client_factory.clone(),
                                sender_factory.clone(),
                            ));
                            new_tasks.push(NewTask {
                                db_name: db_name.clone(),
                                epoch,
                                slot_range_start: slot_range.start,
                                slot_range_end: slot_range.end,
                                task: Either::Right(task.clone()),
                            });
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

        let empty = migration_dbs.is_empty();

        (
            Self {
                empty,
                task_map: migration_dbs,
            },
            new_tasks,
        )
    }

    pub fn commit_importing(&self, switch_arg: SwitchArg) -> Result<(), SwitchError> {
        if let Some(tasks) = self.task_map.get(&switch_arg.meta.db_name) {
            debug!(
                "found tasks for db {} {}",
                switch_arg.meta.db_name,
                tasks.len()
            );

            if let Some(record) = tasks.get(&switch_arg.meta) {
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
                        return importing_task.commit(switch_arg).map_err(|e| match e {
                            MigrationError::NotReady => SwitchError::NotReady,
                            others => SwitchError::MgrErr(others),
                        });
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
            for (_db_name, tasks) in self.task_map.iter() {
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
}

#[derive(Debug)]
pub enum SwitchError {
    InvalidArg,
    TaskNotFound,
    PeerMigrating,
    NotReady,
    MgrErr(MigrationError),
}
