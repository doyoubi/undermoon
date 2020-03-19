use super::scan_task::{RedisScanImportingTask, RedisScanMigratingTask};
use super::task::{ImportingTask, MigratingTask, MigrationError, MigrationState, SwitchArg};
use crate::common::cluster::{DBName, MigrationTaskMeta, RangeList, SlotRange, SlotRangeTag};
use crate::common::config::AtomicMigrationConfig;
use crate::common::db::ProxyDBMap;
use crate::common::track::TrackedFutureRegistry;
use crate::common::utils::{get_slot, ThreadSafe};
use crate::migration::delete_keys::{DeleteKeysTask, DeleteKeysTaskMap};
use crate::migration::task::MgrSubCmd;
use crate::protocol::RedisClientFactory;
use crate::protocol::Resp;
use crate::proxy::backend::{
    CmdTask, CmdTaskFactory, CmdTaskSender, CmdTaskSenderFactory, ReqTask,
};
use crate::proxy::blocking::{BlockingHintTask, TaskBlockingControllerFactory};
use crate::proxy::database::{DBSendError, DBTag};
use crate::proxy::service::ServerProxyConfig;
use crate::proxy::slowlog::TaskEvent;
use futures::TryFutureExt;
use itertools::Either;
use std::collections::HashMap;
use std::sync::Arc;

type TaskRecord<T> = Either<Arc<dyn MigratingTask<Task = T>>, Arc<dyn ImportingTask<Task = T>>>;
struct MgrTask<T: CmdTask> {
    task: TaskRecord<T>,
    _stop_handle: Option<Box<dyn Drop + Send + Sync + 'static>>,
}
type DBTask<T> = HashMap<MigrationTaskMeta, Arc<MgrTask<T>>>;
type TaskMap<T> = HashMap<DBName, DBTask<T>>;
type NewMigrationTuple<T> = (MigrationMap<T>, Vec<NewTask<T>>);

pub struct NewTask<T: CmdTask> {
    db_name: DBName,
    epoch: u64,
    range_list: RangeList,
    task: TaskRecord<T>,
}

pub struct MigrationManager<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe, CTF>
where
    <TSF as CmdTaskSenderFactory>::Sender: ThreadSafe + CmdTaskSender<Task = ReqTask<CTF::Task>>,
    CTF: CmdTaskFactory + ThreadSafe,
    CTF::Task: DBTag,
{
    config: Arc<ServerProxyConfig>,
    mgr_config: Arc<AtomicMigrationConfig>,
    client_factory: Arc<RCF>,
    sender_factory: Arc<TSF>,
    cmd_task_factory: Arc<CTF>,
    future_registry: Arc<TrackedFutureRegistry>,
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe, CTF>
    MigrationManager<RCF, TSF, CTF>
where
    <TSF as CmdTaskSenderFactory>::Sender: ThreadSafe + CmdTaskSender<Task = ReqTask<CTF::Task>>,
    CTF: CmdTaskFactory + ThreadSafe,
    CTF::Task: DBTag,
{
    pub fn new(
        config: Arc<ServerProxyConfig>,
        mgr_config: Arc<AtomicMigrationConfig>,
        client_factory: Arc<RCF>,
        sender_factory: Arc<TSF>,
        cmd_task_factory: Arc<CTF>,
        future_registry: Arc<TrackedFutureRegistry>,
    ) -> Self {
        Self {
            config,
            mgr_config,
            client_factory,
            sender_factory,
            cmd_task_factory,
            future_registry,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_new_migration_map<BCF: TaskBlockingControllerFactory>(
        &self,
        old_migration_map: &MigrationMap<CTF::Task>,
        local_db_map: &ProxyDBMap,
        blocking_ctrl_factory: Arc<BCF>,
    ) -> NewMigrationTuple<CTF::Task> {
        old_migration_map.update_from_old_task_map(
            local_db_map,
            self.config.clone(),
            self.mgr_config.clone(),
            self.client_factory.clone(),
            self.sender_factory.clone(),
            self.cmd_task_factory.clone(),
            blocking_ctrl_factory,
            self.future_registry.clone(),
        )
    }

    pub fn run_tasks(&self, new_tasks: Vec<NewTask<CTF::Task>>) {
        if new_tasks.is_empty() {
            return;
        }

        for NewTask {
            db_name,
            epoch,
            range_list,
            task,
        } in new_tasks.into_iter()
        {
            match task {
                Either::Left(migrating_task) => {
                    info!(
                        "spawn slot migrating task {} {} {}",
                        db_name,
                        epoch,
                        range_list.to_strings().join(" "),
                    );
                    let desc = format!(
                        "migration: tag=migrating db_name={}, epoch={}, slot_range=({})",
                        db_name,
                        epoch,
                        range_list.to_strings().join(" "),
                    );

                    let fut = async move {
                        if let Err(err) = migrating_task.start().await {
                            error!(
                                "master slot task {} {} exit {:?} slot_range {}",
                                db_name,
                                epoch,
                                err,
                                range_list.to_strings().join(" "),
                            );
                        }
                    };

                    let fut = TrackedFutureRegistry::wrap(self.future_registry.clone(), fut, desc);
                    tokio::spawn(fut);
                }
                Either::Right(importing_task) => {
                    info!(
                        "spawn slot importing task {} {} {}",
                        db_name,
                        epoch,
                        range_list.to_strings().join(" "),
                    );
                    let desc = format!(
                        "migration: tag=importing db_name={}, epoch={}, slot_range=({})",
                        db_name,
                        epoch,
                        range_list.to_strings().join(" "),
                    );

                    let fut = async move {
                        if let Err(err) = importing_task.start().await {
                            warn!(
                                "replica slot task {} {} exit {:?} slot_range {}",
                                db_name,
                                epoch,
                                err,
                                range_list.to_strings().join(" "),
                            );
                        }
                    };

                    let fut = TrackedFutureRegistry::wrap(self.future_registry.clone(), fut, desc);
                    tokio::spawn(fut);
                }
            }
        }
        info!("spawn finished");
    }

    pub fn create_new_deleting_task_map(
        &self,
        old_deleting_task_map: &DeleteKeysTaskMap,
        local_db_map: &ProxyDBMap,
        left_slots_after_change: HashMap<DBName, HashMap<String, Vec<SlotRange>>>,
    ) -> (DeleteKeysTaskMap, Vec<Arc<DeleteKeysTask>>) {
        old_deleting_task_map.update_from_old_task_map(
            local_db_map,
            left_slots_after_change,
            self.mgr_config.clone(),
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
                let slot_ranges = task.get_slot_ranges();
                let desc = format!(
                    "deleting_keys: address={} slot_ranges={}",
                    address, slot_ranges
                );
                let fut = fut
                    .map_ok(move |()| info!("deleting keys for {} stopped", address))
                    .map_err(move |e| match e {
                        MigrationError::Canceled => info!("task for deleting keys get canceled"),
                        _ => error!("task for deleting keys exit with error {:?}", e),
                    });

                let fut = TrackedFutureRegistry::wrap(self.future_registry.clone(), fut, desc);
                tokio::spawn(fut);
            }
        }
        info!("spawn finished for deleting keys");
    }
}

pub struct MigrationMap<T>
where
    T: CmdTask + DBTag,
{
    empty: bool,
    task_map: TaskMap<T>,
}

impl<T> MigrationMap<T>
where
    T: CmdTask + DBTag,
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
                            "{} -> {} {}",
                            task_meta
                                .slot_range
                                .range_list
                                .clone()
                                .to_strings()
                                .join(" "),
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

    pub fn send(&self, mut cmd_task: T) -> Result<(), DBSendError<BlockingHintTask<T>>> {
        cmd_task.log_event(TaskEvent::SentToMigrationDB);
        self.send_to_db(cmd_task)
    }

    pub fn send_to_db(&self, cmd_task: T) -> Result<(), DBSendError<BlockingHintTask<T>>> {
        // Optimization for not having any migration.
        if self.empty {
            return Err(DBSendError::SlotNotFound(BlockingHintTask::new(
                cmd_task, false,
            )));
        }

        Self::send_helper(&self.task_map, cmd_task)
    }

    fn send_helper(
        task_map: &TaskMap<T>,
        cmd_task: T,
    ) -> Result<(), DBSendError<BlockingHintTask<T>>> {
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

                for mgr_task in tasks.values() {
                    match &mgr_task.task {
                        Either::Left(migrating_task) if migrating_task.contains_slot(slot) => {
                            return migrating_task.send(cmd_task)
                        }
                        Either::Right(importing_task) if importing_task.contains_slot(slot) => {
                            return importing_task.send(cmd_task)
                        }
                        _ => continue,
                    }
                }

                Err(DBSendError::SlotNotFound(BlockingHintTask::new(
                    cmd_task, false,
                )))
            }
            None => Err(DBSendError::SlotNotFound(BlockingHintTask::new(
                cmd_task, false,
            ))),
        }
    }

    pub fn get_left_slots_after_change(
        &self,
        new_migration_map: &Self,
        new_db_map: &ProxyDBMap,
    ) -> HashMap<DBName, HashMap<String, Vec<SlotRange>>> {
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

    #[allow(clippy::too_many_arguments)]
    pub fn update_from_old_task_map<RCF, CTF, BCF, TSF>(
        &self,
        local_db_map: &ProxyDBMap,
        config: Arc<ServerProxyConfig>,
        mgr_config: Arc<AtomicMigrationConfig>,
        client_factory: Arc<RCF>,
        sender_factory: Arc<TSF>,
        cmd_task_factory: Arc<CTF>,
        blocking_ctrl_map: Arc<BCF>,
        future_registry: Arc<TrackedFutureRegistry>,
    ) -> (Self, Vec<NewTask<T>>)
    where
        RCF: RedisClientFactory,
        CTF: CmdTaskFactory<Task = T> + ThreadSafe,
        BCF: TaskBlockingControllerFactory,
        TSF: CmdTaskSenderFactory + ThreadSafe,
        <TSF as CmdTaskSenderFactory>::Sender: CmdTaskSender<Task = ReqTask<T>> + ThreadSafe,
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
                            if let Some(migrating_task) = old_task_map
                                .get(db_name)
                                .and_then(|tasks| tasks.get(&migration_meta))
                            {
                                let tasks = migration_dbs
                                    .entry(db_name.clone())
                                    .or_insert_with(HashMap::new);
                                tasks.insert(migration_meta, migrating_task.clone());
                            }
                        }
                        SlotRangeTag::Importing(ref _meta) => {
                            let migration_meta = MigrationTaskMeta {
                                db_name: db_name.clone(),
                                slot_range: slot_range.clone(),
                            };
                            if let Some(importing_task) = old_task_map
                                .get(db_name)
                                .and_then(|tasks| tasks.get(&migration_meta))
                            {
                                let tasks = migration_dbs
                                    .entry(db_name.clone())
                                    .or_insert_with(HashMap::new);
                                tasks.insert(migration_meta, importing_task.clone());
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
                    match slot_range.tag {
                        SlotRangeTag::Migrating(ref meta) => {
                            let epoch = meta.epoch;
                            let migration_meta = MigrationTaskMeta {
                                db_name: db_name.clone(),
                                slot_range: slot_range.clone(),
                            };

                            if Some(true)
                                == migration_dbs
                                    .get(db_name)
                                    .map(|tasks| tasks.contains_key(&migration_meta))
                            {
                                continue;
                            }

                            let ctrl = blocking_ctrl_map.create(meta.src_node_address.clone());
                            let task = Arc::new(RedisScanMigratingTask::new(
                                config.clone(),
                                mgr_config.clone(),
                                db_name.clone(),
                                slot_range.clone(),
                                meta.clone(),
                                client_factory.clone(),
                                ctrl,
                                future_registry.clone(),
                            ));
                            new_tasks.push(NewTask {
                                db_name: db_name.clone(),
                                epoch,
                                range_list: slot_range.to_range_list(),
                                task: Either::Left(task.clone()),
                            });
                            let tasks = migration_dbs
                                .entry(db_name.clone())
                                .or_insert_with(HashMap::new);
                            let stop_handle = task.get_stop_handle();
                            let mgr_task = MgrTask {
                                task: Either::Left(task),
                                _stop_handle: stop_handle,
                            };
                            tasks.insert(migration_meta, Arc::new(mgr_task));
                        }
                        SlotRangeTag::Importing(ref meta) => {
                            let epoch = meta.epoch;
                            let migration_meta = MigrationTaskMeta {
                                db_name: db_name.clone(),
                                slot_range: slot_range.clone(),
                            };

                            if Some(true)
                                == migration_dbs
                                    .get(db_name)
                                    .map(|tasks| tasks.contains_key(&migration_meta))
                            {
                                continue;
                            }

                            let task = Arc::new(RedisScanImportingTask::new(
                                mgr_config.clone(),
                                meta.clone(),
                                slot_range.clone(),
                                client_factory.clone(),
                                sender_factory.clone(),
                                cmd_task_factory.clone(),
                            ));
                            new_tasks.push(NewTask {
                                db_name: db_name.clone(),
                                epoch,
                                range_list: slot_range.to_range_list(),
                                task: Either::Right(task.clone()),
                            });
                            let tasks = migration_dbs
                                .entry(db_name.clone())
                                .or_insert_with(HashMap::new);
                            let stop_handle = task.get_stop_handle();
                            let mgr_task = MgrTask {
                                task: Either::Right(task),
                                _stop_handle: stop_handle,
                            };
                            tasks.insert(migration_meta, Arc::new(mgr_task));
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

    pub fn handle_switch(
        &self,
        switch_arg: SwitchArg,
        sub_cmd: MgrSubCmd,
    ) -> Result<(), SwitchError> {
        if let Some(tasks) = self.task_map.get(&switch_arg.meta.db_name) {
            debug!(
                "found tasks for db {} {}",
                switch_arg.meta.db_name,
                tasks.len()
            );

            if let Some(mgr_task) = tasks.get(&switch_arg.meta) {
                debug!("found record for db {}", switch_arg.meta.db_name);
                match &mgr_task.task {
                    Either::Left(_migrating_task) => {
                        error!(
                            "Received switch request when migrating {:?}",
                            switch_arg.meta
                        );
                        return Err(SwitchError::PeerMigrating);
                    }
                    Either::Right(importing_task) => {
                        return importing_task.handle_switch(switch_arg, sub_cmd).map_err(
                            |e| match e {
                                MigrationError::NotReady => SwitchError::NotReady,
                                others => SwitchError::MgrErr(others),
                            },
                        );
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
                for (meta, mgr_task) in tasks.iter() {
                    let state = match &mgr_task.task {
                        Either::Left(migrating_task) => migrating_task.get_state(),
                        Either::Right(importing_task) => importing_task.get_state(),
                    };
                    if state == MigrationState::SwitchCommitted {
                        metadata.push(meta.clone());
                    }
                }
            }
        }
        metadata
    }

    pub fn get_states(&self, db_name: &DBName) -> HashMap<RangeList, MigrationState> {
        let mut m = HashMap::new();
        if let Some(tasks) = self.task_map.get(db_name) {
            for (meta, mgr_task) in tasks.iter() {
                let state = match &mgr_task.task {
                    Either::Left(migrating_task) => migrating_task.get_state(),
                    Either::Right(importing_task) => importing_task.get_state(),
                };
                m.insert(meta.slot_range.to_range_list(), state);
            }
        }
        m
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
