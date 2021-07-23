use super::scan_task::{RedisScanImportingTask, RedisScanMigratingTask};
use super::task::{ImportingTask, MigratingTask, MigrationError, MigrationState, SwitchArg};
use crate::common::cluster::{
    ClusterName, MigrationTaskMeta, RangeList, SlotRange, SlotRangeTag, EMPTY_CLUSTER_NAME,
};
use crate::common::config::{AtomicMigrationConfig, ClusterConfig};
use crate::common::track::TrackedFutureRegistry;
use crate::common::utils::{generate_slot, ThreadSafe};
use crate::migration::task::MgrSubCmd;
use crate::protocol::Resp;
use crate::protocol::{Array, BinSafeStr, BulkStr, RedisClientFactory, RespVec};
use crate::proxy::backend::{CmdTask, CmdTaskFactory, ReqTask};
use crate::proxy::blocking::{BlockingHint, BlockingHintTask, TaskBlockingControllerFactory};
use crate::proxy::cluster::ClusterSendError;
use crate::proxy::command::CmdTypeTuple;
use crate::proxy::migration_backend::WaitableTask;
use crate::proxy::sender::{CmdTaskSender, CmdTaskSenderFactory};
use crate::proxy::service::ServerProxyConfig;
use crate::proxy::slowlog::TaskEvent;
use itertools::Either;
use std::collections::HashMap;
use std::sync::Arc;

type TaskRecord<T> = Either<Arc<dyn MigratingTask<Task = T>>, Arc<dyn ImportingTask<Task = T>>>;
struct MgrTask<T: CmdTask> {
    task: TaskRecord<T>,
    _stop_handle: Option<Box<dyn Drop + Send + Sync + 'static>>,
}
type ClusterTask<T> = HashMap<MigrationTaskMeta, Arc<MgrTask<T>>>;
type NewMigrationTuple<T> = (MigrationMap<T>, Vec<NewTask<T>>);

pub struct NewTask<T: CmdTask> {
    cluster_name: ClusterName,
    epoch: u64,
    range_list: RangeList,
    task: TaskRecord<T>,
}

pub struct MigrationManager<RCF, TSF, DTSF, PTSF, CTF>
where
    RCF: RedisClientFactory,
    <TSF as CmdTaskSenderFactory>::Sender: ThreadSafe + CmdTaskSender<Task = ReqTask<CTF::Task>>,
    TSF: CmdTaskSenderFactory + ThreadSafe,
    <DTSF as CmdTaskSenderFactory>::Sender:
        ThreadSafe + CmdTaskSender<Task = ReqTask<WaitableTask<CTF::Task>>>,
    DTSF: CmdTaskSenderFactory + ThreadSafe,
    <PTSF as CmdTaskSenderFactory>::Sender: ThreadSafe + CmdTaskSender<Task = ReqTask<CTF::Task>>,
    PTSF: CmdTaskSenderFactory + ThreadSafe,
    CTF: CmdTaskFactory + ThreadSafe,
    CTF::Task: CmdTask<TaskType = CmdTypeTuple>,
{
    config: Arc<ServerProxyConfig>,
    client_factory: Arc<RCF>,
    sender_factory: Arc<TSF>,
    dst_sender_factory: Arc<DTSF>,
    proxy_sender_factory: Arc<PTSF>,
    cmd_task_factory: Arc<CTF>,
    future_registry: Arc<TrackedFutureRegistry>,
}

impl<RCF, TSF, DTSF, PTSF, CTF> MigrationManager<RCF, TSF, DTSF, PTSF, CTF>
where
    RCF: RedisClientFactory,
    <TSF as CmdTaskSenderFactory>::Sender: ThreadSafe + CmdTaskSender<Task = ReqTask<CTF::Task>>,
    TSF: CmdTaskSenderFactory + ThreadSafe,
    <DTSF as CmdTaskSenderFactory>::Sender:
        ThreadSafe + CmdTaskSender<Task = ReqTask<WaitableTask<CTF::Task>>>,
    DTSF: CmdTaskSenderFactory + ThreadSafe,
    <PTSF as CmdTaskSenderFactory>::Sender: ThreadSafe + CmdTaskSender<Task = ReqTask<CTF::Task>>,
    PTSF: CmdTaskSenderFactory + ThreadSafe,
    CTF: CmdTaskFactory + ThreadSafe,
    CTF::Task: CmdTask<TaskType = CmdTypeTuple>,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Arc<ServerProxyConfig>,
        client_factory: Arc<RCF>,
        sender_factory: Arc<TSF>,
        dst_sender_factory: Arc<DTSF>,
        proxy_sender_factory: Arc<PTSF>,
        cmd_task_factory: Arc<CTF>,
        future_registry: Arc<TrackedFutureRegistry>,
    ) -> Self {
        Self {
            config,
            client_factory,
            sender_factory,
            dst_sender_factory,
            proxy_sender_factory,
            cmd_task_factory,
            future_registry,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_new_migration_map<BCF: TaskBlockingControllerFactory>(
        &self,
        cluster_name: ClusterName,
        old_migration_map: &MigrationMap<CTF::Task>,
        local_cluster_map: &HashMap<String, Vec<SlotRange>>,
        cluster_config: &ClusterConfig,
        blocking_ctrl_factory: Arc<BCF>,
    ) -> NewMigrationTuple<CTF::Task> {
        // TODO: Remove AtomicMigrationConfig and use MigrationConfig directly.
        let mgr_config = Arc::new(AtomicMigrationConfig::from_config(
            cluster_config.migration_config.clone(),
        ));
        old_migration_map.update_from_old_task_map(
            cluster_name,
            local_cluster_map,
            cluster_config,
            self.config.clone(),
            mgr_config,
            self.client_factory.clone(),
            self.sender_factory.clone(),
            self.dst_sender_factory.clone(),
            self.proxy_sender_factory.clone(),
            self.cmd_task_factory.clone(),
            blocking_ctrl_factory,
        )
    }

    pub fn run_tasks(&self, new_tasks: Vec<NewTask<CTF::Task>>) {
        if new_tasks.is_empty() {
            return;
        }

        for NewTask {
            cluster_name,
            epoch,
            range_list,
            task,
        } in new_tasks.into_iter()
        {
            match task {
                Either::Left(migrating_task) => {
                    info!(
                        "spawn slot migrating task {} {} {}",
                        cluster_name,
                        epoch,
                        range_list.to_strings().join(" "),
                    );
                    let desc = format!(
                        "migration: tag=migrating cluster_name={}, epoch={}, slot_range=({})",
                        cluster_name,
                        epoch,
                        range_list.to_strings().join(" "),
                    );

                    let fut = async move {
                        if let Err(err) = migrating_task.start().await {
                            error!(
                                "master slot task {} {} exit {:?} slot_range {}",
                                cluster_name,
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
                        cluster_name,
                        epoch,
                        range_list.to_strings().join(" "),
                    );
                    let desc = format!(
                        "migration: tag=importing cluster_name={}, epoch={}, slot_range=({})",
                        cluster_name,
                        epoch,
                        range_list.to_strings().join(" "),
                    );

                    let fut = async move {
                        if let Err(err) = importing_task.start().await {
                            warn!(
                                "replica slot task {} {} exit {:?} slot_range {}",
                                cluster_name,
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
}

pub struct MigrationMap<T>
where
    T: CmdTask,
{
    empty: bool,
    cluster_name: ClusterName,
    task_map: ClusterTask<T>,
}

impl<T> MigrationMap<T>
where
    T: CmdTask,
{
    pub fn empty() -> Self {
        Self {
            empty: true,
            cluster_name: EMPTY_CLUSTER_NAME.clone(),
            task_map: HashMap::new(),
        }
    }

    pub fn info(&self) -> RespVec {
        let mut lines = vec![format!("name: {}", self.cluster_name)];
        for (task_meta, mgr_task) in self.task_map.iter() {
            if let Some(migration_meta) = task_meta.slot_range.tag.get_migration_meta() {
                let state = match &mgr_task.task {
                    Either::Left(task) => task.get_state(),
                    Either::Right(task) => task.get_state(),
                };
                lines.push(format!(
                    "{} {} -> {} {}",
                    task_meta
                        .slot_range
                        .range_list
                        .clone()
                        .to_strings()
                        .join(" "),
                    migration_meta.src_node_address,
                    migration_meta.dst_node_address,
                    state,
                ));
            } else {
                error!("invalid slot range migration meta");
            }
        }
        Resp::Arr(Array::Arr(
            lines
                .into_iter()
                .map(|s| Resp::Bulk(BulkStr::Str(s.into_bytes())))
                .collect(),
        ))
    }

    pub fn send(&self, mut cmd_task: T) -> Result<(), ClusterSendError<BlockingHintTask<T>>> {
        cmd_task.log_event(TaskEvent::SentToMigrationBackend);

        // Optimization for not having any migration.
        // When cluster_name is empty, it does not actually contain a cluster.
        if self.empty || self.cluster_name.is_empty() {
            return Err(ClusterSendError::SlotNotFound(BlockingHintTask::new(
                cmd_task,
                BlockingHint::NotBlocking,
            )));
        }

        Self::send_helper(&self.task_map, cmd_task)
    }

    fn send_helper(
        task_map: &ClusterTask<T>,
        cmd_task: T,
    ) -> Result<(), ClusterSendError<BlockingHintTask<T>>> {
        let slot = match cmd_task.get_slot() {
            Some(slot) => slot,
            None => {
                let resp = Resp::Error("missing key".to_string().into_bytes());
                cmd_task.set_resp_result(Ok(resp));
                return Err(ClusterSendError::MissingKey);
            }
        };

        for mgr_task in task_map.values() {
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

        Err(ClusterSendError::SlotNotFound(BlockingHintTask::new(
            cmd_task,
            BlockingHint::NotBlocking,
        )))
    }

    pub async fn send_sync_task(
        &self,
        mut cmd_task: T,
    ) -> Result<(), ClusterSendError<BlockingHintTask<T>>> {
        cmd_task.log_event(TaskEvent::SentToMigrationBackend);

        // Optimization for not having any migration.
        if self.empty {
            return Err(ClusterSendError::SlotNotFound(BlockingHintTask::new(
                cmd_task,
                BlockingHint::NotBlocking,
            )));
        }

        let slot = match cmd_task.get_slot() {
            Some(slot) => slot,
            None => {
                let resp = Resp::Error("missing key".to_string().into_bytes());
                cmd_task.set_resp_result(Ok(resp));
                return Err(ClusterSendError::MissingKey);
            }
        };

        for mgr_task in self.task_map.values() {
            match &mgr_task.task {
                Either::Left(migrating_task) if migrating_task.contains_slot(slot) => {
                    return migrating_task.send_sync_task(cmd_task).await
                }
                _ => continue,
            }
        }

        Err(ClusterSendError::SlotNotFound(BlockingHintTask::new(
            cmd_task,
            BlockingHint::NotBlocking,
        )))
    }

    pub fn keys_are_importing(&self, keys: &[BinSafeStr]) -> bool {
        let slots: Vec<usize> = keys
            .iter()
            .map(|key| generate_slot(key.as_slice()))
            .collect();
        for mgr_task in self.task_map.values() {
            if let Either::Right(importing_task) = &mgr_task.task {
                for slot in slots.iter() {
                    if importing_task.contains_slot(*slot) {
                        return true;
                    }
                }
            }
        }

        false
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_from_old_task_map<RCF, CTF, BCF, TSF, DTSF, PTSF>(
        &self,
        cluster_name: ClusterName,
        local_cluster_map: &HashMap<String, Vec<SlotRange>>,
        cluster_config: &ClusterConfig,
        config: Arc<ServerProxyConfig>,
        mgr_config: Arc<AtomicMigrationConfig>,
        client_factory: Arc<RCF>,
        sender_factory: Arc<TSF>,
        dst_sender_factory: Arc<DTSF>,
        proxy_sender_factory: Arc<PTSF>,
        cmd_task_factory: Arc<CTF>,
        blocking_ctrl_factory: Arc<BCF>,
    ) -> (Self, Vec<NewTask<T>>)
    where
        RCF: RedisClientFactory,
        CTF: CmdTaskFactory<Task = T> + ThreadSafe,
        CTF::Task: CmdTask<TaskType = CmdTypeTuple>,
        BCF: TaskBlockingControllerFactory,
        TSF: CmdTaskSenderFactory + ThreadSafe,
        <TSF as CmdTaskSenderFactory>::Sender: CmdTaskSender<Task = ReqTask<T>> + ThreadSafe,
        DTSF: CmdTaskSenderFactory + ThreadSafe,
        <DTSF as CmdTaskSenderFactory>::Sender:
            CmdTaskSender<Task = ReqTask<WaitableTask<T>>> + ThreadSafe,
        PTSF: CmdTaskSenderFactory + ThreadSafe,
        <PTSF as CmdTaskSenderFactory>::Sender: CmdTaskSender<Task = ReqTask<T>> + ThreadSafe,
    {
        let old_task_map = &self.task_map;

        let mut migration_tasks = HashMap::new();

        for slot_ranges in local_cluster_map.values() {
            for slot_range in slot_ranges.iter() {
                match slot_range.tag {
                    SlotRangeTag::Migrating(ref _meta) => {
                        let migration_meta = MigrationTaskMeta {
                            cluster_name: cluster_name.clone(),
                            slot_range: slot_range.clone(),
                        };
                        if let Some(migrating_task) = old_task_map.get(&migration_meta) {
                            migration_tasks.insert(migration_meta, migrating_task.clone());
                        }
                    }
                    SlotRangeTag::Importing(ref _meta) => {
                        let migration_meta = MigrationTaskMeta {
                            cluster_name: cluster_name.clone(),
                            slot_range: slot_range.clone(),
                        };
                        if let Some(importing_task) = old_task_map.get(&migration_meta) {
                            migration_tasks.insert(migration_meta, importing_task.clone());
                        }
                    }
                    SlotRangeTag::None => continue,
                }
            }
        }

        let mut new_tasks = Vec::new();

        for slot_ranges in local_cluster_map.values() {
            for slot_range in slot_ranges {
                match slot_range.tag {
                    SlotRangeTag::Migrating(ref meta) => {
                        let epoch = meta.epoch;
                        let migration_meta = MigrationTaskMeta {
                            cluster_name: cluster_name.clone(),
                            slot_range: slot_range.clone(),
                        };

                        if migration_tasks.contains_key(&migration_meta) {
                            continue;
                        }

                        let cluster_mgr_config = Arc::new(AtomicMigrationConfig::from_config(
                            cluster_config.migration_config.clone(),
                        ));

                        let ctrl = blocking_ctrl_factory.create(meta.src_node_address.clone());
                        let task = Arc::new(RedisScanMigratingTask::new(
                            config.clone(),
                            cluster_mgr_config,
                            cluster_name.clone(),
                            slot_range.clone(),
                            meta.clone(),
                            client_factory.clone(),
                            ctrl,
                        ));
                        new_tasks.push(NewTask {
                            cluster_name: cluster_name.clone(),
                            epoch,
                            range_list: slot_range.to_range_list(),
                            task: Either::Left(task.clone()),
                        });
                        let stop_handle = task.get_stop_handle();
                        let mgr_task = MgrTask {
                            task: Either::Left(task),
                            _stop_handle: stop_handle,
                        };
                        migration_tasks.insert(migration_meta, Arc::new(mgr_task));
                    }
                    SlotRangeTag::Importing(ref meta) => {
                        let epoch = meta.epoch;
                        let migration_meta = MigrationTaskMeta {
                            cluster_name: cluster_name.clone(),
                            slot_range: slot_range.clone(),
                        };

                        if migration_tasks.contains_key(&migration_meta) {
                            continue;
                        }

                        let task = Arc::new(RedisScanImportingTask::new(
                            config.clone(),
                            mgr_config.clone(),
                            meta.clone(),
                            slot_range.clone(),
                            client_factory.clone(),
                            sender_factory.clone(),
                            dst_sender_factory.clone(),
                            proxy_sender_factory.clone(),
                            cmd_task_factory.clone(),
                        ));
                        new_tasks.push(NewTask {
                            cluster_name: cluster_name.clone(),
                            epoch,
                            range_list: slot_range.to_range_list(),
                            task: Either::Right(task.clone()),
                        });
                        let stop_handle = task.get_stop_handle();
                        let mgr_task = MgrTask {
                            task: Either::Right(task),
                            _stop_handle: stop_handle,
                        };
                        migration_tasks.insert(migration_meta, Arc::new(mgr_task));
                    }
                    SlotRangeTag::None => continue,
                }
            }
        }

        let empty = migration_tasks.is_empty();

        (
            Self {
                empty,
                cluster_name,
                task_map: migration_tasks,
            },
            new_tasks,
        )
    }

    pub fn handle_switch(
        &self,
        switch_arg: SwitchArg,
        sub_cmd: MgrSubCmd,
    ) -> Result<(), SwitchError> {
        if self.cluster_name == switch_arg.meta.cluster_name {
            debug!(
                "found tasks for cluster {} {}",
                switch_arg.meta.cluster_name,
                self.task_map.len()
            );

            if let Some(mgr_task) = self.task_map.get(&switch_arg.meta) {
                debug!("found record for cluster {}", switch_arg.meta.cluster_name);
                return match &mgr_task.task {
                    Either::Left(_migrating_task) => {
                        error!(
                            "Received switch request when migrating {:?}",
                            switch_arg.meta
                        );
                        Err(SwitchError::PeerMigrating)
                    }
                    Either::Right(importing_task) => importing_task
                        .handle_switch(switch_arg, sub_cmd)
                        .map_err(|e| match e {
                            MigrationError::NotReady => SwitchError::NotReady,
                            others => SwitchError::MgrErr(others),
                        }),
                };
            }
        }

        warn!("No corresponding task found {:?}", switch_arg.meta);
        Err(SwitchError::TaskNotFound)
    }

    pub fn get_finished_tasks(&self) -> Vec<MigrationTaskMeta> {
        let mut metadata = vec![];
        {
            for (meta, mgr_task) in self.task_map.iter() {
                let state = match &mgr_task.task {
                    Either::Left(migrating_task) => migrating_task.get_state(),
                    Either::Right(importing_task) => importing_task.get_state(),
                };
                if state == MigrationState::SwitchCommitted {
                    metadata.push(meta.clone());
                }
            }
        }
        metadata
    }

    pub fn get_states(&self) -> HashMap<RangeList, MigrationState> {
        let mut m = HashMap::new();
        for (meta, mgr_task) in self.task_map.iter() {
            let state = match &mgr_task.task {
                Either::Left(migrating_task) => migrating_task.get_state(),
                Either::Right(importing_task) => importing_task.get_state(),
            };
            m.insert(meta.slot_range.to_range_list(), state);
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
