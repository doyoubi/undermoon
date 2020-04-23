use super::scan_task::{RedisScanImportingTask, RedisScanMigratingTask};
use super::task::{ImportingTask, MigratingTask, MigrationError, MigrationState, SwitchArg};
use crate::common::cluster::{ClusterName, MigrationTaskMeta, RangeList, SlotRange, SlotRangeTag};
use crate::common::config::AtomicMigrationConfig;
use crate::common::proto::ProxyClusterMap;
use crate::common::track::TrackedFutureRegistry;
use crate::common::utils::ThreadSafe;
use crate::migration::delete_keys::{DeleteKeysTask, DeleteKeysTaskMap};
use crate::migration::task::MgrSubCmd;
use crate::protocol::Resp;
use crate::protocol::{Array, BulkStr, RedisClientFactory, RespVec};
use crate::proxy::backend::{CmdTask, CmdTaskFactory, ReqTask};
use crate::proxy::blocking::{BlockingHintTask, TaskBlockingControllerFactory};
use crate::proxy::cluster::{ClusterSendError, ClusterTag};
use crate::proxy::sender::{CmdTaskSender, CmdTaskSenderFactory};
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
type ClusterTask<T> = HashMap<MigrationTaskMeta, Arc<MgrTask<T>>>;
type TaskMap<T> = HashMap<ClusterName, ClusterTask<T>>;
type NewMigrationTuple<T> = (MigrationMap<T>, Vec<NewTask<T>>);

pub struct NewTask<T: CmdTask> {
    cluster_name: ClusterName,
    epoch: u64,
    range_list: RangeList,
    task: TaskRecord<T>,
}

pub struct MigrationManager<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe, CTF>
where
    <TSF as CmdTaskSenderFactory>::Sender: ThreadSafe + CmdTaskSender<Task = ReqTask<CTF::Task>>,
    CTF: CmdTaskFactory + ThreadSafe,
    CTF::Task: ClusterTag,
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
    CTF::Task: ClusterTag,
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
        local_cluster_map: &ProxyClusterMap,
        blocking_ctrl_factory: Arc<BCF>,
    ) -> NewMigrationTuple<CTF::Task> {
        old_migration_map.update_from_old_task_map(
            local_cluster_map,
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

    pub fn create_new_deleting_task_map(
        &self,
        old_deleting_task_map: &DeleteKeysTaskMap,
        local_cluster_map: &ProxyClusterMap,
        left_slots_after_change: HashMap<ClusterName, HashMap<String, Vec<SlotRange>>>,
    ) -> (DeleteKeysTaskMap, Vec<Arc<DeleteKeysTask>>) {
        old_deleting_task_map.update_from_old_task_map(
            local_cluster_map,
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
    T: CmdTask + ClusterTag,
{
    empty: bool,
    task_map: TaskMap<T>,
}

impl<T> MigrationMap<T>
where
    T: CmdTask + ClusterTag,
{
    pub fn empty() -> Self {
        Self {
            empty: true,
            task_map: HashMap::new(),
        }
    }

    pub fn info(&self) -> RespVec {
        let tasks = self
            .task_map
            .iter()
            .map(|(cluster_name, tasks)| {
                let mut lines = vec![format!("name: {}", cluster_name)];
                for (task_meta, mgr_task) in tasks.iter() {
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
            })
            .collect::<Vec<RespVec>>();
        Resp::Arr(Array::Arr(tasks))
    }

    pub fn send(&self, mut cmd_task: T) -> Result<(), ClusterSendError<BlockingHintTask<T>>> {
        cmd_task.log_event(TaskEvent::SentToMigrationBackend);

        // Optimization for not having any migration.
        if self.empty {
            return Err(ClusterSendError::SlotNotFound(BlockingHintTask::new(
                cmd_task, false,
            )));
        }

        Self::send_helper(&self.task_map, cmd_task)
    }

    fn send_helper(
        task_map: &TaskMap<T>,
        cmd_task: T,
    ) -> Result<(), ClusterSendError<BlockingHintTask<T>>> {
        let cluster_name = cmd_task.get_cluster_name();
        match task_map.get(cluster_name) {
            Some(tasks) => {
                let slot = match cmd_task.get_slot() {
                    Some(slot) => slot,
                    None => {
                        let resp = Resp::Error("missing key".to_string().into_bytes());
                        cmd_task.set_resp_result(Ok(resp));
                        return Err(ClusterSendError::MissingKey);
                    }
                };

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

                Err(ClusterSendError::SlotNotFound(BlockingHintTask::new(
                    cmd_task, false,
                )))
            }
            None => Err(ClusterSendError::SlotNotFound(BlockingHintTask::new(
                cmd_task, false,
            ))),
        }
    }

    pub fn send_sync_task(
        &self,
        mut cmd_task: T,
    ) -> Result<(), ClusterSendError<BlockingHintTask<T>>> {
        cmd_task.log_event(TaskEvent::SentToMigrationBackend);

        // Optimization for not having any migration.
        if self.empty {
            return Err(ClusterSendError::SlotNotFound(BlockingHintTask::new(
                cmd_task, false,
            )));
        }

        let cluster_name = cmd_task.get_cluster_name();
        if let Some(tasks) = self.task_map.get(cluster_name) {
            let slot = match cmd_task.get_slot() {
                Some(slot) => slot,
                None => {
                    let resp = Resp::Error("missing key".to_string().into_bytes());
                    cmd_task.set_resp_result(Ok(resp));
                    return Err(ClusterSendError::MissingKey);
                }
            };

            for mgr_task in tasks.values() {
                match &mgr_task.task {
                    Either::Left(migrating_task) if migrating_task.contains_slot(slot) => {
                        return migrating_task.send_sync_task(cmd_task)
                    }
                    _ => continue,
                }
            }
        }

        Err(ClusterSendError::SlotNotFound(BlockingHintTask::new(
            cmd_task, false,
        )))
    }

    pub fn get_left_slots_after_change(
        &self,
        new_migration_map: &Self,
        new_cluster_map: &ProxyClusterMap,
    ) -> HashMap<ClusterName, HashMap<String, Vec<SlotRange>>> {
        let mut left_slots = HashMap::new();
        for (cluster_name, cluster_task) in self.task_map.iter() {
            let nodes = match new_cluster_map.get_map().get(cluster_name) {
                Some(nodes) => nodes,
                None => continue,
            };

            for task_meta in cluster_task.keys() {
                if new_migration_map
                    .task_map
                    .get(cluster_name)
                    .and_then(|cluster_task_map| cluster_task_map.get(task_meta))
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
                    .entry(cluster_name.clone())
                    .or_insert_with(HashMap::new)
                    .insert(address.clone(), slots.clone());
            }
        }
        left_slots
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_from_old_task_map<RCF, CTF, BCF, TSF>(
        &self,
        local_cluster_map: &ProxyClusterMap,
        config: Arc<ServerProxyConfig>,
        mgr_config: Arc<AtomicMigrationConfig>,
        client_factory: Arc<RCF>,
        sender_factory: Arc<TSF>,
        cmd_task_factory: Arc<CTF>,
        blocking_ctrl_factory: Arc<BCF>,
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

        let new_cluster_map = local_cluster_map.get_map();

        let mut migration_clusters = HashMap::new();

        for (cluster_name, node_map) in new_cluster_map.iter() {
            for (_node, slot_ranges) in node_map.iter() {
                for slot_range in slot_ranges.iter() {
                    match slot_range.tag {
                        SlotRangeTag::Migrating(ref _meta) => {
                            let migration_meta = MigrationTaskMeta {
                                cluster_name: cluster_name.clone(),
                                slot_range: slot_range.clone(),
                            };
                            if let Some(migrating_task) = old_task_map
                                .get(cluster_name)
                                .and_then(|tasks| tasks.get(&migration_meta))
                            {
                                let tasks = migration_clusters
                                    .entry(cluster_name.clone())
                                    .or_insert_with(HashMap::new);
                                tasks.insert(migration_meta, migrating_task.clone());
                            }
                        }
                        SlotRangeTag::Importing(ref _meta) => {
                            let migration_meta = MigrationTaskMeta {
                                cluster_name: cluster_name.clone(),
                                slot_range: slot_range.clone(),
                            };
                            if let Some(importing_task) = old_task_map
                                .get(cluster_name)
                                .and_then(|tasks| tasks.get(&migration_meta))
                            {
                                let tasks = migration_clusters
                                    .entry(cluster_name.clone())
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

        for (cluster_name, node_map) in new_cluster_map.iter() {
            for (_node, slot_ranges) in node_map.iter() {
                for slot_range in slot_ranges {
                    match slot_range.tag {
                        SlotRangeTag::Migrating(ref meta) => {
                            let epoch = meta.epoch;
                            let migration_meta = MigrationTaskMeta {
                                cluster_name: cluster_name.clone(),
                                slot_range: slot_range.clone(),
                            };

                            if Some(true)
                                == migration_clusters
                                    .get(cluster_name)
                                    .map(|tasks| tasks.contains_key(&migration_meta))
                            {
                                continue;
                            }

                            let ctrl = blocking_ctrl_factory.create(meta.src_node_address.clone());
                            let task = Arc::new(RedisScanMigratingTask::new(
                                config.clone(),
                                mgr_config.clone(),
                                cluster_name.clone(),
                                slot_range.clone(),
                                meta.clone(),
                                client_factory.clone(),
                                ctrl,
                                future_registry.clone(),
                            ));
                            new_tasks.push(NewTask {
                                cluster_name: cluster_name.clone(),
                                epoch,
                                range_list: slot_range.to_range_list(),
                                task: Either::Left(task.clone()),
                            });
                            let tasks = migration_clusters
                                .entry(cluster_name.clone())
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
                                cluster_name: cluster_name.clone(),
                                slot_range: slot_range.clone(),
                            };

                            if Some(true)
                                == migration_clusters
                                    .get(cluster_name)
                                    .map(|tasks| tasks.contains_key(&migration_meta))
                            {
                                continue;
                            }

                            let task = Arc::new(RedisScanImportingTask::new(
                                config.clone(),
                                mgr_config.clone(),
                                meta.clone(),
                                slot_range.clone(),
                                client_factory.clone(),
                                sender_factory.clone(),
                                cmd_task_factory.clone(),
                            ));
                            new_tasks.push(NewTask {
                                cluster_name: cluster_name.clone(),
                                epoch,
                                range_list: slot_range.to_range_list(),
                                task: Either::Right(task.clone()),
                            });
                            let tasks = migration_clusters
                                .entry(cluster_name.clone())
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

        let empty = migration_clusters.is_empty();

        (
            Self {
                empty,
                task_map: migration_clusters,
            },
            new_tasks,
        )
    }

    pub fn handle_switch(
        &self,
        switch_arg: SwitchArg,
        sub_cmd: MgrSubCmd,
    ) -> Result<(), SwitchError> {
        if let Some(tasks) = self.task_map.get(&switch_arg.meta.cluster_name) {
            debug!(
                "found tasks for cluster {} {}",
                switch_arg.meta.cluster_name,
                tasks.len()
            );

            if let Some(mgr_task) = tasks.get(&switch_arg.meta) {
                debug!("found record for cluster {}", switch_arg.meta.cluster_name);
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
            for (_cluster_name, tasks) in self.task_map.iter() {
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

    pub fn get_states(&self, cluster_name: &ClusterName) -> HashMap<RangeList, MigrationState> {
        let mut m = HashMap::new();
        if let Some(tasks) = self.task_map.get(cluster_name) {
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
