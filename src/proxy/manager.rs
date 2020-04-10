use super::backend::{
    gen_migration_sender_factory, BackendError, CmdTaskSender, CmdTaskSenderFactory,
    MigrationBackendSenderFactory,
};
use super::blocking::{
    gen_basic_blocking_sender_factory, gen_blocking_sender_factory, BasicBlockingSenderFactory,
    BlockingBackendSenderFactory, BlockingCmdTaskSender, BlockingMap, CounterTask,
};
use super::cluster::{
    ClusterBackendMap, ClusterMetaError, ClusterSendError, ClusterTag, DEFAULT_CLUSTER,
};
use super::reply::{DecompressCommitHandlerFactory, ReplyCommitHandlerFactory};
use super::service::ServerProxyConfig;
use super::session::{CmdCtx, CmdCtxFactory};
use super::slowlog::TaskEvent;
use crate::common::cluster::{ClusterName, MigrationTaskMeta, SlotRangeTag};
use crate::common::config::AtomicMigrationConfig;
use crate::common::proto::ProxyClusterMeta;
use crate::common::track::TrackedFutureRegistry;
use crate::migration::delete_keys::DeleteKeysTaskMap;
use crate::migration::manager::{MigrationManager, MigrationMap, SwitchError};
use crate::migration::task::MgrSubCmd;
use crate::migration::task::SwitchArg;
use crate::protocol::{Array, BulkStr, RedisClientFactory, Resp, RespPacket, RespVec};
use crate::proxy::backend::{CmdTask, ConnFactory};
use crate::replication::manager::ReplicatorManager;
use crate::replication::replicator::ReplicatorMeta;
use arc_swap::ArcSwap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub struct MetaMap<S: CmdTaskSender, T>
where
    <S as CmdTaskSender>::Task: ClusterTag,
    T: CmdTask + ClusterTag,
{
    cluster_map: ClusterBackendMap<S>,
    migration_map: MigrationMap<T>,
    deleting_task_map: DeleteKeysTaskMap,
}

impl<S: CmdTaskSender, T> MetaMap<S, T>
where
    <S as CmdTaskSender>::Task: ClusterTag,
    T: CmdTask + ClusterTag,
{
    pub fn empty() -> Self {
        let cluster_map = ClusterBackendMap::default();
        let migration_map = MigrationMap::empty();
        let deleting_task_map = DeleteKeysTaskMap::empty();
        Self {
            cluster_map,
            migration_map,
            deleting_task_map,
        }
    }

    pub fn get_cluster_map(&self) -> &ClusterBackendMap<S> {
        &self.cluster_map
    }
}

type BasicSenderFactory<C> =
    BasicBlockingSenderFactory<DecompressCommitHandlerFactory<CounterTask<CmdCtx>, C>, C>;
type SenderFactory<C> = BlockingBackendSenderFactory<
    DecompressCommitHandlerFactory<CounterTask<CmdCtx>, C>,
    C,
    BlockingTaskRetrySender<C>,
>;
type MigrationSenderFactory<C> = MigrationBackendSenderFactory<ReplyCommitHandlerFactory, C>;
pub type SharedMetaMap<C> =
    Arc<ArcSwap<MetaMap<<SenderFactory<C> as CmdTaskSenderFactory>::Sender, CmdCtx>>>;

pub struct MetaManager<F: RedisClientFactory, C: ConnFactory<Pkt = RespPacket>> {
    config: Arc<ServerProxyConfig>,
    // Now replicator is not in meta_map, if later we need consistency
    // between replication metadata and other metadata, we should put that
    // inside meta_map.
    meta_map: SharedMetaMap<C>,
    epoch: AtomicU64,
    lock: Mutex<()>, // This is the write lock for `epoch`, `cluster`, and `task`.
    replicator_manager: ReplicatorManager<F>,
    migration_manager: MigrationManager<F, MigrationSenderFactory<C>, CmdCtxFactory>,
    sender_factory: SenderFactory<C>,
    blocking_map: Arc<BlockingMap<BasicSenderFactory<C>, BlockingTaskRetrySender<C>>>,
}

impl<F: RedisClientFactory, C: ConnFactory<Pkt = RespPacket>> MetaManager<F, C> {
    pub fn new(
        config: Arc<ServerProxyConfig>,
        client_factory: Arc<F>,
        conn_factory: Arc<C>,
        meta_map: SharedMetaMap<C>,
        future_registry: Arc<TrackedFutureRegistry>,
    ) -> Self {
        let reply_handler_factory = Arc::new(DecompressCommitHandlerFactory::new(meta_map.clone()));
        let blocking_task_sender = Arc::new(BlockingTaskRetrySender::new(meta_map.clone()));
        let basic_sender_factory = gen_basic_blocking_sender_factory(
            config.clone(),
            reply_handler_factory,
            conn_factory.clone(),
            future_registry.clone(),
        );
        let blocking_map = Arc::new(BlockingMap::new(basic_sender_factory, blocking_task_sender));
        let sender_factory = gen_blocking_sender_factory(blocking_map.clone());
        let migration_sender_factory = Arc::new(gen_migration_sender_factory(
            config.clone(),
            Arc::new(ReplyCommitHandlerFactory::default()),
            conn_factory,
            future_registry.clone(),
        ));
        let cmd_ctx_factory = Arc::new(CmdCtxFactory::default());
        let migration_config = Arc::new(AtomicMigrationConfig::default());
        let config_clone = config.clone();
        Self {
            config,
            meta_map,
            epoch: AtomicU64::new(0),
            lock: Mutex::new(()),
            replicator_manager: ReplicatorManager::new(
                client_factory.clone(),
                future_registry.clone(),
            ),
            migration_manager: MigrationManager::new(
                config_clone,
                migration_config,
                client_factory,
                migration_sender_factory,
                cmd_ctx_factory,
                future_registry,
            ),
            sender_factory,
            blocking_map,
        }
    }

    pub fn gen_cluster_nodes(&self, cluster_name: ClusterName) -> String {
        let meta_map = self.meta_map.load();
        let migration_states = meta_map.migration_map.get_states(&cluster_name);
        meta_map.cluster_map.gen_cluster_nodes(
            cluster_name,
            self.config.announce_address.clone(),
            &migration_states,
        )
    }

    pub fn gen_cluster_slots(&self, cluster_name: ClusterName) -> Result<RespVec, String> {
        let meta_map = self.meta_map.load();
        let migration_states = meta_map.migration_map.get_states(&cluster_name);
        meta_map.cluster_map.gen_cluster_slots(
            cluster_name,
            self.config.announce_address.clone(),
            &migration_states,
        )
    }

    pub fn get_clusters(&self) -> Vec<ClusterName> {
        self.meta_map.load().cluster_map.get_clusters()
    }

    pub fn set_meta(&self, cluster_meta: ProxyClusterMeta) -> Result<(), ClusterMetaError> {
        let active_redirection = self.config.active_redirection;

        let sender_factory = &self.sender_factory;
        let migration_manager = &self.migration_manager;

        let _guard = self.lock.lock().expect("MetaManager::set_meta");

        if cluster_meta.get_epoch() <= self.epoch.load(Ordering::SeqCst)
            && !cluster_meta.get_flags().force
        {
            return Err(ClusterMetaError::OldEpoch);
        }

        let old_meta_map = self.meta_map.load();
        let cluster_map =
            ClusterBackendMap::from_cluster_map(&cluster_meta, sender_factory, active_redirection);
        let (migration_map, new_tasks) = migration_manager.create_new_migration_map(
            &old_meta_map.migration_map,
            cluster_meta.get_local(),
            self.blocking_map.clone(),
        );

        let left_slots_after_change = old_meta_map
            .migration_map
            .get_left_slots_after_change(&migration_map, cluster_meta.get_local());
        let (deleting_task_map, new_deleting_tasks) = migration_manager
            .create_new_deleting_task_map(
                &old_meta_map.deleting_task_map,
                cluster_meta.get_local(),
                left_slots_after_change,
            );

        self.meta_map.store(Arc::new(MetaMap {
            cluster_map,
            migration_map,
            deleting_task_map,
        }));
        // Should go after the meta_map.store above
        self.epoch.store(cluster_meta.get_epoch(), Ordering::SeqCst);

        self.migration_manager.run_tasks(new_tasks);
        self.migration_manager
            .run_deleting_tasks(new_deleting_tasks);

        Ok(())
    }

    pub fn update_replicators(&self, meta: ReplicatorMeta) -> Result<(), ClusterMetaError> {
        self.replicator_manager.update_replicators(meta)
    }

    pub fn get_replication_info(&self) -> RespVec {
        self.replicator_manager.get_metadata_report()
    }

    pub fn info(&self) -> RespVec {
        let meta_map = self.meta_map.load();
        let cluster_info = meta_map.cluster_map.info();
        let mgr_info = meta_map.migration_map.info();
        let del_info = meta_map.deleting_task_map.info();
        let repl_info = self.replicator_manager.get_metadata_report();
        Resp::Arr(Array::Arr(vec![
            Resp::Bulk(BulkStr::Str(b"Cluster".to_vec())),
            cluster_info,
            Resp::Bulk(BulkStr::Str(b"Replication".to_vec())),
            repl_info,
            Resp::Bulk(BulkStr::Str(b"Migration".to_vec())),
            mgr_info,
            Resp::Bulk(BulkStr::Str(b"DeletingKeyTask".to_vec())),
            del_info,
        ]))
    }

    pub fn handle_switch(
        &self,
        switch_arg: SwitchArg,
        sub_cmd: MgrSubCmd,
    ) -> Result<(), SwitchError> {
        let mut task_meta = switch_arg.meta.clone();

        // The stored meta is with importing tag.
        // We need to change from migrating tag to importing tag.
        let arg_epoch = match switch_arg.meta.slot_range.tag {
            SlotRangeTag::None => return Err(SwitchError::InvalidArg),
            SlotRangeTag::Migrating(ref meta) => {
                let epoch = meta.epoch;
                task_meta.slot_range.tag = SlotRangeTag::Importing(meta.clone());
                epoch
            }
            SlotRangeTag::Importing(ref meta) => meta.epoch,
        };

        if self.epoch.load(Ordering::SeqCst) < arg_epoch {
            return Err(SwitchError::NotReady);
        }

        self.meta_map.load().migration_map.handle_switch(
            SwitchArg {
                version: switch_arg.version,
                meta: task_meta,
            },
            sub_cmd,
        )
    }

    pub fn get_finished_migration_tasks(&self) -> Vec<MigrationTaskMeta> {
        self.meta_map.load().migration_map.get_finished_tasks()
    }

    pub fn send(&self, cmd_ctx: CmdCtx) {
        send_cmd_ctx(&self.meta_map, cmd_ctx);
    }

    pub fn try_select_cluster(&self, mut cmd_ctx: CmdCtx) -> CmdCtx {
        if cmd_ctx.get_cluster_name().as_str() != DEFAULT_CLUSTER {
            return cmd_ctx;
        }

        if let Some(cluster_name) = self.meta_map.load().cluster_map.auto_select_cluster() {
            cmd_ctx.set_cluster_name(cluster_name);
        }
        cmd_ctx
    }

    pub fn get_epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }
}

pub fn send_cmd_ctx<C: ConnFactory<Pkt = RespPacket>>(
    meta_map: &SharedMetaMap<C>,
    cmd_ctx: CmdCtx,
) {
    let meta_map = meta_map.lease();
    let mut cmd_ctx = match meta_map.migration_map.send(cmd_ctx) {
        Ok(()) => return,
        Err(e) => match e {
            ClusterSendError::SlotNotFound(cmd_ctx) => cmd_ctx,
            err => {
                error!("migration send task failed: {:?}", err);
                return;
            }
        },
    };

    cmd_ctx.log_event(TaskEvent::SentToCluster);
    let res = meta_map.cluster_map.send(cmd_ctx);
    if let Err(e) = res {
        match e {
            ClusterSendError::MissingKey => (),
            err => warn!("Failed to forward cmd_ctx: {:?}", err),
        }
    }
}

pub struct BlockingTaskRetrySender<C: ConnFactory<Pkt = RespPacket>> {
    meta_map: SharedMetaMap<C>,
}

impl<C: ConnFactory<Pkt = RespPacket>> BlockingTaskRetrySender<C> {
    fn new(meta_map: SharedMetaMap<C>) -> Self {
        Self { meta_map }
    }
}

impl<C: ConnFactory<Pkt = RespPacket>> CmdTaskSender for BlockingTaskRetrySender<C> {
    type Task = CmdCtx;

    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError> {
        send_cmd_ctx(&self.meta_map, cmd_task);
        Ok(())
    }
}

impl<C: ConnFactory<Pkt = RespPacket>> BlockingCmdTaskSender for BlockingTaskRetrySender<C> {}
