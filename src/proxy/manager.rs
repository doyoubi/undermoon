use super::backend::{
    gen_migration_sender_factory, BackendError, CmdTaskSender, CmdTaskSenderFactory,
    MigrationBackendSenderFactory,
};
use super::blocking::{
    gen_basic_blocking_sender_factory, gen_blocking_sender_factory, BasicBlockingSenderFactory,
    BlockingBackendSenderFactory, BlockingCmdTaskSender, BlockingMap, CounterTask,
};
use super::database::{DBError, DBSendError, DBTag, DatabaseMap, DEFAULT_DB};
use super::reply::{DecompressCommitHandlerFactory, ReplyCommitHandlerFactory};
use super::service::ServerProxyConfig;
use super::session::{CmdCtx, CmdCtxFactory};
use super::slowlog::TaskEvent;
use crate::common::cluster::{DBName, MigrationTaskMeta, SlotRangeTag};
use crate::common::config::AtomicMigrationConfig;
use crate::common::db::ProxyDBMeta;
use crate::common::track::TrackedFutureRegistry;
use crate::migration::delete_keys::DeleteKeysTaskMap;
use crate::migration::manager::{MigrationManager, MigrationMap, SwitchError};
use crate::migration::task::MgrSubCmd;
use crate::migration::task::SwitchArg;
use crate::protocol::{RedisClientFactory, RespPacket, RespVec};
use crate::proxy::backend::{CmdTask, DefaultConnFactory};
use crate::replication::manager::ReplicatorManager;
use crate::replication::replicator::ReplicatorMeta;
use arc_swap::ArcSwap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub struct MetaMap<S: CmdTaskSender, T>
where
    <S as CmdTaskSender>::Task: DBTag,
    T: CmdTask + DBTag,
{
    db_map: DatabaseMap<S>,
    migration_map: MigrationMap<T>,
    deleting_task_map: DeleteKeysTaskMap,
}

impl<S: CmdTaskSender, T> MetaMap<S, T>
where
    <S as CmdTaskSender>::Task: DBTag,
    T: CmdTask + DBTag,
{
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let db_map = DatabaseMap::default();
        let migration_map = MigrationMap::new();
        let deleting_task_map = DeleteKeysTaskMap::new();
        Self {
            db_map,
            migration_map,
            deleting_task_map,
        }
    }

    pub fn get_db_map(&self) -> &DatabaseMap<S> {
        &self.db_map
    }
}

type BasicSenderFactory = BasicBlockingSenderFactory<
    DecompressCommitHandlerFactory<CounterTask<CmdCtx>>,
    DefaultConnFactory<RespPacket>,
>;
type SenderFactory = BlockingBackendSenderFactory<
    DecompressCommitHandlerFactory<CounterTask<CmdCtx>>,
    DefaultConnFactory<RespPacket>,
    BlockingTaskRetrySender,
>;
type MigrationSenderFactory =
    MigrationBackendSenderFactory<ReplyCommitHandlerFactory, DefaultConnFactory<RespPacket>>;
pub type SharedMetaMap =
    Arc<ArcSwap<MetaMap<<SenderFactory as CmdTaskSenderFactory>::Sender, CmdCtx>>>;

pub struct MetaManager<F: RedisClientFactory> {
    config: Arc<ServerProxyConfig>,
    // Now replicator is not in meta_map, if later we need consistency
    // between replication metadata and other metadata, we should put that
    // inside meta_map.
    meta_map: SharedMetaMap,
    epoch: AtomicU64,
    lock: Mutex<()>, // This is the write lock for `epoch`, `db`, and `task`.
    replicator_manager: ReplicatorManager<F>,
    migration_manager: MigrationManager<F, MigrationSenderFactory, CmdCtxFactory>,
    sender_factory: SenderFactory,
    blocking_map: Arc<BlockingMap<BasicSenderFactory, BlockingTaskRetrySender>>,
}

impl<F: RedisClientFactory> MetaManager<F> {
    pub fn new(
        config: Arc<ServerProxyConfig>,
        client_factory: Arc<F>,
        meta_map: SharedMetaMap,
        future_registry: Arc<TrackedFutureRegistry>,
    ) -> Self {
        let reply_handler_factory = Arc::new(DecompressCommitHandlerFactory::new(meta_map.clone()));
        let conn_factory = Arc::new(DefaultConnFactory::default());
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

    pub fn gen_cluster_nodes(&self, db_name: DBName) -> String {
        let meta_map = self.meta_map.load();
        let migration_states = meta_map.migration_map.get_states(&db_name);
        meta_map.db_map.gen_cluster_nodes(
            db_name,
            self.config.announce_address.clone(),
            &migration_states,
        )
    }

    pub fn gen_cluster_slots(&self, db_name: DBName) -> Result<RespVec, String> {
        let meta_map = self.meta_map.load();
        let migration_states = meta_map.migration_map.get_states(&db_name);
        meta_map.db_map.gen_cluster_slots(
            db_name,
            self.config.announce_address.clone(),
            &migration_states,
        )
    }

    pub fn get_dbs(&self) -> Vec<DBName> {
        self.meta_map.load().db_map.get_dbs()
    }

    pub fn set_meta(&self, db_meta: ProxyDBMeta) -> Result<(), DBError> {
        let sender_factory = &self.sender_factory;
        let migration_manager = &self.migration_manager;

        let _guard = self.lock.lock().expect("MetaManager::set_meta");

        if db_meta.get_epoch() <= self.epoch.load(Ordering::SeqCst) && !db_meta.get_flags().force {
            return Err(DBError::OldEpoch);
        }

        let old_meta_map = self.meta_map.load();
        let db_map = DatabaseMap::from_db_map(&db_meta, sender_factory);
        let (migration_map, new_tasks) = migration_manager.create_new_migration_map(
            &old_meta_map.migration_map,
            db_meta.get_local(),
            self.blocking_map.clone(),
        );
        let left_slots_after_change = old_meta_map
            .migration_map
            .get_left_slots_after_change(&migration_map, db_meta.get_local());
        let (deleting_task_map, new_deleting_tasks) = migration_manager
            .create_new_deleting_task_map(
                &old_meta_map.deleting_task_map,
                db_meta.get_local(),
                left_slots_after_change,
            );
        self.meta_map.store(Arc::new(MetaMap {
            db_map,
            migration_map,
            deleting_task_map,
        }));
        self.epoch.store(db_meta.get_epoch(), Ordering::SeqCst);

        self.migration_manager.run_tasks(new_tasks);
        self.migration_manager
            .run_deleting_tasks(new_deleting_tasks);
        debug!("Successfully update db meta data");

        Ok(())
    }

    pub fn update_replicators(&self, meta: ReplicatorMeta) -> Result<(), DBError> {
        self.replicator_manager.update_replicators(meta)
    }

    pub fn get_replication_info(&self) -> String {
        self.replicator_manager.get_metadata_report()
    }

    pub fn info(&self) -> String {
        let meta_map = self.meta_map.load();
        let db_info = meta_map.db_map.info();
        let mgr_info = meta_map.migration_map.info();
        let del_info = meta_map.deleting_task_map.info();
        format!(
            "# DB\r\n{}\r\n# Migration\r\n{}\r\n{}\r\n",
            db_info, mgr_info, del_info
        )
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

    pub fn try_select_db(&self, mut cmd_ctx: CmdCtx) -> CmdCtx {
        if cmd_ctx.get_db_name().as_str() != DEFAULT_DB {
            return cmd_ctx;
        }

        if let Some(db_name) = self.meta_map.load().db_map.auto_select_db() {
            cmd_ctx.set_db_name(db_name);
        }
        cmd_ctx
    }
}

pub fn send_cmd_ctx(meta_map: &SharedMetaMap, cmd_ctx: CmdCtx) {
    let meta_map = meta_map.lease();
    let cmd_ctx = match meta_map.migration_map.send(cmd_ctx) {
        Ok(()) => return,
        Err(e) => match e {
            DBSendError::SlotNotFound(cmd_ctx) => cmd_ctx,
            err => {
                error!("migration send task failed: {:?}", err);
                return;
            }
        },
    };

    cmd_ctx.log_event(TaskEvent::SentToDB);
    let res = meta_map.db_map.send(cmd_ctx);
    if let Err(e) = res {
        match e {
            DBSendError::MissingKey => (),
            err => warn!("Failed to forward cmd_ctx: {:?}", err),
        }
    }
}

pub struct BlockingTaskRetrySender {
    meta_map: SharedMetaMap,
}

impl BlockingTaskRetrySender {
    fn new(meta_map: SharedMetaMap) -> Self {
        Self { meta_map }
    }
}

impl CmdTaskSender for BlockingTaskRetrySender {
    type Task = CmdCtx;

    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError> {
        send_cmd_ctx(&self.meta_map, cmd_task);
        Ok(())
    }
}

impl BlockingCmdTaskSender for BlockingTaskRetrySender {}
