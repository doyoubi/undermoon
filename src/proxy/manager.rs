use super::backend::{
    CachedSenderFactory, CmdTaskSender, CmdTaskSenderFactory, RRSenderGroupFactory,
    RecoverableBackendNodeFactory, RedirectionSenderFactory,
};
use super::database::{DBError, DBSendError, DatabaseMap};
use super::service::ServerProxyConfig;
use super::session::CmdCtx;
use super::slowlog::TaskEvent;
use ::common::cluster::{MigrationTaskMeta, SlotRangeTag};
use ::migration::manager::{MigrationManager, MigrationMap, SwitchError};
use ::migration::task::{MigrationConfig, SwitchArg};
use arc_swap::ArcSwap;
use common::db::ProxyDBMeta;
use protocol::{RedisClientFactory, Resp};
use proxy::backend::CmdTask;
use proxy::database::{DBTag, DEFAULT_DB};
use replication::manager::ReplicatorManager;
use replication::replicator::ReplicatorMeta;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

struct MetaMap<F: CmdTaskSenderFactory>
where
    <<F as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task: DBTag,
{
    db_map: DatabaseMap<F>,
    migration_map: MigrationMap<RedirectionSenderFactory<CmdCtx>>,
}

pub struct MetaManager<F: RedisClientFactory> {
    config: Arc<ServerProxyConfig>,
    meta_map: ArcSwap<
        MetaMap<CachedSenderFactory<RRSenderGroupFactory<RecoverableBackendNodeFactory<CmdCtx>>>>,
    >,
    epoch: AtomicU64,
    lock: Mutex<()>, // This is the write lock for `epoch`, `db`, `replicator` and `task`.
    replicator_manager: ReplicatorManager<F>,
    migration_manager: MigrationManager<F, RedirectionSenderFactory<CmdCtx>>,
    sender_factory:
        CachedSenderFactory<RRSenderGroupFactory<RecoverableBackendNodeFactory<CmdCtx>>>,
}

impl<F: RedisClientFactory> MetaManager<F> {
    pub fn new(config: Arc<ServerProxyConfig>, client_factory: Arc<F>) -> Self {
        let sender_factory = CachedSenderFactory::new(RRSenderGroupFactory::new(
            config.backend_conn_num,
            RecoverableBackendNodeFactory::new(config.clone()),
        ));
        let db_map = DatabaseMap::default();
        let migration_map = MigrationMap::new();
        let meta_map = MetaMap {
            db_map,
            migration_map,
        };
        let redirection_sender_factory = Arc::new(RedirectionSenderFactory::default());
        let migration_config = Arc::new(MigrationConfig::default());
        Self {
            config,
            meta_map: ArcSwap::new(Arc::new(meta_map)),
            epoch: AtomicU64::new(0),
            lock: Mutex::new(()),
            replicator_manager: ReplicatorManager::new(client_factory.clone()),
            migration_manager: MigrationManager::new(
                migration_config,
                client_factory,
                redirection_sender_factory,
            ),
            sender_factory,
        }
    }

    pub fn gen_cluster_nodes(&self, db_name: String) -> String {
        self.meta_map
            .load()
            .db_map
            .gen_cluster_nodes(db_name, self.config.announce_address.clone())
    }

    pub fn gen_cluster_slots(&self, db_name: String) -> Result<Resp, String> {
        self.meta_map
            .load()
            .db_map
            .gen_cluster_slots(db_name, self.config.announce_address.clone())
    }

    pub fn get_dbs(&self) -> Vec<String> {
        self.meta_map.load().db_map.get_dbs()
    }

    pub fn set_meta(&self, db_meta: ProxyDBMeta) -> Result<(), DBError> {
        let sender_factory = &self.sender_factory;
        let migration_manager = &self.migration_manager;

        let _guard = self.lock.lock().unwrap();

        if db_meta.get_epoch() <= self.epoch.load(Ordering::SeqCst) && !db_meta.get_flags().force {
            return Err(DBError::OldEpoch);
        }

        // TODO: later we need to use it to update migration and replication.
        let old_meta_map = self.meta_map.load();
        let db_map = DatabaseMap::from_db_map(&db_meta, sender_factory);
        let (migration_map, new_tasks) = migration_manager
            .create_new_migration_map(&old_meta_map.migration_map, db_meta.get_local());
        self.meta_map.store(Arc::new(MetaMap {
            db_map,
            migration_map,
        }));
        self.epoch.store(db_meta.get_epoch(), Ordering::SeqCst);

        self.migration_manager.run_tasks(new_tasks);

        Ok(())

        // Put db meta and migration meta together for consistency.
        // We can make sure that IMPORTING slots will not be handled directly
        // before the migration succeed. This is also why we should store the
        // new metadata to `migration_manager` first.
        //        match self.migration_manager.update(db_meta.get_local()) {
        //            Ok(()) => {
        //                debug!("Successfully update migration meta data");
        //                debug!("local meta data: {:?}", db_map);
        //                let epoch = db_map.get_epoch();
        //                match self.db.set_dbs(db_meta) {
        //                    Ok(()) => {
        ////                        self.migration_manager.clear_tmp_dbs(epoch);
        //                        self.migration_manager.update_local_meta_epoch(epoch);
        //                        Ok(())
        //                    }
        //                    err => err,
        //                }
        //            }
        //            err => err,
        //        }
    }

    pub fn update_replicators(&self, meta: ReplicatorMeta) -> Result<(), DBError> {
        self.replicator_manager.update_replicators(meta)
    }

    pub fn get_replication_info(&self) -> String {
        self.replicator_manager.get_metadata_report()
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

        if self.epoch.load(Ordering::SeqCst) < arg_epoch {
            return Err(SwitchError::NotReady);
        }

        self.meta_map
            .load()
            .migration_map
            .commit_importing(switch_arg)
    }

    pub fn get_finished_migration_tasks(&self) -> Vec<MigrationTaskMeta> {
        self.meta_map.load().migration_map.get_finished_tasks()
    }

    pub fn send(&self, cmd_ctx: CmdCtx) {
        cmd_ctx
            .get_slowlog()
            .log_event(TaskEvent::SentToMigrationManager);
        let cmd_ctx = match self.meta_map.load().migration_map.send(cmd_ctx) {
            Ok(()) => return,
            Err(e) => match e {
                DBSendError::SlotNotFound(cmd_ctx) => cmd_ctx,
                err => {
                    error!("migration send task failed: {:?}", err);
                    return;
                }
            },
        };

        cmd_ctx.get_slowlog().log_event(TaskEvent::SentToDB);
        let res = self.meta_map.load().db_map.send(cmd_ctx);
        if let Err(e) = res {
            warn!("Failed to forward cmd_ctx: {:?}", e)
        }
    }

    pub fn try_select_db(&self, cmd_ctx: CmdCtx) -> CmdCtx {
        if cmd_ctx.get_db_name() != DEFAULT_DB {
            return cmd_ctx;
        }

        if let Some(db_name) = self.meta_map.load().db_map.auto_select_db() {
            cmd_ctx.set_db_name(db_name);
        }
        cmd_ctx
    }
}
