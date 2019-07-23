use super::backend::{
    CachedSenderFactory, RRSenderGroupFactory, RecoverableBackendNodeFactory,
    RedirectionSenderFactory,
};
use super::database::{DBError, DBSendError, DatabaseMap};
use super::service::ServerProxyConfig;
use super::session::CmdCtx;
use super::slowlog::TaskEvent;
use ::common::cluster::MigrationTaskMeta;
use ::migration::manager::{MigrationManager, SwitchError};
use ::migration::task::{MigrationConfig, SwitchArg};
use common::db::{ProxyDBMeta};
use protocol::{RedisClientFactory, Resp};
use proxy::backend::CmdTask;
use proxy::database::{DBTag, DEFAULT_DB};
use replication::manager::ReplicatorManager;
use replication::replicator::ReplicatorMeta;
use arc_swap::ArcSwap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub struct MetaManager<F: RedisClientFactory> {
    config: Arc<ServerProxyConfig>,
    db: ArcSwap<DatabaseMap<
        CachedSenderFactory<RRSenderGroupFactory<RecoverableBackendNodeFactory<CmdCtx>>>,
    >>,
    epoch: AtomicU64,
    lock: Mutex<()>,  // This is the write lock for `epoch`, `db`, `replicator` and `task`.
    replicator_manager: ReplicatorManager<F>,
    migration_manager: MigrationManager<F, RedirectionSenderFactory<CmdCtx>>,
    sender_factory: CachedSenderFactory<RRSenderGroupFactory<RecoverableBackendNodeFactory<CmdCtx>>>,
}

impl<F: RedisClientFactory> MetaManager<F> {
    pub fn new(config: Arc<ServerProxyConfig>, client_factory: Arc<F>) -> Self {
        let sender_factory = CachedSenderFactory::new(RRSenderGroupFactory::new(
            config.backend_conn_num,
            RecoverableBackendNodeFactory::new(config.clone()),
        ));
        let db = DatabaseMap::new();
        let redirection_sender_factory = Arc::new(RedirectionSenderFactory::default());
        let migration_config = Arc::new(MigrationConfig::default());
        Self {
            config,
            db: ArcSwap::new(Arc::new(db)),
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
        self.db.load()
            .gen_cluster_nodes(db_name, self.config.announce_address.clone())
    }

    pub fn gen_cluster_slots(&self, db_name: String) -> Result<Resp, String> {
        self.db.load()
            .gen_cluster_slots(db_name, self.config.announce_address.clone())
    }

    pub fn get_dbs(&self) -> Vec<String> {
        self.db.load().get_dbs()
    }

    pub fn set_db(&self, db_meta: ProxyDBMeta) -> Result<(), DBError> {
        let sender_factory = &self.sender_factory;
        let _guard = self.lock.lock().unwrap();

        if db_meta.get_epoch() <= self.epoch.load(Ordering::SeqCst) {
            return Err(DBError::OldEpoch)
        }

        // TODO: later we need to use it to update migration and replication.
        let _old_db = self.db.load();
        let db_map = DatabaseMap::from_db_map(&db_meta, sender_factory);
        self.db.store(Arc::new(db_map));

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
        self.migration_manager.commit_importing(switch_arg)
    }

    pub fn get_finished_migration_tasks(&self) -> Vec<MigrationTaskMeta> {
        self.migration_manager.get_finished_tasks()
    }

    pub fn send(&self, cmd_ctx: CmdCtx) {
        cmd_ctx
            .get_slowlog()
            .log_event(TaskEvent::SentToMigrationManager);
        let cmd_ctx = match self.migration_manager.send(cmd_ctx) {
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
        let res = self.db.load().send(cmd_ctx);
        if let Err(e) = res {
            warn!("Failed to forward cmd_ctx: {:?}", e)
        }
    }

    pub fn try_select_db(&self, cmd_ctx: CmdCtx) -> CmdCtx {
        if cmd_ctx.get_db_name() != DEFAULT_DB {
            return cmd_ctx;
        }

        if let Some(db_name) = self.db.load().auto_select_db() {
            cmd_ctx.set_db_name(db_name);
        }
        cmd_ctx
    }
}
