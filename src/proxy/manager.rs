use super::backend::{
    CachedSenderFactory, DirectionSenderFactory, RRSenderGroupFactory,
    RecoverableBackendNodeFactory,
};
use super::database::{DBError, DBSendError, DatabaseMap};
use super::session::CmdCtx;
use ::common::cluster::MigrationTaskMeta;
use ::migration::manager::{MigrationManager, SwitchError};
use ::migration::task::{MigrationConfig, SwitchArg};
use common::db::HostDBMap;
use protocol::{RedisClientFactory, Resp};
use replication::manager::ReplicatorManager;
use replication::replicator::ReplicatorMeta;
use std::sync::Arc;

pub struct MetaManager<F: RedisClientFactory> {
    service_address: String,
    db: DatabaseMap<
        CachedSenderFactory<RRSenderGroupFactory<RecoverableBackendNodeFactory<CmdCtx>>>,
    >,
    replicator_manager: ReplicatorManager<F>,
    migration_manager: MigrationManager<F, DirectionSenderFactory<CmdCtx>>,
}

impl<F: RedisClientFactory> MetaManager<F> {
    pub fn new(service_address: String, client_factory: Arc<F>) -> Self {
        let sender_factory = CachedSenderFactory::new(RRSenderGroupFactory::new(
            RecoverableBackendNodeFactory::default(),
        ));
        let db = DatabaseMap::new(sender_factory);
        let redirection_sender_factory = Arc::new(DirectionSenderFactory::default());
        let migration_config = Arc::new(MigrationConfig::default());
        Self {
            service_address,
            db,
            replicator_manager: ReplicatorManager::new(client_factory.clone()),
            migration_manager: MigrationManager::new(
                migration_config,
                client_factory,
                redirection_sender_factory,
            ),
        }
    }

    pub fn gen_cluster_nodes(&self, db_name: String) -> String {
        self.db
            .gen_cluster_nodes(db_name, self.service_address.clone())
    }

    pub fn gen_cluster_slots(&self, db_name: String) -> Result<Resp, String> {
        self.db
            .gen_cluster_slots(db_name, self.service_address.clone())
    }

    pub fn get_dbs(&self) -> Vec<String> {
        self.db.get_dbs()
    }

    pub fn clear_db(&self) {
        self.db.clear()
    }

    pub fn set_db(&self, db_map: HostDBMap) -> Result<(), DBError> {
        let db_map_clone = db_map.clone();

        // Put db meta and migration meta together for consistency.
        // We can make sure that IMPORTING slots will not be handled directly
        // before the migration succeed. This is also why we should store the
        // new metadata to `migration_manager` first.
        match self.migration_manager.update(db_map_clone) {
            Ok(()) => {
                debug!("Successfully update migration meta data");
                debug!("local meta data: {:?}", db_map);
                self.db.set_dbs(db_map)
            }
            err => err,
        }
    }

    pub fn set_peers(&self, db_map: HostDBMap) -> Result<(), DBError> {
        self.db.set_peers(db_map)
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
        let res = self.db.send(cmd_ctx);
        if let Err(e) = res {
            error!("Failed to foward cmd_ctx: {:?}", e)
        }
    }
}
