use super::backend::{CmdTask, ConnFactory, IntoTask, SenderBackendError};
use super::blocking::{
    gen_basic_blocking_sender_factory, gen_blocking_sender_factory, BasicBlockingSenderFactory,
    BlockingBackendSenderFactory, BlockingCmdTaskSender, BlockingMap, CounterTask,
};
use super::cluster::{ClusterBackendMap, ClusterMetaError, ClusterSendError, ClusterTag};
use super::reply::{DecompressCommitHandlerFactory, ReplyCommitHandlerFactory};
use super::sender::{
    gen_migration_sender_factory, gen_sender_factory, BackendSenderFactory, CmdTaskSender,
    CmdTaskSenderFactory, MigrationBackendSenderFactory,
};
use super::service::ServerProxyConfig;
use super::session::{CmdCtx, CmdCtxFactory};
use super::slowlog::TaskEvent;
use crate::common::cluster::{ClusterName, MigrationTaskMeta, SlotRangeTag};
use crate::common::config::ClusterConfig;
use crate::common::proto::ProxyClusterMeta;
use crate::common::response;
use crate::common::track::TrackedFutureRegistry;
use crate::common::utils::{gen_moved, RetryError};
use crate::migration::manager::{MigrationManager, MigrationMap, SwitchError};
use crate::migration::task::MgrSubCmd;
use crate::migration::task::SwitchArg;
use crate::protocol::{Array, BulkStr, RedisClientFactory, Resp, RespPacket, RespVec};
use crate::replication::manager::ReplicatorManager;
use crate::replication::replicator::ReplicatorMeta;
use arc_swap::{ArcSwap, Lease};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub struct MetaMap<S: CmdTaskSender, P: CmdTaskSender, T>
where
    <S as CmdTaskSender>::Task: ClusterTag,
    <P as CmdTaskSender>::Task: ClusterTag,
    <S as CmdTaskSender>::Task: IntoTask<<P as CmdTaskSender>::Task>,
    T: CmdTask + ClusterTag,
{
    cluster_map: ClusterBackendMap<S, P>,
    migration_map: MigrationMap<T>,
}

impl<S: CmdTaskSender, P: CmdTaskSender, T> MetaMap<S, P, T>
where
    <S as CmdTaskSender>::Task: ClusterTag,
    <P as CmdTaskSender>::Task: ClusterTag,
    <S as CmdTaskSender>::Task: IntoTask<<P as CmdTaskSender>::Task>,
    T: CmdTask + ClusterTag,
{
    pub fn empty() -> Self {
        let cluster_map = ClusterBackendMap::default();
        let migration_map = MigrationMap::empty();
        Self {
            cluster_map,
            migration_map,
        }
    }

    pub fn get_cluster_map(&self) -> &ClusterBackendMap<S, P> {
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

type PeerSenderFactory<C> = BackendSenderFactory<ReplyCommitHandlerFactory, C>;

type MigrationSenderFactory<C> =
    MigrationBackendSenderFactory<DecompressCommitHandlerFactory<CmdCtx, C>, C>;
type MigrationProxySenderFactory<C> = MigrationBackendSenderFactory<ReplyCommitHandlerFactory, C>;

type ProxyMetaMap<C> = MetaMap<
    <SenderFactory<C> as CmdTaskSenderFactory>::Sender,
    <PeerSenderFactory<C> as CmdTaskSenderFactory>::Sender,
    CmdCtx,
>;
pub type SharedMetaMap<C> = Arc<ArcSwap<ProxyMetaMap<C>>>;

pub struct MetaManager<F: RedisClientFactory, C: ConnFactory<Pkt = RespPacket>> {
    config: Arc<ServerProxyConfig>,
    // Now replicator is not in meta_map, if later we need consistency
    // between replication metadata and other metadata, we should put that
    // inside meta_map.
    meta_map: SharedMetaMap<C>,
    epoch: AtomicU64,
    lock: Mutex<()>, // This is the write lock for `epoch`, `cluster`, and `task`.
    replicator_manager: ReplicatorManager<F>,
    migration_manager: MigrationManager<
        F,
        MigrationSenderFactory<C>,
        MigrationProxySenderFactory<C>,
        CmdCtxFactory,
    >,
    sender_factory: SenderFactory<C>,
    peer_sender_factory: PeerSenderFactory<C>,
    blocking_map: Arc<BlockingMap<BasicSenderFactory<C>, BlockingTaskRetrySender<C>>>,
    cluster_config: ClusterConfig,
}

impl<F: RedisClientFactory, C: ConnFactory<Pkt = RespPacket>> MetaManager<F, C> {
    pub fn new(
        config: Arc<ServerProxyConfig>,
        cluster_config: ClusterConfig,
        client_factory: Arc<F>,
        conn_factory: Arc<C>,
        meta_map: SharedMetaMap<C>,
        future_registry: Arc<TrackedFutureRegistry>,
    ) -> Self {
        let reply_handler_factory = Arc::new(DecompressCommitHandlerFactory::new(meta_map.clone()));
        let blocking_task_sender = Arc::new(BlockingTaskRetrySender::new(
            meta_map.clone(),
            config.max_redirections,
            config.default_redirection_address.clone(),
        ));
        let basic_sender_factory = gen_basic_blocking_sender_factory(
            config.clone(),
            reply_handler_factory,
            conn_factory.clone(),
            future_registry.clone(),
        );
        let blocking_map = Arc::new(BlockingMap::new(basic_sender_factory, blocking_task_sender));
        let sender_factory = gen_blocking_sender_factory(blocking_map.clone());
        let reply_commit_handler_factory = Arc::new(ReplyCommitHandlerFactory::default());
        let peer_sender_factory = gen_sender_factory(
            config.clone(),
            reply_commit_handler_factory,
            conn_factory.clone(),
            future_registry.clone(),
        );
        let migration_sender_factory = Arc::new(gen_migration_sender_factory(
            config.clone(),
            Arc::new(DecompressCommitHandlerFactory::new(meta_map.clone())),
            conn_factory.clone(),
            future_registry.clone(),
        ));
        let migration_proxy_sender_factory = Arc::new(gen_migration_sender_factory(
            config.clone(),
            Arc::new(ReplyCommitHandlerFactory::default()),
            conn_factory,
            future_registry.clone(),
        ));
        let cmd_ctx_factory = Arc::new(CmdCtxFactory::default());
        let config_clone = config.clone();
        let cluster_config_clone = cluster_config.clone();
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
                cluster_config_clone,
                client_factory,
                migration_sender_factory,
                migration_proxy_sender_factory,
                cmd_ctx_factory,
                future_registry,
            ),
            sender_factory,
            peer_sender_factory,
            blocking_map,
            cluster_config,
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
        let peer_sender_factory = &self.peer_sender_factory;
        let migration_manager = &self.migration_manager;
        let cluster_config = &self.cluster_config;

        {
            let _guard = self.lock.lock().expect("MetaManager::set_meta");

            if cluster_meta.get_epoch() <= self.epoch.load(Ordering::SeqCst)
                && !cluster_meta.get_flags().force
            {
                return Err(ClusterMetaError::OldEpoch);
            }

            let old_meta_map = self.meta_map.load();
            let cluster_map = ClusterBackendMap::from_cluster_map(
                &cluster_meta,
                sender_factory,
                peer_sender_factory,
                active_redirection,
                cluster_config,
            );
            let (migration_map, new_tasks) = migration_manager.create_new_migration_map(
                &old_meta_map.migration_map,
                cluster_meta.get_local(),
                cluster_meta.get_configs(),
                self.blocking_map.clone(),
            );

            self.meta_map.store(Arc::new(MetaMap {
                cluster_map,
                migration_map,
            }));
            // Should go after the meta_map.store above
            self.epoch.store(cluster_meta.get_epoch(), Ordering::SeqCst);

            self.migration_manager.run_tasks(new_tasks);
        };

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
        let repl_info = self.replicator_manager.get_metadata_report();
        Resp::Arr(Array::Arr(vec![
            Resp::Bulk(BulkStr::Str(b"Cluster".to_vec())),
            cluster_info,
            Resp::Bulk(BulkStr::Str(b"Replication".to_vec())),
            repl_info,
            Resp::Bulk(BulkStr::Str(b"Migration".to_vec())),
            mgr_info,
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
        let max_redirections = self.config.max_redirections;
        let default_redirection_address = self.config.default_redirection_address.as_ref();
        loop_send_cmd_ctx(
            &self.meta_map,
            cmd_ctx,
            max_redirections,
            default_redirection_address,
        );
    }

    pub fn send_sync_task(&self, cmd_ctx: CmdCtx) {
        let meta_map = self.meta_map.load();
        if let Err(err) = meta_map.migration_map.send_sync_task(cmd_ctx) {
            match err {
                ClusterSendError::SlotNotFound(task) => {
                    // When the migrating task is closed,
                    // the destination proxy might keep sending UMSYNC to source proxy.
                    // We need to tag this case.
                    task.set_resp_result(Ok(Resp::Error(
                        response::MIGRATION_TASK_NOT_FOUND.to_string().into_bytes(),
                    )));
                }
                ClusterSendError::ActiveRedirection { task, .. } => {
                    task.set_resp_result(Ok(Resp::Error(
                        b"unexpected active redirection".to_vec(),
                    )));
                }
                ClusterSendError::Retry(task) => {
                    task.set_resp_result(Ok(Resp::Error(
                        b"unexpected retry error on sync task".to_vec(),
                    )));
                }
                other_err => {
                    error!("Failed to process sync task {:?}", other_err);
                }
            }
        }
    }

    pub fn try_select_cluster(&self, mut cmd_ctx: CmdCtx) -> CmdCtx {
        let exists = self
            .meta_map
            .lease()
            .cluster_map
            .cluster_exists(cmd_ctx.get_cluster_name());
        if exists {
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

    pub fn is_ready(&self, cluster_name: ClusterName) -> bool {
        // This is used to determined whether this proxy could be
        // added or removed from the Service of kubernetes.
        //
        // The proxy is ready when it contains stable slots.
        //
        // When scaling out, the proxy will not be added to the service
        // when slots start migrating to it.
        //
        // When scaling down, the proxy is turned into not ready
        // once migration started, which could leave the time for
        // the service to remove the proxy.
        let meta_map = self.meta_map.load();
        meta_map.cluster_map.is_ready(cluster_name)
    }
}

pub fn loop_send_cmd_ctx<C: ConnFactory<Pkt = RespPacket>>(
    meta_map: &SharedMetaMap<C>,
    cmd_ctx: CmdCtx,
    max_redirections: Option<NonZeroUsize>,
    default_redirection_address: Option<&String>,
) {
    let mut cmd_ctx = cmd_ctx;
    const MAX_RETRY_NUM: usize = 10;
    for i in 0..MAX_RETRY_NUM {
        cmd_ctx = match send_cmd_ctx(
            meta_map,
            cmd_ctx,
            max_redirections,
            default_redirection_address,
        ) {
            Ok(()) => return,
            Err(retry_err) => retry_err.into_inner(),
        };
        if i + 1 == MAX_RETRY_NUM {
            let resp = Resp::Error(b"cmd exceeds retry limit".to_vec());
            cmd_ctx.set_resp_result(Ok(resp));
            return;
        }
        info!("retry send cmd_ctx");
    }
}

pub fn send_cmd_ctx<C: ConnFactory<Pkt = RespPacket>>(
    meta_map: &SharedMetaMap<C>,
    cmd_ctx: CmdCtx,
    max_redirections: Option<NonZeroUsize>,
    default_redirection_address: Option<&String>,
) -> Result<(), RetryError<CmdCtx>> {
    let meta_map = meta_map.lease();
    let mut cmd_ctx = match meta_map.migration_map.send(cmd_ctx) {
        Ok(()) => return Ok(()),
        Err(e) => match e {
            ClusterSendError::SlotNotFound(cmd_ctx) => cmd_ctx,
            ClusterSendError::ActiveRedirection {
                task,
                slot,
                address,
            } => {
                let cmd_ctx = task.into_task();
                send_cmd_ctx_to_remote_directly(
                    &meta_map,
                    cmd_ctx,
                    slot,
                    address,
                    max_redirections,
                );
                return Ok(());
            }
            ClusterSendError::Retry(cmd_ctx) => return Err(RetryError::new(cmd_ctx.into_inner())),
            err => {
                error!("migration send task failed: {:?}", err);
                return Ok(());
            }
        },
    };

    cmd_ctx.log_event(TaskEvent::SentToCluster);
    let res = meta_map.cluster_map.send(cmd_ctx);

    if let Err(e) = res {
        match e {
            ClusterSendError::MissingKey => (),
            ClusterSendError::ClusterNotFound { task, cluster_name } => {
                match default_redirection_address {
                    Some(redirection_address) => match task.get_slot() {
                        None => {
                            let resp = Resp::Error("missing key".to_string().into_bytes());
                            task.set_resp_result(Ok(resp));
                        }
                        Some(slot) => {
                            let resp = Resp::Error(
                                gen_moved(slot, redirection_address.clone()).into_bytes(),
                            );
                            task.set_resp_result(Ok(resp));
                        }
                    },
                    None => {
                        let resp = Resp::Error(
                            format!("{}: {}", response::ERR_CLUSTER_NOT_FOUND, cluster_name)
                                .into_bytes(),
                        );
                        task.set_resp_result(Ok(resp));
                    }
                }
            }
            ClusterSendError::ActiveRedirection {
                task,
                slot,
                address,
            } => {
                let cmd_ctx = task.into_task();
                send_cmd_ctx_to_remote_directly(
                    &meta_map,
                    cmd_ctx,
                    slot,
                    address,
                    max_redirections,
                );
                return Ok(());
            }
            ClusterSendError::Retry(cmd_ctx) => {
                return Err(RetryError::new(cmd_ctx.into_inner()));
            }
            err => warn!("Failed to forward cmd_ctx: {:?}", err),
        }
    }

    Ok(())
}

fn send_cmd_ctx_to_remote_directly<C: ConnFactory<Pkt = RespPacket>>(
    meta_map: &Lease<Arc<ProxyMetaMap<C>>>,
    mut cmd_ctx: CmdCtx,
    slot: usize,
    address: String,
    max_redirections: Option<NonZeroUsize>,
) {
    let times = cmd_ctx
        .get_redirection_times()
        .or_else(|| max_redirections.map(|n| n.get() - 1));
    if let Some(times) = times {
        let times = match times.checked_sub(1) {
            None => {
                cmd_ctx.set_resp_result(Ok(Resp::Error(
                    response::ERR_TOO_MANY_REDIRECTIONS.to_string().into_bytes(),
                )));
                return;
            }
            Some(times) => times,
        };

        let res = cmd_ctx.wrap_cmd(vec![b"UMFORWARD".to_vec(), times.to_string().into_bytes()]);
        if !res {
            cmd_ctx.set_resp_result(Ok(Resp::Error(
                b"failed to wrap command for redirections".to_vec(),
            )));
            return;
        }
    }

    let res = meta_map
        .cluster_map
        .send_remote_directly(cmd_ctx, slot, address);
    if let Err(e) = res {
        match e {
            ClusterSendError::MissingKey => (),
            err => warn!("Failed to forward cmd_ctx to remote: {:?}", err),
        }
    }
}

pub struct BlockingTaskRetrySender<C: ConnFactory<Pkt = RespPacket>> {
    meta_map: SharedMetaMap<C>,
    max_redirections: Option<NonZeroUsize>,
    default_redirection_address: Option<String>,
}

impl<C: ConnFactory<Pkt = RespPacket>> BlockingTaskRetrySender<C> {
    fn new(
        meta_map: SharedMetaMap<C>,
        max_redirections: Option<NonZeroUsize>,
        default_redirection_address: Option<String>,
    ) -> Self {
        Self {
            meta_map,
            max_redirections,
            default_redirection_address,
        }
    }
}

impl<C: ConnFactory<Pkt = RespPacket>> CmdTaskSender for BlockingTaskRetrySender<C> {
    type Task = CmdCtx;

    fn send(&self, cmd_task: Self::Task) -> Result<(), SenderBackendError<Self::Task>> {
        loop_send_cmd_ctx(
            &self.meta_map,
            cmd_task,
            self.max_redirections,
            self.default_redirection_address.as_ref(),
        );
        Ok(())
    }
}

impl<C: ConnFactory<Pkt = RespPacket>> BlockingCmdTaskSender for BlockingTaskRetrySender<C> {}
