use super::backend::{CmdTask, ConnFactory, IntoTask, SenderBackendError};
use super::blocking::{
    gen_basic_blocking_sender_factory, gen_blocking_sender_factory, BasicBlockingSenderFactory,
    BlockingBackendSenderFactory, BlockingCmdTaskSender, BlockingMap, CounterTask,
};
use super::cluster::{ClusterBackendMap, ClusterMetaError, ClusterSendError};
use super::reply::{DecompressCommitHandlerFactory, ReplyCommitHandlerFactory};
use super::sender::{
    gen_migration_sender_factory, gen_sender_factory, BackendSenderFactory, CmdTaskSender,
    CmdTaskSenderFactory, MigrationBackendSenderFactory,
};
use super::service::ServerProxyConfig;
use super::session::{CmdCtx, CmdCtxFactory};
use super::slowlog::TaskEvent;
use crate::common::batch::BatchStats;
use crate::common::cluster::{ClusterName, MigrationTaskMeta, SlotRangeTag};
use crate::common::proto::{NodeMap, ProxyClusterMeta};
use crate::common::response;
use crate::common::track::TrackedFutureRegistry;
use crate::common::utils::{gen_moved, RetryError};
use crate::migration::manager::{MigrationManager, MigrationMap, SwitchError};
use crate::migration::task::MgrSubCmd;
use crate::migration::task::SwitchArg;
use crate::protocol::{
    Array, BinSafeStr, BulkStr, RedisClient, RedisClientFactory, Resp, RespPacket, RespVec,
};
use crate::proxy::backend::CmdTaskFactory;
use crate::proxy::migration_backend::WaitableTask;
use crate::replication::manager::ReplicatorManager;
use crate::replication::replicator::ReplicatorMeta;
use arc_swap::ArcSwap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct MetaMap<S: CmdTaskSender, P: CmdTaskSender, T>
where
    <S as CmdTaskSender>::Task: IntoTask<<P as CmdTaskSender>::Task>,
    T: CmdTask,
{
    cluster_map: ClusterBackendMap<S, P>,
    migration_map: MigrationMap<T>,
}

impl<S: CmdTaskSender, P: CmdTaskSender, T> MetaMap<S, P, T>
where
    <S as CmdTaskSender>::Task: IntoTask<<P as CmdTaskSender>::Task>,
    T: CmdTask,
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
type MigrationDstSenderFactory<C> =
    MigrationBackendSenderFactory<DecompressCommitHandlerFactory<WaitableTask<CmdCtx>, C>, C>;
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
    lock: parking_lot::Mutex<()>, // This is the write lock for `epoch`, `cluster`, and `task`.
    replicator_manager: ReplicatorManager<F>,
    migration_manager: MigrationManager<
        F,
        MigrationSenderFactory<C>,
        MigrationDstSenderFactory<C>,
        MigrationProxySenderFactory<C>,
        CmdCtxFactory,
    >,
    sender_factory: SenderFactory<C>,
    peer_sender_factory: PeerSenderFactory<C>,
    blocking_map: Arc<BlockingMap<BasicSenderFactory<C>, BlockingTaskRetrySender<C>>>,
    client_factory: Arc<F>,
    batch_stats: Arc<BatchStats>,
}

impl<F: RedisClientFactory, C: ConnFactory<Pkt = RespPacket>> MetaManager<F, C> {
    pub fn new(
        config: Arc<ServerProxyConfig>,
        client_factory: Arc<F>,
        conn_factory: Arc<C>,
        meta_map: SharedMetaMap<C>,
        future_registry: Arc<TrackedFutureRegistry>,
    ) -> Self {
        let batch_stats = Arc::new(BatchStats::default());
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
            batch_stats.clone(),
        );
        let blocking_map = Arc::new(BlockingMap::new(basic_sender_factory, blocking_task_sender));
        let sender_factory = gen_blocking_sender_factory(blocking_map.clone());
        let reply_commit_handler_factory = Arc::new(ReplyCommitHandlerFactory::default());
        let peer_sender_factory = gen_sender_factory(
            config.clone(),
            reply_commit_handler_factory,
            conn_factory.clone(),
            future_registry.clone(),
            batch_stats.clone(),
        );
        let migration_sender_factory = Arc::new(gen_migration_sender_factory(
            config.clone(),
            Arc::new(DecompressCommitHandlerFactory::new(meta_map.clone())),
            conn_factory.clone(),
            future_registry.clone(),
            batch_stats.clone(),
        ));
        let migration_dst_sender_factory = Arc::new(gen_migration_sender_factory(
            config.clone(),
            Arc::new(DecompressCommitHandlerFactory::new(meta_map.clone())),
            conn_factory.clone(),
            future_registry.clone(),
            batch_stats.clone(),
        ));
        let migration_proxy_sender_factory = Arc::new(gen_migration_sender_factory(
            config.clone(),
            Arc::new(ReplyCommitHandlerFactory::default()),
            conn_factory,
            future_registry.clone(),
            batch_stats.clone(),
        ));
        let cmd_ctx_factory = Arc::new(CmdCtxFactory::default());
        let config_clone = config.clone();
        Self {
            config,
            meta_map,
            epoch: AtomicU64::new(0),
            lock: parking_lot::Mutex::new(()),
            replicator_manager: ReplicatorManager::new(
                client_factory.clone(),
                future_registry.clone(),
            ),
            migration_manager: MigrationManager::new(
                config_clone,
                client_factory.clone(),
                migration_sender_factory,
                migration_dst_sender_factory,
                migration_proxy_sender_factory,
                cmd_ctx_factory,
                future_registry,
            ),
            sender_factory,
            peer_sender_factory,
            blocking_map,
            client_factory,
            batch_stats,
        }
    }

    pub fn gen_cluster_nodes(&self) -> String {
        let cluster_nodes_version = self.config.command_cluster_nodes_version;
        let meta_map = self.meta_map.load();
        let migration_states = meta_map.migration_map.get_states();
        meta_map.cluster_map.gen_cluster_nodes(
            self.config.announce_address.clone(),
            &migration_states,
            cluster_nodes_version,
        )
    }

    pub fn gen_cluster_slots(&self) -> Result<RespVec, String> {
        let meta_map = self.meta_map.load();
        let migration_states = meta_map.migration_map.get_states();
        meta_map
            .cluster_map
            .gen_cluster_slots(self.config.announce_address.clone(), &migration_states)
    }

    pub fn get_cluster(&self) -> ClusterName {
        self.meta_map.load().cluster_map.get_cluster()
    }

    pub fn set_meta(&self, cluster_meta: ProxyClusterMeta) -> Result<(), ClusterMetaError> {
        let cluster_name = cluster_meta.get_cluster_name();

        // validation
        let local = NodeMap::new(cluster_meta.get_local().clone());
        if !local.check_hosts(self.config.announce_host.as_str(), cluster_name) {
            return Err(ClusterMetaError::NotMyMeta);
        }

        let active_redirection = self.config.active_redirection;

        let sender_factory = &self.sender_factory;
        let peer_sender_factory = &self.peer_sender_factory;
        let migration_manager = &self.migration_manager;

        {
            let _guard = self.lock.lock();

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
            );
            let (migration_map, new_tasks) = migration_manager.create_new_migration_map(
                cluster_name.clone(),
                &old_meta_map.migration_map,
                cluster_meta.get_local(),
                cluster_meta.get_config(),
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
        self.replicator_manager
            .update_replicators(meta, self.config.announce_host.clone())
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

    pub fn get_stats(&self) -> Vec<String> {
        let mut lines = vec!["# Migration".to_string()];
        for (k, v) in self.migration_manager.get_stats().into_iter() {
            lines.push(format!("{}: {}", k, v));
        }
        lines
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

    pub async fn send_to_any_local_node(&self, cmd_ctx: &CmdCtx) -> RespVec {
        let address = match self.meta_map.load().cluster_map.get_cluster_any_node() {
            None => {
                return Resp::Error(response::ERR_CLUSTER_NOT_FOUND.to_string().into_bytes());
            }
            Some(address) => address,
        };

        let mut client = match self.client_factory.create_client(address).await {
            Err(err) => {
                return Resp::Error(err.to_string().into_bytes());
            }
            Ok(client) => client,
        };

        let cmd = match cmd_ctx.get_cmd().to_safe_str_vec() {
            None => {
                return Resp::Error(b"send_with_temp_conn: invalid command".to_vec());
            }
            Some(cmd) => cmd,
        };
        match client.execute_single(cmd).await {
            Err(err) => Resp::Error(err.to_string().into_bytes()),
            Ok(res) => res,
        }
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

    pub async fn send_sync_task(&self, cmd_ctx: CmdCtx) {
        let meta_map = self.meta_map.load();
        if let Err(err) = meta_map.migration_map.send_sync_task(cmd_ctx).await {
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

    pub async fn ensure_keys_imported(
        &self,
        cmd_ctx: &CmdCtx,
        keys: &[BinSafeStr],
    ) -> Result<(), RespVec> {
        // This check and do will not result in race condition,
        // because in "not imported" state,
        // the keys are not owned by this proxy and will be "MOVED".
        if !self.meta_map.load().migration_map.keys_are_importing(keys) {
            return Ok(());
        }

        // Send EXISTS command to trigger key migration.
        let mut futs = vec![];
        let factory = CmdCtxFactory::default();

        for key in keys.iter() {
            let resp = Resp::Arr(Array::Arr(vec![
                Resp::Bulk(BulkStr::Str("EXISTS".to_string().into_bytes())),
                Resp::Bulk(BulkStr::Str(key.clone())),
            ]));
            let (sub_cmd_ctx, reply_fut) = factory.create_with_ctx(cmd_ctx.get_context(), resp);
            self.send(sub_cmd_ctx);
            futs.push(reply_fut);
        }

        let res = futures::future::join_all(futs).await;
        for sub_result in res.into_iter() {
            let reply = match sub_result {
                Ok(reply) => reply,
                Err(err) => {
                    return Err(Resp::Error(format!("ERR: {}", err).into_bytes()));
                }
            };
            // This will also handle the MOVED case.
            if matches!(reply, Resp::Error(_)) {
                return Err(reply);
            }
        }

        Ok(())
    }

    pub fn get_epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    pub fn is_ready(&self) -> bool {
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

        // If there's only replicas, this proxy don't own any slot
        // but should be in ready state.
        let (master_num, replica_num) = self.replicator_manager.get_role_num();
        if master_num == 0 && replica_num > 0 {
            return true;
        }
        let meta_map = self.meta_map.load();
        meta_map.cluster_map.is_ready()
    }

    pub fn get_batch_stats(&self) -> &Arc<BatchStats> {
        &self.batch_stats
    }
}

pub fn loop_send_cmd_ctx<C: ConnFactory<Pkt = RespPacket>>(
    meta_map: &SharedMetaMap<C>,
    cmd_ctx: CmdCtx,
    max_redirections: Option<NonZeroUsize>,
    default_redirection_address: Option<&String>,
) {
    let mut cmd_ctx = cmd_ctx;
    const MAX_RETRY_NUM: usize = 3;
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
    let meta_map = meta_map.load();
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
            ClusterSendError::ClusterNotFound { task } => match default_redirection_address {
                Some(redirection_address) => match task.get_slot() {
                    None => {
                        let resp = Resp::Error("missing key".to_string().into_bytes());
                        task.set_resp_result(Ok(resp));
                    }
                    Some(slot) => {
                        let resp =
                            Resp::Error(gen_moved(slot, redirection_address.clone()).into_bytes());
                        task.set_resp_result(Ok(resp));
                    }
                },
                None => {
                    let resp =
                        Resp::Error(response::ERR_CLUSTER_NOT_FOUND.to_string().into_bytes());
                    task.set_resp_result(Ok(resp));
                }
            },
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
    meta_map: &arc_swap::Guard<Arc<ProxyMetaMap<C>>>,
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
