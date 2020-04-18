use super::scan_migration::ScanMigrationTask;
use super::task::{
    AtomicMigrationState, ImportingTask, MgrSubCmd, MigratingTask, MigrationError, MigrationState,
    SwitchArg,
};
use crate::common::cluster::{
    ClusterName, MigrationMeta, MigrationTaskMeta, RangeMap, SlotRange, SlotRangeTag,
};
use crate::common::config::AtomicMigrationConfig;
use crate::common::resp_execution::keep_connecting_and_sending_cmd;
use crate::common::response;
use crate::common::track::TrackedFutureRegistry;
use crate::common::utils::{gen_moved, pretty_print_bytes, ThreadSafe};
use crate::common::version::UNDERMOON_MIGRATION_VERSION;
use crate::protocol::RespVec;
use crate::protocol::{RedisClientError, RedisClientFactory, Resp};
use crate::proxy::backend::{CmdTask, CmdTaskFactory, ReqTask};
use crate::proxy::blocking::{BlockingHandle, BlockingHintTask, TaskBlockingController};
use crate::proxy::cluster::ClusterSendError;
use crate::proxy::migration_backend::RestoreDataCmdTaskHandler;
use crate::proxy::sender::{CmdTaskSender, CmdTaskSenderFactory};
use crate::proxy::service::ServerProxyConfig;
use atomic_option::AtomicOption;
use futures::channel::oneshot;
use futures::{future, select, Future, FutureExt, TryFutureExt};
use futures_timer::Delay;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

// Cover this case:
// (1) random node
// (2) importing node (PreSwitched not done)
// (3) migrating node (PreSwitched done this time)
// (4) importing node again and finally process it.
pub const MAX_REDIRECTIONS: usize = 4;

pub struct RedisScanMigratingTask<RCF, T, BC>
where
    RCF: RedisClientFactory,
    T: CmdTask,
    BC: TaskBlockingController,
{
    mgr_config: Arc<AtomicMigrationConfig>,
    cluster_name: ClusterName,
    slot_range: SlotRange,
    range_map: RangeMap,
    meta: MigrationMeta,
    state: Arc<AtomicMigrationState>,
    client_factory: Arc<RCF>,
    stop_signal_sender: AtomicOption<oneshot::Sender<()>>,
    stop_signal_receiver: AtomicOption<oneshot::Receiver<()>>,
    task: Arc<ScanMigrationTask>,
    blocking_ctrl: Arc<BC>,
    future_registry: Arc<TrackedFutureRegistry>,
    phantom: PhantomData<T>,
    active_redirection: bool,
}

impl<RCF, T, BC> RedisScanMigratingTask<RCF, T, BC>
where
    RCF: RedisClientFactory,
    T: CmdTask,
    BC: TaskBlockingController,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Arc<ServerProxyConfig>,
        mgr_config: Arc<AtomicMigrationConfig>,
        cluster_name: ClusterName,
        slot_range: SlotRange,
        meta: MigrationMeta,
        client_factory: Arc<RCF>,
        blocking_ctrl: Arc<BC>,
        future_registry: Arc<TrackedFutureRegistry>,
    ) -> Self {
        let (stop_signal_sender, stop_signal_receiver) = oneshot::channel();
        let task = ScanMigrationTask::new(
            meta.src_node_address.clone(),
            meta.dst_node_address.clone(),
            slot_range.clone(),
            client_factory.clone(),
            mgr_config.clone(),
        );
        let range_map = RangeMap::from(slot_range.get_range_list());
        let active_redirection = config.active_redirection;
        Self {
            mgr_config,
            cluster_name,
            slot_range,
            range_map,
            meta,
            state: Arc::new(AtomicMigrationState::initial_state()),
            client_factory,
            stop_signal_sender: AtomicOption::new(Box::new(stop_signal_sender)),
            stop_signal_receiver: AtomicOption::new(Box::new(stop_signal_receiver)),
            task: Arc::new(task),
            blocking_ctrl,
            future_registry,
            phantom: PhantomData,
            active_redirection,
        }
    }

    fn gen_switch_arg(&self, sub_cmd: &str) -> Vec<String> {
        let mut cmd = vec!["UMCTL".to_string(), sub_cmd.to_string()];
        let arg = SwitchArg {
            version: UNDERMOON_MIGRATION_VERSION.to_string(),
            meta: MigrationTaskMeta {
                cluster_name: self.cluster_name.clone(),
                slot_range: SlotRange {
                    range_list: self.slot_range.to_range_list(),
                    tag: SlotRangeTag::Migrating(self.meta.clone()),
                },
            },
        }
        .into_strings();
        cmd.extend(arg.into_iter());
        cmd
    }

    async fn pre_check(&self) {
        let state = self.state.clone();
        let meta = self.meta.clone();

        let handle_pre_check = move |resp: RespVec| -> Result<(), RedisClientError> {
            match resp {
                Resp::Error(err_str) => {
                    if err_str == response::NOT_READY_FOR_SWITCHING_REPLY.as_bytes() {
                        debug!("pre_check not ready, try again {:?}", meta)
                    } else if err_str == response::TASK_NOT_FOUND.as_bytes() {
                        warn!("peer task not found");
                    } else {
                        error!(
                            "failed to check: {:?}",
                            pretty_print_bytes(err_str.as_slice())
                        );
                    }
                    Ok(())
                }
                _reply => {
                    info!("pre_check done");
                    state.set_state(MigrationState::PreBlocking);
                    Err(RedisClientError::Done)
                }
            }
        };

        let client_factory = self.client_factory.clone();
        let dst_proxy_address = self.meta.dst_proxy_address.clone();
        let cmd = self
            .gen_switch_arg("PRECHECK")
            .into_iter()
            .map(|e| e.into_bytes())
            .collect();
        let interval = Duration::from_millis(10);

        keep_connecting_and_sending_cmd(
            client_factory,
            dst_proxy_address,
            cmd,
            interval,
            handle_pre_check,
        )
        .await;
        info!("pre_check done");
    }

    async fn pre_block(&self) -> BlockingHandle<BC::Sender> {
        let state = self.state.clone();

        let ctrl = self.blocking_ctrl.clone();
        let blocking_handle = ctrl.start_blocking();
        while !ctrl.blocking_done() {
            Delay::new(Duration::from_millis(1)).await;
        }
        state.set_state(MigrationState::PreSwitch);
        info!("pre_block done");
        blocking_handle
    }

    async fn pre_switch(&self) {
        let state = self.state.clone();
        let meta = self.meta.clone();

        let handle_pre_switch = move |resp: RespVec| -> Result<(), RedisClientError> {
            match resp {
                Resp::Error(err_str) => {
                    if err_str == response::NOT_READY_FOR_SWITCHING_REPLY.as_bytes() {
                        debug!("pre_switch not ready, try again {:?}", meta)
                    } else if err_str == response::TASK_NOT_FOUND.as_bytes() {
                        warn!("task not found, try again {:?}", meta)
                    } else {
                        error!(
                            "failed to switch: {:?}",
                            pretty_print_bytes(err_str.as_slice())
                        );
                    }
                    Ok(())
                }
                _reply => {
                    state.set_state(MigrationState::Scanning);
                    Err(RedisClientError::Done)
                }
            }
        };

        let client_factory = self.client_factory.clone();
        let dst_proxy_address = self.meta.dst_proxy_address.clone();
        let cmd = self
            .gen_switch_arg("PRESWITCH")
            .into_iter()
            .map(|e| e.into_bytes())
            .collect();
        let interval = Duration::from_millis(1);

        keep_connecting_and_sending_cmd(
            client_factory,
            dst_proxy_address,
            cmd,
            interval,
            handle_pre_switch,
        )
        .await;
        info!("pre_switch done");
    }

    async fn scan_migrate(&self) -> Result<(), MigrationError> {
        let state = self.state.clone();
        let (producer, consumer) = self
            .task
            .start()
            .ok_or_else(|| MigrationError::AlreadyStarted)?;

        let fut = producer
            .map_ok(|()| info!("migration producer finished scanning"))
            .map_err(|err| {
                error!("migration producer finished error: {:?}", err);
            });

        let desc = format!(
            "scan_migrating: cluster_name={} meta={:?} slot_range=({})",
            self.cluster_name,
            self.meta,
            self.slot_range.get_range_list().to_strings().join(" "),
        );
        let fut = TrackedFutureRegistry::wrap(self.future_registry.clone(), fut, desc);
        tokio::spawn(fut);

        match consumer.await {
            Ok(()) => {
                state.set_state(MigrationState::FinalSwitch);
                info!("migration consumer finished forwarding data");
                Ok(())
            }
            Err(err) => {
                error!("migration consumer finished error: {:?}", err);
                Err(err)
            }
        }
    }

    async fn final_switch(&self) {
        let state = self.state.clone();
        let meta = self.meta.clone();

        let handle_final_switch = move |resp: RespVec| -> Result<(), RedisClientError> {
            match resp {
                Resp::Error(err_str) => {
                    error!(
                        "failed to switch: {:?} {:?}",
                        pretty_print_bytes(err_str.as_slice()),
                        meta,
                    );
                    Ok(())
                }
                _reply => {
                    info!("final_switch done: {:?}", meta);
                    state.set_state(MigrationState::SwitchCommitted);
                    Err(RedisClientError::Done)
                }
            }
        };

        let client_factory = self.client_factory.clone();
        let dst_proxy_address = self.meta.dst_proxy_address.clone();
        let cmd = self
            .gen_switch_arg("FINALSWITCH")
            .into_iter()
            .map(|e| e.into_bytes())
            .collect();
        let interval = Duration::from_millis(1);

        keep_connecting_and_sending_cmd(
            client_factory,
            dst_proxy_address,
            cmd,
            interval,
            handle_final_switch,
        )
        .await;
        info!("final_switch done");
    }

    async fn run(&self) -> Result<(), MigrationError> {
        let final_switch = self.final_switch();

        let timeout = Duration::from_secs(self.mgr_config.get_max_migration_time());
        let mut timeout_fut = Delay::new(timeout).fuse();
        select! {
            () = timeout_fut => error!("migration timeout after {:?}, force to commit migration", timeout),
            res = self.run_migration().fuse() => res?,
        };
        final_switch.await;

        Ok(())
    }

    async fn run_migration(&self) -> Result<(), MigrationError> {
        let pre_check = self.pre_check();
        let pre_block = self.pre_block();
        let pre_switch = self.pre_switch();
        let scan_migrate = self.scan_migrate();

        pre_check.await;

        let blocking = async move {
            let blocking_handle = pre_block.await;
            pre_switch.await;
            blocking_handle.stop();
        };

        let max_blocking_time = self.mgr_config.get_max_blocking_time();
        let max_blocking_time = Duration::from_millis(max_blocking_time);
        let blocking_timeout = Delay::new(max_blocking_time);

        let res = select! {
            () = blocking.fuse() => Ok(()),
            () = blocking_timeout.fuse() => Err(MigrationError::Timeout),
        };

        if let Err(err) = res {
            error!("Migration failed {:?}. Force to go ahead.", err);
        }

        scan_migrate.await
    }
}

impl<RCF, T, BC> MigratingTask for RedisScanMigratingTask<RCF, T, BC>
where
    RCF: RedisClientFactory,
    T: CmdTask,
    BC: TaskBlockingController,
{
    type Task = T;

    fn start<'s>(
        &'s self,
    ) -> Pin<Box<dyn Future<Output = Result<(), MigrationError>> + Send + 's>> {
        let receiver = match self.stop_signal_receiver.take(Ordering::SeqCst) {
            Some(r) => r,
            None => return Box::pin(future::err(MigrationError::AlreadyStarted)),
        };

        let meta = self.meta.clone();
        let fut = self.run();

        let fut = async move {
            let r = select! {
                res = fut.fuse() => res,
                _ = receiver.fuse() => Err(MigrationError::Canceled),
            };
            match r {
                Ok(()) => {
                    info!("Migrating tasks stopped {:?}", meta);
                    Ok(())
                }
                Err(err) => {
                    error!("migration exit with error: {:?}", err);
                    Err(err)
                }
            }
        };

        Box::pin(fut)
    }

    fn send(
        &self,
        cmd_task: Self::Task,
    ) -> Result<(), ClusterSendError<BlockingHintTask<Self::Task>>> {
        let state = self.state.get_state();
        match state {
            MigrationState::PreCheck => {
                return Err(ClusterSendError::SlotNotFound(BlockingHintTask::new(
                    cmd_task, false,
                )))
            }
            MigrationState::PreBlocking | MigrationState::PreSwitch => {
                return Err(ClusterSendError::SlotNotFound(BlockingHintTask::new(
                    cmd_task, true,
                )));
            }
            _ => (),
        }

        handle_redirection(
            cmd_task,
            self.meta.dst_proxy_address.clone(),
            self.active_redirection,
        )
    }

    fn get_state(&self) -> MigrationState {
        self.state.get_state()
    }

    fn contains_slot(&self, slot: usize) -> bool {
        self.range_map.contains_slot(slot)
    }

    fn get_stop_handle(&self) -> Option<Box<dyn Drop + Send + Sync + 'static>> {
        let handle = MigratingTaskHandle {
            task: self.task.clone(),
            meta: self.meta.clone(),
            stop_signal_sender: Some(*self.stop_signal_sender.take(Ordering::SeqCst)?),
        };
        Some(Box::new(handle))
    }
}

pub struct MigratingTaskHandle {
    task: Arc<ScanMigrationTask>,
    meta: MigrationMeta,
    stop_signal_sender: Option<oneshot::Sender<()>>,
}

impl MigratingTaskHandle {
    fn send_stop_signal(&mut self) {
        info!("stop migrating task: {:?}", self.meta);
        self.task.stop();
        if let Some(sender) = self.stop_signal_sender.take() {
            if sender.send(()).is_err() {
                warn!("failed to send stop signal");
            }
        }
    }
}

impl Drop for MigratingTaskHandle {
    fn drop(&mut self) {
        self.send_stop_signal()
    }
}

pub struct RedisScanImportingTask<RCF, TSF, CTF>
where
    RCF: RedisClientFactory,
    TSF: CmdTaskSenderFactory + ThreadSafe,
    CTF: CmdTaskFactory + ThreadSafe,
    <TSF as CmdTaskSenderFactory>::Sender: ThreadSafe + CmdTaskSender<Task = ReqTask<CTF::Task>>,
{
    _mgr_config: Arc<AtomicMigrationConfig>,
    meta: MigrationMeta,
    range_map: RangeMap,
    state: Arc<AtomicMigrationState>,
    _client_factory: Arc<RCF>,
    _sender_factory: Arc<TSF>,
    stop_signal_sender: AtomicOption<oneshot::Sender<()>>,
    stop_signal_receiver: AtomicOption<oneshot::Receiver<()>>,
    cmd_handler: RestoreDataCmdTaskHandler<CTF, <TSF as CmdTaskSenderFactory>::Sender>,
    _cmd_task_factory: Arc<CTF>,
    active_redirection: bool,
}

impl<RCF, TSF, CTF> RedisScanImportingTask<RCF, TSF, CTF>
where
    RCF: RedisClientFactory,
    TSF: CmdTaskSenderFactory + ThreadSafe,
    CTF: CmdTaskFactory + ThreadSafe,
    <TSF as CmdTaskSenderFactory>::Sender: ThreadSafe + CmdTaskSender<Task = ReqTask<CTF::Task>>,
{
    pub fn new(
        config: Arc<ServerProxyConfig>,
        mgr_config: Arc<AtomicMigrationConfig>,
        meta: MigrationMeta,
        slot_range: SlotRange,
        client_factory: Arc<RCF>,
        sender_factory: Arc<TSF>,
        cmd_task_factory: Arc<CTF>,
    ) -> Self {
        let src_sender = sender_factory.create(meta.src_node_address.clone());
        let dst_sender = sender_factory.create(meta.dst_node_address.clone());
        let cmd_handler =
            RestoreDataCmdTaskHandler::new(src_sender, dst_sender, cmd_task_factory.clone());
        let (stop_signal_sender, stop_signal_receiver) = oneshot::channel();
        let range_map = RangeMap::from(slot_range.get_range_list());
        let active_redirection = config.active_redirection;
        Self {
            _mgr_config: mgr_config,
            meta,
            range_map,
            state: Arc::new(AtomicMigrationState::initial_state()),
            _client_factory: client_factory,
            _sender_factory: sender_factory,
            stop_signal_sender: AtomicOption::new(Box::new(stop_signal_sender)),
            stop_signal_receiver: AtomicOption::new(Box::new(stop_signal_receiver)),
            cmd_handler,
            _cmd_task_factory: cmd_task_factory,
            active_redirection,
        }
    }
}

impl<RCF, TSF, CTF> ImportingTask for RedisScanImportingTask<RCF, TSF, CTF>
where
    RCF: RedisClientFactory,
    TSF: CmdTaskSenderFactory + ThreadSafe,
    CTF: CmdTaskFactory + ThreadSafe,
    <TSF as CmdTaskSenderFactory>::Sender: ThreadSafe + CmdTaskSender<Task = ReqTask<CTF::Task>>,
{
    type Task = CTF::Task;

    fn start<'s>(
        &'s self,
    ) -> Pin<Box<dyn Future<Output = Result<(), MigrationError>> + Send + 's>> {
        let receiver = match self.stop_signal_receiver.take(Ordering::SeqCst) {
            Some(r) => r,
            None => return Box::pin(future::err(MigrationError::AlreadyStarted)),
        };

        let meta = self.meta.clone();
        let fut = self.cmd_handler.run_task_handler();
        let stop_handle = self.cmd_handler.get_stop_handle();

        let fut = async move {
            let res = future::select(Box::pin(fut.fuse()), Box::pin(receiver.fuse())).await;
            match res {
                future::Either::Left(_) => {
                    error!("handler exited unexpectedly");
                }
                future::Either::Right((_, handler_task)) => {
                    info!("Received stop signal. Wait for the handler to finish all the remaining commnands.");
                    stop_handle.stop();
                    handler_task.await;
                }
            };
            warn!("Importing tasks stopped {:?}", meta);
            Ok(())
        };

        Box::pin(fut)
    }

    fn send(
        &self,
        cmd_task: Self::Task,
    ) -> Result<(), ClusterSendError<BlockingHintTask<Self::Task>>> {
        if self.state.get_state() == MigrationState::PreCheck {
            return handle_redirection(
                cmd_task,
                self.meta.src_proxy_address.clone(),
                self.active_redirection,
            );
        }

        self.cmd_handler.handle_cmd_task(cmd_task);
        Ok(())
    }

    fn get_state(&self) -> MigrationState {
        self.state.get_state()
    }

    fn contains_slot(&self, slot: usize) -> bool {
        self.range_map.contains_slot(slot)
    }

    fn get_stop_handle(&self) -> Option<Box<dyn Drop + Send + Sync + 'static>> {
        let handle = ImportingTaskHandle {
            meta: self.meta.clone(),
            stop_signal_sender: Some(*self.stop_signal_sender.take(Ordering::SeqCst)?),
        };
        Some(Box::new(handle))
    }

    fn handle_switch(
        &self,
        switch_arg: SwitchArg,
        sub_cmd: MgrSubCmd,
    ) -> Result<(), MigrationError> {
        if switch_arg.version != UNDERMOON_MIGRATION_VERSION {
            return Err(MigrationError::IncompatibleVersion);
        }

        match sub_cmd {
            MgrSubCmd::PreCheck => self.state.set_state(MigrationState::PreCheck),
            MgrSubCmd::PreSwitch => self.state.set_state(MigrationState::PreSwitch),
            MgrSubCmd::FinalSwitch => self.state.set_state(MigrationState::SwitchCommitted),
        }
        Ok(())
    }
}

pub struct ImportingTaskHandle {
    meta: MigrationMeta,
    stop_signal_sender: Option<oneshot::Sender<()>>,
}

impl ImportingTaskHandle {
    fn send_stop_signal(&mut self) {
        info!("stop importing task: {:?}", self.meta);
        if let Some(sender) = self.stop_signal_sender.take() {
            if sender.send(()).is_err() {
                warn!("failed to send stop signal");
            }
        }
    }
}

impl Drop for ImportingTaskHandle {
    fn drop(&mut self) {
        self.send_stop_signal()
    }
}

fn handle_redirection<T: CmdTask>(
    cmd_task: T,
    redirection_address: String,
    active_redirection: bool,
) -> Result<(), ClusterSendError<BlockingHintTask<T>>> {
    let slot = match cmd_task.get_slot() {
        Some(slot) => slot,
        None => {
            let resp = Resp::Error("missing key".to_string().into_bytes());
            cmd_task.set_resp_result(Ok(resp));
            return Ok(());
        }
    };

    if active_redirection {
        let cmd_task = BlockingHintTask::new(cmd_task, false);
        // Proceed the command inside this proxy.
        Err(ClusterSendError::ActiveRedirection {
            task: cmd_task,
            slot,
            address: redirection_address,
        })
    } else {
        let resp = Resp::Error(gen_moved(slot, redirection_address).into_bytes());
        cmd_task.set_resp_result(Ok(resp));
        Ok(())
    }
}
