use super::scan_migration::ScanMigrationTask;
use super::task::{
    AtomicMigrationState, ImportingTask, MgrSubCmd, MigratingTask, MigrationError, MigrationState,
    SwitchArg,
};
use crate::common::cluster::{DBName, MigrationMeta, MigrationTaskMeta, SlotRange, SlotRangeTag};
use crate::common::config::AtomicMigrationConfig;
use crate::common::resp_execution::keep_connecting_and_sending_cmd;
use crate::common::utils::{pretty_print_bytes, ThreadSafe, NOT_READY_FOR_SWITCHING_REPLY};
use crate::common::version::UNDERMOON_MIGRATION_VERSION;
use crate::protocol::RespVec;
use crate::protocol::{RedisClientError, RedisClientFactory, Resp};
use crate::proxy::backend::{
    CmdTaskFactory, CmdTaskSender, CmdTaskSenderFactory, RedirectionSenderFactory,
};
use crate::proxy::blocking::{BlockingHandle, TaskBlockingController};
use crate::proxy::database::DBSendError;
use crate::proxy::migration_backend::RestoreDataCmdTaskHandler;
use crate::proxy::service::ServerProxyConfig;
use atomic_option::AtomicOption;
use futures::channel::oneshot;
use futures::{future, select, Future, FutureExt, TryFutureExt};
use futures_timer::Delay;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

pub struct RedisScanMigratingTask<
    RCF: RedisClientFactory,
    TSF: CmdTaskSenderFactory + ThreadSafe,
    BC: TaskBlockingController,
> {
    mgr_config: Arc<AtomicMigrationConfig>,
    db_name: DBName,
    slot_range: (usize, usize),
    meta: MigrationMeta,
    state: Arc<AtomicMigrationState>,
    client_factory: Arc<RCF>,
    _sender_factory: Arc<TSF>,
    redirection_sender_factory:
        RedirectionSenderFactory<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>,
    stop_signal_sender: AtomicOption<oneshot::Sender<()>>,
    stop_signal_receiver: AtomicOption<oneshot::Receiver<()>>,
    task: ScanMigrationTask,
    blocking_ctrl: Arc<BC>,
}

impl<
        RCF: RedisClientFactory,
        TSF: CmdTaskSenderFactory + ThreadSafe,
        BC: TaskBlockingController,
    > RedisScanMigratingTask<RCF, TSF, BC>
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        _config: Arc<ServerProxyConfig>,
        mgr_config: Arc<AtomicMigrationConfig>,
        db_name: DBName,
        slot_range: (usize, usize),
        meta: MigrationMeta,
        client_factory: Arc<RCF>,
        sender_factory: Arc<TSF>,
        blocking_ctrl: Arc<BC>,
    ) -> Self {
        let (stop_signal_sender, stop_signal_receiver) = oneshot::channel();
        let task = ScanMigrationTask::new(
            meta.src_node_address.clone(),
            meta.dst_node_address.clone(),
            slot_range,
            client_factory.clone(),
            mgr_config.clone(),
        );
        let redirection_sender_factory = RedirectionSenderFactory::default();
        Self {
            mgr_config,
            meta,
            state: Arc::new(AtomicMigrationState::new()),
            client_factory,
            _sender_factory: sender_factory,
            redirection_sender_factory,
            db_name,
            slot_range,
            stop_signal_sender: AtomicOption::new(Box::new(stop_signal_sender)),
            stop_signal_receiver: AtomicOption::new(Box::new(stop_signal_receiver)),
            task,
            blocking_ctrl,
        }
    }

    fn send_stop_signal(&self) -> Result<(), MigrationError> {
        if let Some(sender) = self.stop_signal_sender.take(Ordering::SeqCst) {
            sender.send(()).map_err(|()| {
                error!("failed to send stop signal");
                MigrationError::Canceled
            })
        } else {
            Err(MigrationError::AlreadyEnded)
        }
    }

    fn gen_switch_arg(&self, sub_cmd: &str) -> Vec<String> {
        let mut cmd = vec!["UMCTL".to_string(), sub_cmd.to_string()];
        let arg = SwitchArg {
            version: UNDERMOON_MIGRATION_VERSION.to_string(),
            meta: MigrationTaskMeta {
                cluster_name: self.db_name.clone(),
                slot_range: SlotRange {
                    start: self.slot_range.0,
                    end: self.slot_range.1,
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
                    if err_str == NOT_READY_FOR_SWITCHING_REPLY.as_bytes() {
                        debug!("pre_check not ready, try again {:?}", meta)
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
                    if err_str == NOT_READY_FOR_SWITCHING_REPLY.as_bytes() {
                        debug!("pre_switch not ready, try again {:?}", meta)
                    }
                    error!(
                        "failed to switch: {:?}",
                        pretty_print_bytes(err_str.as_slice())
                    );
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

        tokio::spawn(
            producer
                .map_ok(|()| info!("migration producer finished scanning"))
                .map_err(|err| {
                    error!("migration producer finished error: {:?}", err);
                }),
        );
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

impl<
        RCF: RedisClientFactory,
        TSF: CmdTaskSenderFactory + ThreadSafe,
        BC: TaskBlockingController,
    > MigratingTask for RedisScanMigratingTask<RCF, TSF, BC>
{
    type Task = <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task;

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

    fn stop<'s>(&'s self) -> Pin<Box<dyn Future<Output = Result<(), MigrationError>> + Send + 's>> {
        self.task.stop();
        let r = self.send_stop_signal();
        Box::pin(async { r })
    }

    fn send(&self, cmd_task: Self::Task) -> Result<(), DBSendError<Self::Task>> {
        let state = self.state.get_state();
        match state {
            MigrationState::PreCheck | MigrationState::PreBlocking | MigrationState::PreSwitch => {
                return Err(DBSendError::SlotNotFound(cmd_task));
            }
            _ => (),
        }

        // TODO: add blocking for PreBlocking

        let redirection_sender = self
            .redirection_sender_factory
            .create(self.meta.dst_proxy_address.clone());
        redirection_sender
            .send(cmd_task)
            .map_err(|_e| DBSendError::MigrationError)
    }

    fn get_state(&self) -> MigrationState {
        self.state.get_state()
    }
}

impl<
        RCF: RedisClientFactory,
        TSF: CmdTaskSenderFactory + ThreadSafe,
        BC: TaskBlockingController,
    > Drop for RedisScanMigratingTask<RCF, TSF, BC>
{
    fn drop(&mut self) {
        self.send_stop_signal().unwrap_or(())
    }
}

pub struct RedisScanImportingTask<RCF, TSF, CTF>
where
    RCF: RedisClientFactory,
    TSF: CmdTaskSenderFactory + ThreadSafe,
    CTF: CmdTaskFactory<Task = <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>
        + ThreadSafe,
    <TSF as CmdTaskSenderFactory>::Sender: ThreadSafe,
{
    _mgr_config: Arc<AtomicMigrationConfig>,
    meta: MigrationMeta,
    state: Arc<AtomicMigrationState>,
    _client_factory: Arc<RCF>,
    _sender_factory: Arc<TSF>,
    redirection_sender_factory:
        RedirectionSenderFactory<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>,
    stop_signal_sender: AtomicOption<oneshot::Sender<()>>,
    stop_signal_receiver: AtomicOption<oneshot::Receiver<()>>,
    cmd_handler: RestoreDataCmdTaskHandler<CTF, <TSF as CmdTaskSenderFactory>::Sender>,
    _cmd_task_factory: Arc<CTF>,
}

impl<RCF, TSF, CTF> RedisScanImportingTask<RCF, TSF, CTF>
where
    RCF: RedisClientFactory,
    TSF: CmdTaskSenderFactory + ThreadSafe,
    CTF: CmdTaskFactory<Task = <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>
        + ThreadSafe,
    <TSF as CmdTaskSenderFactory>::Sender: ThreadSafe,
{
    pub fn new(
        _config: Arc<ServerProxyConfig>,
        mgr_config: Arc<AtomicMigrationConfig>,
        _db_name: DBName,
        meta: MigrationMeta,
        client_factory: Arc<RCF>,
        sender_factory: Arc<TSF>,
        cmd_task_factory: Arc<CTF>,
    ) -> Self {
        let src_sender = sender_factory.create(meta.src_node_address.clone());
        let dst_sender = sender_factory.create(meta.dst_node_address.clone());
        let cmd_handler =
            RestoreDataCmdTaskHandler::new(src_sender, dst_sender, cmd_task_factory.clone());
        let (stop_signal_sender, stop_signal_receiver) = oneshot::channel();
        let redirection_sender_factory = RedirectionSenderFactory::default();
        Self {
            _mgr_config: mgr_config,
            meta,
            state: Arc::new(AtomicMigrationState::new()),
            _client_factory: client_factory,
            _sender_factory: sender_factory,
            redirection_sender_factory,
            stop_signal_sender: AtomicOption::new(Box::new(stop_signal_sender)),
            stop_signal_receiver: AtomicOption::new(Box::new(stop_signal_receiver)),
            cmd_handler,
            _cmd_task_factory: cmd_task_factory,
        }
    }

    fn send_stop_signal(&self) -> Result<(), MigrationError> {
        if let Some(sender) = self.stop_signal_sender.take(Ordering::SeqCst) {
            sender.send(()).map_err(|()| {
                error!("failed to send stop signal");
                MigrationError::Canceled
            })
        } else {
            Err(MigrationError::AlreadyEnded)
        }
    }
}

impl<RCF, TSF, CTF> Drop for RedisScanImportingTask<RCF, TSF, CTF>
where
    RCF: RedisClientFactory,
    TSF: CmdTaskSenderFactory + ThreadSafe,
    CTF: CmdTaskFactory<Task = <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>
        + ThreadSafe,
    <TSF as CmdTaskSenderFactory>::Sender: ThreadSafe,
{
    fn drop(&mut self) {
        self.send_stop_signal().unwrap_or(())
    }
}

impl<RCF, TSF, CTF> ImportingTask for RedisScanImportingTask<RCF, TSF, CTF>
where
    RCF: RedisClientFactory,
    TSF: CmdTaskSenderFactory + ThreadSafe,
    CTF: CmdTaskFactory<Task = <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>
        + ThreadSafe,
    <TSF as CmdTaskSenderFactory>::Sender: ThreadSafe,
{
    type Task = <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task;

    fn start<'s>(
        &'s self,
    ) -> Pin<Box<dyn Future<Output = Result<(), MigrationError>> + Send + 's>> {
        let receiver = match self.stop_signal_receiver.take(Ordering::SeqCst) {
            Some(r) => r,
            None => return Box::pin(future::err(MigrationError::AlreadyStarted)),
        };

        let meta = self.meta.clone();
        let fut = self.cmd_handler.run_task_handler();

        let fut = async move {
            let r = select! {
                () = fut.fuse() => Ok(()),
                _ = receiver.fuse() => Err(MigrationError::Canceled),
            };
            match r {
                Ok(()) => {
                    warn!("Importing tasks stopped {:?}", meta);
                    Ok(())
                }
                Err(err) => {
                    error!("importing exit with error: {:?}", err);
                    Err(err)
                }
            }
        };

        Box::pin(fut)
    }

    fn stop<'s>(&'s self) -> Pin<Box<dyn Future<Output = Result<(), MigrationError>> + Send + 's>> {
        let r = self.send_stop_signal();
        Box::pin(async { r })
    }

    fn send(&self, cmd_task: Self::Task) -> Result<(), DBSendError<Self::Task>> {
        if self.state.get_state() == MigrationState::PreCheck {
            let redirection_sender = self
                .redirection_sender_factory
                .create(self.meta.src_proxy_address.clone());
            return redirection_sender
                .send(cmd_task)
                .map_err(|_e| DBSendError::MigrationError);
        }

        self.cmd_handler.handle_cmd_task(cmd_task);
        Ok(())
    }

    fn get_state(&self) -> MigrationState {
        self.state.get_state()
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
