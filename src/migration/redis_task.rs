use super::task::{
    AtomicMigrationState, ImportingTask, MigratingTask, MigrationError, MigrationState,
    MigrationTaskMeta,
};
use ::common::cluster::MigrationMeta;
use ::common::utils::ThreadSafe;
use ::protocol::RedisClientFactory;
use ::proxy::database::DBSendError;
use atomic_option::AtomicOption;
use futures::sync::mpsc;
use futures::sync::oneshot;
use futures::{future, Future};
use proxy::backend::{CmdTaskSender, CmdTaskSenderFactory};
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct RedisMigratingTask<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> {
    meta: MigrationMeta,
    state: Arc<AtomicMigrationState>,
    client_factory: Arc<RCF>,
    sender_factory: Arc<TSF>,
    cmd_task_sender:
        mpsc::UnboundedSender<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>,
    cmd_task_receiver: Arc<
        mpsc::UnboundedReceiver<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>,
    >,
    stop_signal: AtomicOption<oneshot::Sender<()>>,
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> ThreadSafe
    for RedisMigratingTask<RCF, TSF>
{
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> RedisMigratingTask<RCF, TSF> {
    pub fn new(meta: MigrationMeta, client_factory: Arc<RCF>, sender_factory: Arc<TSF>) -> Self {
        let (sender, receiver) = mpsc::unbounded();
        Self {
            meta,
            state: Arc::new(AtomicMigrationState::new()),
            client_factory,
            sender_factory,
            cmd_task_sender: sender,
            cmd_task_receiver: Arc::new(receiver),
            stop_signal: AtomicOption::empty(),
        }
    }

    fn send_stop_signal(&self) -> Result<(), MigrationError> {
        if let Some(sender) = self.stop_signal.take(Ordering::SeqCst) {
            sender.send(()).map_err(|()| {
                error!("failed to send stop signal");
                MigrationError::Canceled
            })
        } else {
            Err(MigrationError::AlreadyEnded)
        }
    }
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> MigratingTask
    for RedisMigratingTask<RCF, TSF>
{
    type Task = <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task;

    fn start(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send> {
        // TODO: implement it
        Box::new(future::ok(()))
    }

    fn stop(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send> {
        Box::new(future::result(self.send_stop_signal()))
    }

    fn send(&self, cmd_task: Self::Task) -> Result<(), DBSendError<Self::Task>> {
        if self.state.get_state() == MigrationState::TransferringData {
            return Err(DBSendError::SlotNotFound(cmd_task));
        }

        self.cmd_task_sender
            .unbounded_send(cmd_task)
            .map_err(|err| {
                error!("Failed to tmp queue {:?}", err);
                DBSendError::MigrationError
            })
    }
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> Drop
    for RedisMigratingTask<RCF, TSF>
{
    fn drop(&mut self) {
        self.send_stop_signal().unwrap_or(())
    }
}

pub struct RedisImportingTask<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> {
    meta: MigrationMeta,
    state: Arc<AtomicMigrationState>,
    client_factory: Arc<RCF>,
    sender_factory: Arc<TSF>,
    stop_signal: AtomicOption<oneshot::Sender<()>>,
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> ThreadSafe
    for RedisImportingTask<RCF, TSF>
{
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> RedisImportingTask<RCF, TSF> {
    pub fn new(meta: MigrationMeta, client_factory: Arc<RCF>, sender_factory: Arc<TSF>) -> Self {
        Self {
            meta,
            state: Arc::new(AtomicMigrationState::new()),
            client_factory,
            sender_factory,
            stop_signal: AtomicOption::empty(),
        }
    }

    fn send_stop_signal(&self) -> Result<(), MigrationError> {
        if let Some(sender) = self.stop_signal.take(Ordering::SeqCst) {
            sender.send(()).map_err(|()| {
                error!("failed to send stop signal");
                MigrationError::Canceled
            })
        } else {
            Err(MigrationError::AlreadyEnded)
        }
    }
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> Drop
    for RedisImportingTask<RCF, TSF>
{
    fn drop(&mut self) {
        self.send_stop_signal().unwrap_or(())
    }
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> ImportingTask
    for RedisImportingTask<RCF, TSF>
{
    type Task = <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task;

    fn start(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send> {
        // TODO: implement it
        Box::new(future::ok(()))
    }

    fn stop(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send> {
        Box::new(future::result(self.send_stop_signal()))
    }

    fn send(&self, cmd_task: Self::Task) -> Result<(), DBSendError<Self::Task>> {
        if self.state.get_state() != MigrationState::SwitchCommitted {
            return Err(DBSendError::SlotNotFound(cmd_task));
        }

        let redirection_sender = self
            .sender_factory
            .create(self.meta.src_proxy_address.clone());
        redirection_sender
            .send(cmd_task)
            .map_err(|_e| DBSendError::MigrationError)
    }

    fn commit(&self) -> Result<(), MigrationError> {
        self.state.set_state(MigrationState::SwitchCommitted);
        Ok(())
    }
}
