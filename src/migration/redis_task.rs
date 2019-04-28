use super::task::{
    AtomicMigrationState, ImportingTask, MigratingTask, MigrationError, MigrationState,
    MigrationTaskMeta,
};
use ::protocol::RedisClientFactory;
use ::proxy::database::DBSendError;
use atomic_option::AtomicOption;
use futures::sync::mpsc;
use futures::sync::oneshot;
use futures::{future, Future};
use proxy::backend::{CmdTaskSender, CmdTaskSenderFactory};
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct RedisMigratingTask<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory> {
    meta: MigrationTaskMeta,
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

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory> RedisMigratingTask<RCF, TSF> {
    pub fn new(
        meta: MigrationTaskMeta,
        client_factory: Arc<RCF>,
        sender_factory: Arc<TSF>,
    ) -> Self {
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

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory> MigratingTask
    for RedisMigratingTask<RCF, TSF>
{
    type Task = <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task;

    fn start(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send> {
        Box::new(future::ok(()))
    }

    fn stop(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send> {
        Box::new(future::result(self.send_stop_signal()))
    }

    fn send(&self, cmd_task: Self::Task) -> Result<(), DBSendError<Self::Task>> {
        Ok(())
    }
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory> Drop for RedisMigratingTask<RCF, TSF> {
    fn drop(&mut self) {
        self.send_stop_signal().unwrap_or(())
    }
}

pub struct RedisImportingTask<RCF: RedisClientFactory> {
    meta: MigrationTaskMeta,
    state: Arc<AtomicMigrationState>,
    client_factory: Arc<RCF>,
    stop_signal: AtomicOption<oneshot::Sender<()>>,
}

impl<RCF: RedisClientFactory> RedisImportingTask<RCF> {
    pub fn new(meta: MigrationTaskMeta, client_factory: Arc<RCF>) -> Self {
        Self {
            meta,
            state: Arc::new(AtomicMigrationState::new()),
            client_factory,
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

impl<RCF: RedisClientFactory> Drop for RedisImportingTask<RCF> {
    fn drop(&mut self) {
        self.send_stop_signal().unwrap_or(())
    }
}

impl<RCF: RedisClientFactory> ImportingTask for RedisImportingTask<RCF> {
    fn start(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send> {
        Box::new(future::ok(()))
    }
    fn stop(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send> {
        Box::new(future::result(self.send_stop_signal()))
    }
}
