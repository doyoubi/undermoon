use super::task::{
    AtomicMigrationState, ImportingTask, MigratingTask, MigrationError, MigrationState, SwitchArg,
};
use ::common::cluster::{MigrationMeta, MigrationTaskMeta, SlotRange, SlotRangeTag};
use ::common::config::AtomicMigrationConfig;
use ::common::utils::{ThreadSafe, NOT_READY_FOR_SWITCHING_REPLY};
use ::common::version::UNDERMOON_MIGRATION_VERSION_2;
use ::protocol::{BulkStr, RedisClientError, RedisClientFactory, Resp};
use ::proxy::database::DBSendError;
use atomic_option::AtomicOption;
use crossbeam_channel;
use futures::sync::oneshot;
use futures::{future, Future};
use futures_timer::Delay;
use proxy::backend::{CmdTaskSender, CmdTaskSenderFactory};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct RedisScanMigratingTask<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> {
    config: Arc<AtomicMigrationConfig>,
    db_name: String,
    slot_range: (usize, usize),
    meta: MigrationMeta,
    state: Arc<AtomicMigrationState>,
    client_factory: Arc<RCF>,
    sender_factory: Arc<TSF>,
    stop_signal_sender: AtomicOption<oneshot::Sender<()>>,
    stop_signal_receiver: AtomicOption<oneshot::Receiver<()>>,
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> ThreadSafe
for RedisScanMigratingTask<RCF, TSF>
{
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> RedisScanMigratingTask<RCF, TSF> {
    pub fn new(
        config: Arc<AtomicMigrationConfig>,
        db_name: String,
        slot_range: (usize, usize),
        meta: MigrationMeta,
        client_factory: Arc<RCF>,
        sender_factory: Arc<TSF>,
    ) -> Self {
        let (stop_signal_sender, stop_signal_receiver) = oneshot::channel();
        Self {
            config,
            meta,
            state: Arc::new(AtomicMigrationState::new()),
            client_factory,
            sender_factory,
            db_name,
            slot_range,
            stop_signal_sender: AtomicOption::new(Box::new(stop_signal_sender)),
            stop_signal_receiver: AtomicOption::new(Box::new(stop_signal_receiver)),
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

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> MigratingTask
for RedisScanMigratingTask<RCF, TSF>
{
    type Task = <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task;

    fn start(&self) -> Box<dyn Future<Item=(), Error=MigrationError> + Send> {
        let receiver = match self.stop_signal_receiver.take(Ordering::SeqCst) {
            Some(r) => r,
            None => return Box::new(future::err(MigrationError::AlreadyStarted)),
        };
        Box::new(future::ok(()))
    }

    fn stop(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send> {
        Box::new(future::result(self.send_stop_signal()))
    }

    fn send(&self, cmd_task: Self::Task) -> Result<(), DBSendError<Self::Task>> {
        Ok(())
    }

    fn get_state(&self) -> MigrationState {
        self.state.get_state()
    }
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> Drop
for RedisScanMigratingTask<RCF, TSF>
{
    fn drop(&mut self) {
        self.send_stop_signal().unwrap_or(())
    }
}

pub struct RedisScanImportingTask<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> {
    config: Arc<AtomicMigrationConfig>,
    meta: MigrationMeta,
    state: Arc<AtomicMigrationState>,
    client_factory: Arc<RCF>,
    sender_factory: Arc<TSF>,
    stop_signal_sender: AtomicOption<oneshot::Sender<()>>,
    stop_signal_receiver: AtomicOption<oneshot::Receiver<()>>,
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> ThreadSafe
for RedisScanImportingTask<RCF, TSF>
{
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> RedisScanImportingTask<RCF, TSF> {
    pub fn new(
        config: Arc<AtomicMigrationConfig>,
        db_name: String,
        meta: MigrationMeta,
        client_factory: Arc<RCF>,
        sender_factory: Arc<TSF>,
    ) -> Self {
        let (stop_signal_sender, stop_signal_receiver) = oneshot::channel();
        Self {
            config,
            meta: meta.clone(),
            state: Arc::new(AtomicMigrationState::new()),
            client_factory,
            sender_factory,
            stop_signal_sender: AtomicOption::new(Box::new(stop_signal_sender)),
            stop_signal_receiver: AtomicOption::new(Box::new(stop_signal_receiver)),
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

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> Drop
for RedisScanImportingTask<RCF, TSF>
{
    fn drop(&mut self) {
        self.send_stop_signal().unwrap_or(())
    }
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> ImportingTask
for RedisScanImportingTask<RCF, TSF>
{
    type Task = <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task;

    fn start(&self) -> Box<dyn Future<Item=(), Error=MigrationError> + Send> {
        let receiver = match self.stop_signal_receiver.take(Ordering::SeqCst) {
            Some(r) => r,
            None => return Box::new(future::err(MigrationError::AlreadyStarted)),
        };

        Box::new(future::ok(()))
    }

    fn stop(&self) -> Box<dyn Future<Item=(), Error=MigrationError> + Send> {
        Box::new(future::result(self.send_stop_signal()))
    }

    fn send(&self, cmd_task: Self::Task) -> Result<(), DBSendError<Self::Task>> {
        Ok(())
    }

    fn commit(&self, switch_arg: SwitchArg) -> Result<(), MigrationError> {
        Ok(())
    }
}
