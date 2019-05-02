use super::task::{
    AtomicMigrationState, ImportingTask, MigratingTask, MigrationError, MigrationState,
};
use ::common::cluster::MigrationMeta;
use ::common::resp_execution::keep_connecting_and_sending;
use ::common::utils::ThreadSafe;
use ::protocol::{RedisClientFactory, Resp};
use ::proxy::database::DBSendError;
use atomic_option::AtomicOption;
use futures::sync::mpsc;
use futures::sync::oneshot;
use futures::{future, Future};
use proxy::backend::{CmdTaskSender, CmdTaskSenderFactory};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

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

    fn check_repl_state(&self) -> impl Future<Item = (), Error = MigrationError> + Send {
        future::ok(())
    }

    fn commit_switch(&self) -> impl Future<Item = (), Error = MigrationError> + Send {
        self.state.set_state(MigrationState::SwitchStarted);

        let state = self.state.clone();
        let client_factory = self.client_factory.clone();

        let cmd = vec![
            "UMCTL".to_string(),
            "TMPSWITCH".to_string(),
            self.meta.epoch.to_string(),
            self.meta.src_node_address.clone(),
            self.meta.src_proxy_address.clone(),
            self.meta.dst_node_address.clone(),
            self.meta.dst_node_address.clone(),
        ];
        let interval = Duration::new(1, 0);
        let meta = self.meta.clone();

        let handle_func = move |response| match response {
            Resp::Error(err_str) => {
                error!("failed to switch {:?} {:?}", meta, err_str);
                Ok(())
            }
            reply => {
                state.set_state(MigrationState::SwitchCommitted);
                info!("Successfully switch {:?} {:?}", meta, reply);
                Ok(())
            }
        };

        info!("try switching {:?}", cmd);
        keep_connecting_and_sending(
            client_factory,
            self.meta.dst_proxy_address.clone(),
            cmd,
            interval,
            handle_func,
        )
        .map_err(MigrationError::RedisError)
    }
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> MigratingTask
    for RedisMigratingTask<RCF, TSF>
{
    type Task = <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task;

    fn start(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send> {
        let (sender, receiver) = oneshot::channel();
        if self
            .stop_signal
            .try_store(Box::new(sender), Ordering::SeqCst)
            .is_some()
        {
            return Box::new(future::err(MigrationError::AlreadyStarted));
        }

        let check_phase = self.check_repl_state();
        let commit_phase = self.commit_switch();
        let migration_fut = check_phase.and_then(|()| commit_phase);

        let meta = self.meta.clone();

        Box::new(
            receiver
                .map_err(|_| MigrationError::Canceled)
                .select(migration_fut)
                .then(move |_| {
                    warn!("RedisMasterReplicator {:?} stopped", meta);
                    future::ok(())
                }),
        )
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
        let (sender, receiver) = oneshot::channel();
        if self
            .stop_signal
            .try_store(Box::new(sender), Ordering::SeqCst)
            .is_some()
        {
            return Box::new(future::err(MigrationError::AlreadyStarted));
        }

        let meta = self.meta.clone();

        // Now it just does nothing.
        // TODO: Add state monitoring and print them to logs.
        Box::new(
            receiver
                .map_err(|_| MigrationError::Canceled)
                .select(future::ok(()))
                .then(move |_| {
                    warn!("Importing tasks {:?} stopped", meta);
                    future::ok(())
                }),
        )
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

struct ReplicaState {
    ip: String,
    port: u64,
    state: String,
    offset: u64,
    lag: u64,
}

impl ReplicaState {
    fn parse_replica_meta(value: String) -> Result<Self, ()> {
        let mut kv_map = HashMap::new();

        let segs = value.split(',');
        for kv in segs {
            let mut kv_segs_iter = kv.split('=');
            let key = kv_segs_iter.next().ok_or(())?;
            let value = kv_segs_iter.next().ok_or(())?;
            kv_map.insert(key, value);
        }

        Ok(ReplicaState {
            ip: kv_map.get("ip").ok_or(())?.to_string(),
            port: kv_map
                .get("port")
                .ok_or(())?
                .parse::<u64>()
                .map_err(|_| ())?,
            state: kv_map.get("state").ok_or(())?.to_string(),
            offset: kv_map
                .get("offset")
                .ok_or(())?
                .parse::<u64>()
                .map_err(|_| ())?,
            lag: kv_map
                .get("lag")
                .ok_or(())?
                .parse::<u64>()
                .map_err(|_| ())?,
        })
    }
}

fn extract_replicas_from_replication_info(info: String) -> Result<Vec<ReplicaState>, ()> {
    let mut states = Vec::new();
    let lines = info.split("\r\n");
    for line in lines {
        if !line.starts_with("slave") {
            continue;
        }
        let mut kv = line.split(':');
        let slavex = kv.next().ok_or(())?;
        let mut value = kv.next().ok_or(())?.to_string();
        value.pop().ok_or(())?;
        states.push(ReplicaState::parse_replica_meta(value)?);
    }
    Ok(states)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_slave_value() {
        let value = "ip=127.0.0.1,port=6000,state=online,offset=233,lag=6699";
        let state =
            ReplicaState::parse_replica_meta(value.to_string()).expect("test_parse_slave_value");
        assert_eq!(state.ip, "127.0.0.1");
        assert_eq!(state.port, 6000);
        assert_eq!(state.state, "online");
        assert_eq!(state.offset, 233);
        assert_eq!(state.lag, 6699);
    }

    #[test]
    fn test_parse_replication() {
        let replication_info = "Replication\r
role:master\r
connected_slaves:1\r
slave0:ip=127.0.0.1,port=6000,state=online,offset=233,lag=6699\r
slave1:ip=127.0.0.2,port=6001,state=online,offset=666,lag=7799\r
master_replid:3934c1b1bce5d067567f7e263301879303e8f633\r
master_replid2:0000000000000000000000000000000000000000\r
master_repl_offset:56\r
second_repl_offset:-1\r
repl_backlog_active:1\r
repl_backlog_size:1048576\r
repl_backlog_first_byte_offset:1\r
repl_backlog_histlen:56\r";
        println!("{:?}", replication_info);
        let states = extract_replicas_from_replication_info(replication_info.to_string())
            .expect("test_parse_replication");
        assert_eq!(states.len(), 2);
    }
}
