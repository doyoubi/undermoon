use super::redis_controller::RedisImportingController;
use super::task::{
    AtomicMigrationState, ImportingTask, MigratingTask, MigrationConfig, MigrationError,
    MigrationState, SwitchArg,
};
use ::common::cluster::{MigrationMeta, MigrationTaskMeta, SlotRange, SlotRangeTag};
use ::common::resp_execution::keep_connecting_and_sending;
use ::common::utils::{ThreadSafe, NOT_READY_FOR_SWITCHING_REPLY};
use ::common::version::UNDERMOON_VERSION;
use ::protocol::{BulkStr, RedisClientError, RedisClientFactory, Resp};
use ::proxy::database::DBSendError;
use atomic_option::AtomicOption;
use crossbeam_channel;
use futures::sync::oneshot;
use futures::{future, stream, Future, Stream};
use futures_timer::Delay;
use proxy::backend::{CmdTaskSender, CmdTaskSenderFactory};
use std::collections::HashMap;
use std::iter;
use std::str;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub struct RedisMigratingTask<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> {
    config: Arc<MigrationConfig>,
    db_name: String,
    slot_range: (usize, usize),
    meta: MigrationMeta,
    state: Arc<AtomicMigrationState>,
    redirection_stopped: Arc<AtomicBool>,
    client_factory: Arc<RCF>,
    sender_factory: Arc<TSF>,
    cmd_task_sender:
        crossbeam_channel::Sender<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>,
    cmd_task_receiver: Arc<
        crossbeam_channel::Receiver<<<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task>,
    >,
    stop_signal: AtomicOption<oneshot::Sender<()>>,
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> ThreadSafe
    for RedisMigratingTask<RCF, TSF>
{
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> RedisMigratingTask<RCF, TSF> {
    pub fn new(
        config: Arc<MigrationConfig>,
        db_name: String,
        slot_range: (usize, usize),
        meta: MigrationMeta,
        client_factory: Arc<RCF>,
        sender_factory: Arc<TSF>,
    ) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded();
        Self {
            config,
            meta,
            db_name,
            slot_range,
            state: Arc::new(AtomicMigrationState::new()),
            redirection_stopped: Arc::new(AtomicBool::new(false)),
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

    fn replica_state_ready(
        states: &[ReplicaState],
        meta: &MigrationMeta,
        master_repl_offset: u64,
        offset_threshold: u64,
    ) -> bool {
        for state in states.iter() {
            if format!("{}:{}", state.ip, state.port) == meta.dst_node_address
                && master_repl_offset >= state.offset
                && (master_repl_offset - state.offset) < offset_threshold
            {
                return true;
            }
        }
        false
    }

    fn check_repl_state(&self) -> impl Future<Item = (), Error = MigrationError> + Send {
        let config = self.config.clone();
        let client_factory = self.client_factory.clone();
        let interval = Duration::new(1, 0);
        let meta = self.meta.clone();

        let handle_func = move |response| match response {
            Resp::Bulk(BulkStr::Str(data)) => {
                let info = match str::from_utf8(&data) {
                    Ok(s) => s.to_string(),
                    Err(e) => {
                        error!("failed to parse INFO REPLICATION to utf8 string {:?}", e);
                        return Ok(());
                    }
                };
                match extract_replicas_from_replication_info(info) {
                    Ok((master_repl_offset, states)) => {
                        // Put config inside this closure to make dynamically change possible.
                        let offset_threshold = config.get_offset_threshold();
                        if Self::replica_state_ready(
                            &states,
                            &meta,
                            master_repl_offset,
                            offset_threshold,
                        ) {
                            info!("replication for migration is done {:?}", states);
                            Err(RedisClientError::Done)
                        } else {
                            debug!(
                                "replication for migration is still not ready {:?} {:?}",
                                meta, states
                            );
                            Ok(())
                        }
                    }
                    Err(()) => {
                        error!("failed to parse INFO REPLICATION {:?}", meta);
                        Ok(())
                    }
                }
            }
            reply => {
                error!("failed to get replication info {:?} {:?}", meta, reply);
                Ok(())
            }
        };

        let cmd = vec!["INFO".to_string(), "REPLICATION".to_string()];
        keep_connecting_and_sending(
            client_factory,
            self.meta.src_node_address.clone(),
            cmd,
            interval,
            handle_func,
        )
        .then(|result| {
            info!("check_repl_state done {:?}", result);
            Ok(())
        })
    }

    fn block_request(&self) -> impl Future<Item = (), Error = MigrationError> + Send {
        let min_blocking_time = Duration::from_millis(self.config.get_min_blocking_time());
        let state = self.state.clone();
        let meta = self.meta.clone();
        future::ok(())
            .map(move |()| {
                info!("start to block request {:?}", meta);
                state.set_state(MigrationState::Blocking);
            })
            .and_then(move |()| Delay::new(min_blocking_time).map_err(MigrationError::Io))
            .then(|result| {
                info!("blocking request done {:?}", result);
                Ok(())
            })
    }

    fn commit_switch(&self) -> impl Future<Item = (), Error = MigrationError> + Send {
        let state = self.state.clone();
        let state_clone = self.state.clone();
        let client_factory = self.client_factory.clone();

        let mut cmd = vec!["UMCTL".to_string(), "TMPSWITCH".to_string()];
        let arg = SwitchArg {
            version: UNDERMOON_VERSION.to_string(),
            meta: MigrationTaskMeta {
                db_name: self.db_name.clone(),
                slot_range: SlotRange {
                    start: self.slot_range.0,
                    end: self.slot_range.1,
                    tag: SlotRangeTag::Migrating(self.meta.clone()),
                },
            },
        }
        .into_strings();
        cmd.extend(arg.into_iter());

        let interval = Duration::from_millis(self.config.get_switch_retry_interval());
        let meta = self.meta.clone();

        let handle_func = move |response| match response {
            Resp::Error(err_str) => {
                if err_str == NOT_READY_FOR_SWITCHING_REPLY.as_bytes() {
                    info!("switch not ready, try again {:?}", meta)
                } else if state.get_state() != MigrationState::SwitchCommitted {
                    // only log when we still have not committed.
                    error!("failed to switch {:?} {:?}", meta, err_str);
                }
                Ok(())
            }
            reply => {
                if state.get_state() != MigrationState::SwitchCommitted {
                    info!("Migrationg node successfully switch {:?} {:?}", meta, reply);
                }
                state.set_state(MigrationState::SwitchCommitted);
                // Even we have already committed, the destination proxy might fail and reboot.
                // We just keep the sending, but suppress the logs.
                Ok(())
            }
        };

        let keep_sending = keep_connecting_and_sending(
            client_factory,
            self.meta.dst_proxy_address.clone(),
            cmd,
            interval,
            handle_func,
        )
        .then(|result| {
            error!("commit_switch failed {:?}", result);
            Ok(())
        });

        future::ok(())
            .map(move |()| state_clone.set_state(MigrationState::SwitchStarted))
            .and_then(|()| keep_sending)
    }

    fn release_queue(&self) -> impl Future<Item = (), Error = MigrationError> + Send {
        let state = self.state.clone();
        let sender_factory = self.sender_factory.clone();
        let dst_proxy_address = self.meta.dst_proxy_address.clone();
        let cmd_task_receiver = self.cmd_task_receiver.clone();

        let max_blocking_time = u128::from(self.config.get_max_blocking_time());

        let s = stream::iter_ok(iter::repeat(()));
        s.fold(
            0,
            move |lasting_time, ()| -> Box<dyn Future<Item = u128, Error = ()> + Send> {
                let delay_time = if lasting_time > max_blocking_time {
                    warn!("Commit status does not change for so long. Force commit.");
                    state.set_state(MigrationState::SwitchCommitted);
                    Duration::from_millis(0)
                } else {
                    Duration::from_millis(5)
                };

                if state.get_state() != MigrationState::SwitchCommitted {
                    let acc_time = lasting_time + delay_time.as_millis();
                    return Box::new(
                        Delay::new(delay_time)
                            .map(move |_| acc_time)
                            .map_err(|_| ()),
                    );
                }

                info!("start to drain waiting queue");
                Self::drain_waiting_queue(
                    sender_factory.clone(),
                    dst_proxy_address.clone(),
                    cmd_task_receiver.clone(),
                );
                info!("finished draining waiting queue");

                Box::new(
                    future::err(()), // stop
                )
            },
        )
        .then(|result: Result<u128, ()>| {
            info!("release_queue done {:?}", result);
            future::ok(())
        })
    }

    fn stop_redirection(
        redirection_stopped: Arc<AtomicBool>,
        redirection_timeout: u64,
    ) -> impl Future<Item = (), Error = MigrationError> + Send {
        let delay_time = Duration::from_millis(redirection_timeout);
        let delay = Delay::new(delay_time).map_err(MigrationError::Io);
        delay
            .then(move |result| {
                if let Err(err) = result {
                    error!("stop direction timer error {:?}", err);
                }
                info!("Redirecting for too long. Stop it.");
                redirection_stopped.store(true, Ordering::SeqCst);
                future::ok(())
            })
            .map_err(MigrationError::Io)
    }

    fn drain_waiting_queue(
        sender_factory: Arc<TSF>,
        dst_proxy_address: String,
        cmd_task_receiver: Arc<
            crossbeam_channel::Receiver<
                <<TSF as CmdTaskSenderFactory>::Sender as CmdTaskSender>::Task,
            >,
        >,
    ) {
        let sender = sender_factory.create(dst_proxy_address);
        while let Ok(cmd_task) = cmd_task_receiver.try_recv() {
            if let Err(err) = sender.send(cmd_task) {
                error!("failed to drain task {:?}", err);
            }
        }
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

        let meta = self.meta.clone();

        let redirection_stopped = self.redirection_stopped.clone();
        let redirection_timeout = self.config.get_max_redirection_time();

        let check_phase = self.check_repl_state();
        let block_request = self.block_request();
        let commit_switch = self.commit_switch();
        let release_queue = self.release_queue();
        let release_queue_or_timeout = release_queue
            .and_then(move |()| Self::stop_redirection(redirection_stopped, redirection_timeout));
        let migration_fut = check_phase
            .and_then(|()| block_request)
            .and_then(move |()| {
                info!("start to commit {:?}", meta);
                commit_switch.join(release_queue_or_timeout).map(|_| ())
            });

        let meta = self.meta.clone();

        Box::new(
            receiver
                .map_err(|_| MigrationError::Canceled)
                .select(migration_fut)
                .then(move |result| {
                    match result {
                        Ok(_) => warn!("RedisMigratingTask stopped {:?}", meta),
                        Err((err, _other)) => {
                            error!("RedisMigratingTask stopped with error {:?} {:?}", err, meta)
                        }
                    }
                    future::ok(())
                }),
        )
    }

    fn stop(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send> {
        Box::new(future::result(self.send_stop_signal()))
    }

    fn send(&self, cmd_task: Self::Task) -> Result<(), DBSendError<Self::Task>> {
        if self.state.get_state() == MigrationState::TransferringData
            || self.redirection_stopped.load(Ordering::SeqCst)
        {
            return Err(DBSendError::SlotNotFound(cmd_task));
        }

        let redirection_sender = self
            .sender_factory
            .create(self.meta.dst_proxy_address.clone());

        if self.state.get_state() == MigrationState::SwitchCommitted {
            return redirection_sender
                .send(cmd_task)
                .map_err(|_e| DBSendError::MigrationError);
        }

        let res = self.cmd_task_sender.send(cmd_task).or_else(move |err| {
            error!("Failed to tmp queue {:?}", err);
            let cmd_task = err.into_inner();
            redirection_sender
                .send(cmd_task)
                .map_err(|_e| DBSendError::MigrationError)
        });

        // This can make sure that waiting queue will always finally be cleaned up.
        if self.state.get_state() == MigrationState::SwitchCommitted {
            Self::drain_waiting_queue(
                self.sender_factory.clone(),
                self.meta.dst_proxy_address.clone(),
                self.cmd_task_receiver.clone(),
            );
        }

        res
    }

    fn get_state(&self) -> MigrationState {
        self.state.get_state()
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
    config: Arc<MigrationConfig>,
    meta: MigrationMeta,
    state: Arc<AtomicMigrationState>,
    sender_factory: Arc<TSF>,
    stop_signal: AtomicOption<oneshot::Sender<()>>,
    redis_importing_controller: RedisImportingController<RCF>,
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> ThreadSafe
    for RedisImportingTask<RCF, TSF>
{
}

impl<RCF: RedisClientFactory, TSF: CmdTaskSenderFactory + ThreadSafe> RedisImportingTask<RCF, TSF> {
    pub fn new(
        config: Arc<MigrationConfig>,
        db_name: String,
        meta: MigrationMeta,
        client_factory: Arc<RCF>,
        sender_factory: Arc<TSF>,
    ) -> Self {
        Self {
            config,
            meta: meta.clone(),
            state: Arc::new(AtomicMigrationState::new()),
            sender_factory,
            stop_signal: AtomicOption::empty(),
            redis_importing_controller: RedisImportingController::new(
                db_name,
                meta.clone(),
                client_factory,
            ),
        }
    }

    fn release_importing_for_timeout(
        &self,
    ) -> impl Future<Item = (), Error = MigrationError> + Send {
        let state = self.state.clone();
        let max_blocking_time = self.config.get_max_blocking_time();
        let delay_time = Duration::from_millis(max_blocking_time);
        let delay = Delay::new(delay_time).map_err(MigrationError::Io);
        delay.then(move |result| {
            if let Err(err) = result {
                error!("importing timer error {:?}", err);
            }

            info!("Importing timeout. Release importing slots");
            state.set_state(MigrationState::SwitchCommitted);
            future::ok(())
        })
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

        let timeout_release = self.release_importing_for_timeout();
        let importing_control = self.redis_importing_controller.start();
        Box::new(
            receiver
                .map_err(|_| MigrationError::Canceled)
                .select(timeout_release.join(importing_control).map(|_| ()))
                .then(move |_| {
                    warn!("Importing tasks stopped {:?}", meta);
                    future::ok(())
                }),
        )
    }

    fn stop(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send> {
        Box::new(future::result(self.send_stop_signal()))
    }

    fn send(&self, cmd_task: Self::Task) -> Result<(), DBSendError<Self::Task>> {
        // Already checked slot range in manager.
        if self.state.get_state() == MigrationState::SwitchCommitted {
            return Err(DBSendError::SlotNotFound(cmd_task));
        }

        let redirection_sender = self
            .sender_factory
            .create(self.meta.src_proxy_address.clone());
        redirection_sender
            .send(cmd_task)
            .map_err(|_e| DBSendError::MigrationError)
    }

    fn commit(&self, switch_arg: SwitchArg) -> Result<(), MigrationError> {
        if switch_arg.version != UNDERMOON_VERSION {
            return Err(MigrationError::IncompatibleVersion);
        }

        self.redis_importing_controller.switch_to_master()?;

        if self.state.get_state() != MigrationState::SwitchCommitted {
            info!("importing node commit switch {:?}", self.meta);
        }
        self.state.set_state(MigrationState::SwitchCommitted);
        Ok(())
    }
}

#[derive(Debug)]
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

fn extract_replicas_from_replication_info(info: String) -> Result<(u64, Vec<ReplicaState>), ()> {
    let mut master_repl_offset: u64 = 0;
    let mut states = Vec::new();
    let lines = info.split("\r\n");
    for line in lines {
        if line.starts_with("master_repl_offset") {
            master_repl_offset = parse_info_int(line)?;
            continue;
        }
        if !line.starts_with("slave") {
            continue;
        }
        let mut kv = line.split(':');
        let _slavex = kv.next().ok_or(())?;
        let value = kv.next().ok_or(())?.to_string();
        states.push(ReplicaState::parse_replica_meta(value)?);
    }
    Ok((master_repl_offset, states))
}

fn parse_info_int(line: &str) -> Result<u64, ()> {
    let mut kv = line.split(':');
    let _field = kv.next().ok_or(())?;
    kv.next().ok_or(())?.parse::<u64>().map_err(|_| ())
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
        let value = "ip=redis5,port=6379,state=online,offset=28,lag=1";
        let meta =
            ReplicaState::parse_replica_meta(value.to_string()).expect("test_parse_replication");
        assert_eq!(meta.ip, "redis5");
        assert_eq!(meta.port, 6379);
        assert_eq!(meta.state, "online");
        assert_eq!(meta.offset, 28);
        assert_eq!(meta.lag, 1);

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
        let (master_repl_offset, states) =
            extract_replicas_from_replication_info(replication_info.to_string())
                .expect("test_parse_replication");
        assert_eq!(master_repl_offset, 56);
        assert_eq!(states.len(), 2);
    }
}
