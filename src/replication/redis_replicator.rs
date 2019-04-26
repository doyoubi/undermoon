use super::replicator::{
    MasterMeta, MasterReplicator, ReplicaMeta, ReplicaReplicator, ReplicatorError,
};
use atomic_option::AtomicOption;
use common::utils::{revolve_first_address, ThreadSafe};
use futures::sync::oneshot;
use futures::{future, stream, Future, Stream};
use futures_timer::Delay;
use protocol::RedisClientFactory;
use protocol::{RedisClient, RedisClientError, Resp};
use std::iter;
use std::str;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

pub struct RedisMasterReplicator<F: RedisClientFactory> {
    meta: MasterMeta,
    stop_signal: AtomicOption<oneshot::Sender<()>>,
    client_factory: Arc<F>,
}

impl<F: RedisClientFactory> ThreadSafe for RedisMasterReplicator<F> {}

impl<F: RedisClientFactory> RedisMasterReplicator<F> {
    pub fn new(meta: MasterMeta, client_factory: Arc<F>) -> Self {
        Self {
            meta,
            stop_signal: AtomicOption::empty(),
            client_factory,
        }
    }

    fn send_stop_signal(&self) -> Result<(), ReplicatorError> {
        if let Some(sender) = self.stop_signal.take(Ordering::SeqCst) {
            sender.send(()).map_err(|()| {
                error!("failed to send stop signal");
                ReplicatorError::Canceled
            })
        } else {
            Err(ReplicatorError::AlreadyEnded)
        }
    }
}

impl<F: RedisClientFactory> MasterReplicator for RedisMasterReplicator<F> {
    fn start(&self) -> Box<Future<Item = (), Error = ReplicatorError> + Send> {
        let (sender, receiver) = oneshot::channel();
        if self
            .stop_signal
            .try_store(Box::new(sender), Ordering::SeqCst)
            .is_some()
        {
            return Box::new(future::err(ReplicatorError::AlreadyStarted));
        }

        let client_fut = self
            .client_factory
            .create_client(self.meta.master_node_address.clone());
        let interval = Duration::new(5, 0);
        let cmd = vec!["SLAVEOF".to_string(), "NO".to_string(), "ONE".to_string()];
        let send_fut = client_fut.and_then(move |client| keep_sending_cmd(client, cmd, interval));

        let meta = self.meta.clone();

        Box::new(
            receiver
                .map_err(|_| ReplicatorError::Canceled)
                .select(send_fut.map_err(ReplicatorError::RedisError))
                .then(move |_| {
                    warn!("RedisMasterReplicator {:?} stopped", meta);
                    future::ok(())
                }),
        )
    }

    fn stop(&self) -> Box<Future<Item = (), Error = ReplicatorError> + Send> {
        Box::new(future::result(self.send_stop_signal()))
    }

    fn start_migrating(&self) -> Box<Future<Item = (), Error = ReplicatorError> + Send> {
        // TODO: implement migration
        Box::new(future::ok(()))
    }

    fn commit_migrating(&self) -> Box<Future<Item = (), Error = ReplicatorError> + Send> {
        // TODO: implement migration
        Box::new(future::ok(()))
    }

    fn get_meta(&self) -> &MasterMeta {
        &self.meta
    }
}

// Make sure the future will end.
impl<F: RedisClientFactory> Drop for RedisMasterReplicator<F> {
    fn drop(&mut self) {
        self.send_stop_signal().unwrap_or(())
    }
}

pub struct RedisReplicaReplicator<F: RedisClientFactory> {
    meta: ReplicaMeta,
    stop_signal: AtomicOption<oneshot::Sender<()>>,
    client_factory: Arc<F>,
}

impl<F: RedisClientFactory> ThreadSafe for RedisReplicaReplicator<F> {}

impl<F: RedisClientFactory> RedisReplicaReplicator<F> {
    pub fn new(meta: ReplicaMeta, client_factory: Arc<F>) -> Self {
        Self {
            meta,
            stop_signal: AtomicOption::empty(),
            client_factory,
        }
    }

    fn send_stop_signal(&self) -> Result<(), ReplicatorError> {
        if let Some(sender) = self.stop_signal.take(Ordering::SeqCst) {
            sender.send(()).map_err(|()| {
                error!("failed to send stop signal");
                ReplicatorError::Canceled
            })
        } else {
            Err(ReplicatorError::AlreadyEnded)
        }
    }
}

impl<F: RedisClientFactory> ReplicaReplicator for RedisReplicaReplicator<F> {
    fn start(&self) -> Box<Future<Item = (), Error = ReplicatorError> + Send> {
        let (sender, receiver) = oneshot::channel();
        if self
            .stop_signal
            .try_store(Box::new(sender), Ordering::SeqCst)
            .is_some()
        {
            return Box::new(future::err(ReplicatorError::AlreadyStarted));
        }

        // Just get the first one.
        let master_node_address = match self.meta.masters.get(0) {
            Some(repl_meta) => &repl_meta.node_address,
            None => {
                error!("No master for replica {}", self.meta.replica_node_address);
                return Box::new(future::ok(()));
            }
        };

        let address = match revolve_first_address(master_node_address) {
            Some(address) => address,
            None => return Box::new(future::err(ReplicatorError::InvalidAddress)),
        };
        let host = address.ip().to_string();
        let port = address.port().to_string();

        let client_fut = self
            .client_factory
            .create_client(self.meta.replica_node_address.clone());
        let interval = Duration::new(5, 0);
        let cmd = vec!["SLAVEOF".to_string(), host, port];
        let send_fut = client_fut.and_then(move |client| keep_sending_cmd(client, cmd, interval));

        let meta = self.meta.clone();

        Box::new(
            receiver
                .map_err(|_| ReplicatorError::Canceled)
                .select(send_fut.map_err(ReplicatorError::RedisError))
                .then(move |_| {
                    warn!("RedisReplicaReplicator {:?} stopped", meta);
                    future::ok(())
                }),
        )
    }

    fn stop(&self) -> Box<Future<Item = (), Error = ReplicatorError> + Send> {
        Box::new(future::result(self.send_stop_signal()))
    }

    fn start_importing(&self) -> Box<Future<Item = (), Error = ReplicatorError> + Send> {
        // TODO: implement migration
        Box::new(future::ok(()))
    }

    fn commit_importing(&self) -> Box<Future<Item = (), Error = ReplicatorError> + Send> {
        // TODO: implement migration
        Box::new(future::ok(()))
    }

    fn get_meta(&self) -> &ReplicaMeta {
        &self.meta
    }
}

// Make sure the future will end.
impl<F: RedisClientFactory> Drop for RedisReplicaReplicator<F> {
    fn drop(&mut self) {
        self.send_stop_signal().unwrap_or(())
    }
}

fn keep_sending_cmd<C: RedisClient>(
    client: C,
    cmd: Vec<String>,
    interval: Duration,
) -> impl Future<Item = (), Error = RedisClientError> {
    let infinite_stream = stream::iter_ok(iter::repeat(()));
    infinite_stream
        .fold(client, move |client, ()| {
            let byte_cmd = cmd.iter().map(|s| s.clone().into_bytes()).collect();
            let delay = Delay::new(interval).map_err(RedisClientError::Io);
            // debug!("sending repl cmd {:?}", cmd);
            let exec_fut = client
                .execute(byte_cmd)
                .map_err(|e| {
                    error!("failed to send: {}", e);
                    e
                })
                .map(|(client, response)| {
                    // debug!("replicator get response {:?}", response);
                    if let Resp::Error(err) = response {
                        let err_str = str::from_utf8(&err)
                            .map(ToString::to_string)
                            .unwrap_or_else(|_| format!("{:?}", err));
                        error!("error reply: {}", err_str);
                    }
                    client
                });
            exec_fut.join(delay).map(move |(client, ())| client)
        })
        .map(|_| ())
}
