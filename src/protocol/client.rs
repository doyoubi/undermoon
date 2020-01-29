use super::resp::{BinSafeStr, RespVec};
use crate::common::utils::{revolve_first_address, ThreadSafe};
use crate::protocol::RespCodec;
use atomic_option::AtomicOption;
use chashmap::CHashMap;
use crossbeam_channel;
use futures::{Future, SinkExt, StreamExt};
use std::error::Error;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time;
use tokio_util::codec::{Decoder, Framed};

pub trait RedisClient: Send {
    fn execute<'s>(
        &'s mut self,
        command: Vec<BinSafeStr>,
    ) -> Pin<Box<dyn Future<Output = Result<RespVec, RedisClientError>> + Send + 's>>;
}

pub trait RedisClientFactory: ThreadSafe {
    type Client: RedisClient;

    fn create_client<'s>(
        &'s self,
        address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Client, RedisClientError>> + Send + 's>>;
}

#[derive(Debug)]
struct RedisClientConnection {
    sock: TcpStream,
}

impl From<RedisClientConnection> for TcpStream {
    fn from(conn: RedisClientConnection) -> Self {
        conn.sock
    }
}

#[derive(Debug)]
struct Pool<T> {
    sender: Arc<crossbeam_channel::Sender<T>>,
    receiver: crossbeam_channel::Receiver<T>,
}

impl<T> Pool<T> {
    fn new(capacity: usize) -> Self {
        let (sender, receiver) = crossbeam_channel::bounded(capacity);
        Self {
            sender: Arc::new(sender),
            receiver,
        }
    }

    fn get(&self) -> Result<Option<PoolItemHandle<T>>, ()> {
        match self.receiver.try_recv() {
            Ok(item) => Ok(Some(PoolItemHandle {
                item,
                reclaim_sender: self.get_reclaim_sender(),
            })),
            Err(crossbeam_channel::TryRecvError::Empty) => Ok(None),
            Err(crossbeam_channel::TryRecvError::Disconnected) => Err(()),
        }
    }

    fn get_reclaim_sender(&self) -> Arc<crossbeam_channel::Sender<T>> {
        self.sender.clone()
    }
}

#[derive(Debug)]
struct PoolItemHandle<T> {
    item: T,
    reclaim_sender: Arc<crossbeam_channel::Sender<T>>,
}

type ClientCodec = RespCodec<Vec<BinSafeStr>, RespVec>;
type RawConnHandle = PoolItemHandle<RedisClientConnection>;

struct RedisClientConnectionHandle {
    frame: Framed<TcpStream, ClientCodec>,
    reclaim_sender: Arc<crossbeam_channel::Sender<RedisClientConnection>>,
}

#[derive(Debug)]
pub struct PooledRedisClient {
    conn_handle: Option<RedisClientConnectionHandle>,
    timeout: Duration,
    err: bool,
}

impl PooledRedisClient {
    fn new(conn_handle: RedisClientConnectionHandle, timeout: Duration) -> Self {
        Self {
            conn_handle: Some(conn_handle),
            timeout,
            err: false,
        }
    }

    async fn execute_cmd(&mut self, command: Vec<BinSafeStr>) -> Result<RespVec, RedisClientError> {
        let frame = match &mut self.conn_handle {
            Some(RedisClientConnectionHandle { frame, .. }) => frame,
            None => {
                // Should not reach here. self.conn should only get taken in drop.
                return Err(RedisClientError::Closed);
            }
        };

        frame.send(command).await.map_err(RedisClientError::Io)?;
        match frame.next().await {
            Some(Ok(resp)) => Ok(resp),
            Some(Err(err)) => {
                warn!("redis client failed to get reply: {:?}", err);
                Err(RedisClientError::InvalidReply)
            }
            None => Err(RedisClientError::Closed),
        }
    }

    async fn execute_cmd_with_timeout(
        &mut self,
        command: Vec<BinSafeStr>,
    ) -> Result<RespVec, RedisClientError> {
        let exec_cut = self.execute_cmd(command);
        let r = match time::timeout(self.timeout, exec_cut).await {
            Err(err) => {
                warn!("redis client timeout: {:?}", err);
                Err(RedisClientError::Timeout)
            }
            Ok(Err(err)) => Err(err),
            Ok(Ok(resp)) => Ok(resp),
        };
        if r.is_err() {
            self.err = true;
        }
        r
    }
}

impl ThreadSafe for PooledRedisClient {}

impl Drop for PooledRedisClient {
    fn drop(&mut self) {
        if self.err {
            return;
        }
        if let Some(conn_handle) = self.conn_handle.take() {
            let RedisClientConnectionHandle {
                frame,
                reclaim_sender,
            } = conn_handle;
            match reclaim_sender.try_send(frame.into_innter()) {
                Ok(()) => (),
                Err(crossbeam_channel::TrySendError::Full(_)) => debug!("pool is full"),
                Err(crossbeam_channel::TrySendError::Disconnected(_)) => debug!("pool is down"),
            }
        }
    }
}

impl RedisClient for PooledRedisClient {
    fn execute<'s>(
        &'s mut self,
        command: Vec<BinSafeStr>,
    ) -> Pin<Box<dyn Future<Output = Result<RespVec, RedisClientError>> + Send + 's>> {
        Box::pin(self.execute_cmd_with_timeout(command))
    }
}

pub struct PooledRedisClientFactory {
    capacity: usize,
    // TODO: need to cleanup unused pools.
    pool_map: CHashMap<String, Pool<RedisClientConnection>>,
    timeout: Duration,
}

impl ThreadSafe for PooledRedisClientFactory {}

impl PooledRedisClientFactory {
    pub fn new(capacity: usize, timeout: Duration) -> Self {
        Self {
            capacity,
            pool_map: CHashMap::new(),
            timeout,
        }
    }

    async fn create_conn(
        &self,
        address: String,
    ) -> Result<RedisClientConnection, RedisClientError> {
        let sock_address = match revolve_first_address(&address) {
            Some(address) => address,
            None => return Err(RedisClientError::InvalidAddress),
        };
        let sock = match TcpStream::connect(&sock_address).await {
            Ok(conn) => conn,
            Err(io_err) => return Err(RedisClientError::Io(io_err)),
        };
        Ok(RedisClientConnection { sock })
    }

    async fn create_client_impl(
        &self,
        address: String,
    ) -> Result<PooledRedisClient, RedisClientError> {
        let mut existing_conn: Option<RawConnHandle> = None;
        let new_reclaim_sender: AtomicOption<Arc<_>> = AtomicOption::empty();

        self.pool_map.upsert(
            address.clone(),
            || {
                let pool = Pool::new(self.capacity);
                new_reclaim_sender
                    .replace(Some(Box::new(pool.get_reclaim_sender())), Ordering::SeqCst);
                pool
            },
            |pool| {
                match pool.get() {
                    Ok(Some(conn_handle)) => {
                        existing_conn = Some(conn_handle);
                        return;
                    }
                    Ok(None) => (),
                    Err(()) => *pool = Pool::new(self.capacity),
                };
                new_reclaim_sender
                    .replace(Some(Box::new(pool.get_reclaim_sender())), Ordering::SeqCst);
            },
        );

        if let Some(conn_handle) = existing_conn.take() {
            let PoolItemHandle {
                item,
                reclaim_sender,
            } = conn_handle;
            let conn_handle = RedisClientConnectionHandle {
                frame: ClientCodec::default().framed(item.into()),
                reclaim_sender,
            };
            return Ok(PooledRedisClient::new(conn_handle, self.timeout));
        }

        let timeout = self.timeout;

        match new_reclaim_sender.take(Ordering::SeqCst) {
            Some(reclaim_sender) => {
                let conn_fut = self.create_conn(address);
                match time::timeout(timeout, conn_fut).await {
                    Err(err) => {
                        warn!("create connection timeout: {:?}", err);
                        Err(RedisClientError::Timeout)
                    }
                    Ok(Err(err)) => {
                        warn!("failed to create connection: {:?}", err);
                        Err(err)
                    }
                    Ok(Ok(conn)) => {
                        let conn_handle = RedisClientConnectionHandle {
                            frame: ClientCodec::default().framed(conn.into()),
                            reclaim_sender: *reclaim_sender,
                        };
                        Ok(PooledRedisClient::new(conn_handle, timeout))
                    }
                }
            }
            None => {
                error!("invalid state, can't get the reclaim_sender");
                Err(RedisClientError::InitError)
            }
        }
    }
}

impl RedisClientFactory for PooledRedisClientFactory {
    type Client = PooledRedisClient;

    fn create_client<'s>(
        &'s self,
        address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Client, RedisClientError>> + Send + 's>> {
        Box::pin(self.create_client_impl(address))
    }
}

#[derive(Debug)]
pub enum RedisClientError {
    Io(io::Error),
    Timeout,
    InvalidReply,
    InvalidAddress,
    InitError,
    Closed,
    Done,
    Canceled,
    EncodeError,
}

impl fmt::Display for RedisClientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for RedisClientError {
    fn description(&self) -> &str {
        "client error"
    }

    fn cause(&self) -> Option<&dyn Error> {
        match self {
            RedisClientError::Io(err) => Some(err),
            _ => None,
        }
    }
}
