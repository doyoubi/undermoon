use super::decoder::{decode_resp, DecodeError};
use super::encoder::command_to_buf;
use super::resp::{BinSafeStr, RespVec};
use atomic_option::AtomicOption;
use chashmap::CHashMap;
use common::utils::{revolve_first_address, ThreadSafe};
use crossbeam_channel;
use futures::{future, Future};
use std::error::Error;
use std::fmt;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{write_all, AsyncRead, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::prelude::FutureExt;

// TODO: remove ThreadSafe
pub trait RedisClient: ThreadSafe + Sized + std::fmt::Debug {
    fn execute(
        self,
        command: Vec<BinSafeStr>,
    ) -> Box<dyn Future<Item = (Self, RespVec), Error = RedisClientError> + Send + 'static>;
}

pub trait RedisClientFactory: ThreadSafe {
    type Client: RedisClient;

    fn create_client(
        &self,
        address: String,
    ) -> Box<dyn Future<Item = Self::Client, Error = RedisClientError> + Send + 'static>;
}

#[derive(Debug)]
struct RedisClientConnection {
    reader: io::BufReader<ReadHalf<TcpStream>>,
    writer: WriteHalf<TcpStream>,
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

type RedisClientConnectionHandle = PoolItemHandle<Box<RedisClientConnection>>;

#[derive(Debug)]
pub struct PooledRedisClient {
    conn_handle: Option<RedisClientConnectionHandle>,
    timeout: Duration,
}

impl PooledRedisClient {
    fn new(conn_handle: RedisClientConnectionHandle, timeout: Duration) -> Self {
        Self {
            conn_handle: Some(conn_handle),
            timeout,
        }
    }

    fn destruct(mut self) -> (Option<RedisClientConnectionHandle>, Duration) {
        let handle = self.conn_handle.take();
        let timeout = self.timeout;
        (handle, timeout)
    }
}

impl ThreadSafe for PooledRedisClient {}

impl Drop for PooledRedisClient {
    fn drop(&mut self) {
        if let Some(conn_handle) = self.conn_handle.take() {
            let RedisClientConnectionHandle {
                item: conn,
                reclaim_sender,
            } = conn_handle;
            match reclaim_sender.try_send(conn) {
                Ok(()) => (),
                Err(crossbeam_channel::TrySendError::Full(_)) => debug!("pool is full"),
                Err(crossbeam_channel::TrySendError::Disconnected(_)) => debug!("pool is down"),
            }
        }
    }
}

impl RedisClient for PooledRedisClient {
    fn execute(
        self,
        command: Vec<BinSafeStr>,
    ) -> Box<dyn Future<Item = (Self, RespVec), Error = RedisClientError> + Send + 'static> {
        let (conn_handle, timeout) = self.destruct();
        let timeout_clone = timeout;

        let (conn, reclaim_sender) = match conn_handle {
            Some(PoolItemHandle {
                item,
                reclaim_sender,
            }) => (item, reclaim_sender),
            None => {
                // Should not reach here. self.conn should only get taken in drop.
                return Box::new(future::err(RedisClientError::Closed));
            }
        };

        let mut buf = Vec::new();
        command_to_buf(&mut buf, command);
        let conn = *conn;
        let RedisClientConnection { reader, writer } = conn;

        let exec_fut = write_all(writer, buf)
            .map_err(RedisClientError::Io)
            .and_then(move |(writer, _buf)| {
                decode_resp(reader)
                    .map_err(|e| match e {
                        DecodeError::Io(e) => RedisClientError::Io(e),
                        DecodeError::InvalidProtocol => RedisClientError::InvalidReply,
                    })
                    .map(move |(reader, resp)| {
                        let conn = RedisClientConnection { reader, writer };
                        let conn_handle = RedisClientConnectionHandle {
                            item: Box::new(conn),
                            reclaim_sender,
                        };
                        let client = Self {
                            conn_handle: Some(conn_handle),
                            timeout,
                        };
                        (client, resp)
                    })
            });

        let f = exec_fut.timeout(timeout_clone).map_err(move |err| {
            if err.is_inner() {
                match err.into_inner() {
                    Some(redis_err) => redis_err,
                    None => {
                        debug!("unexpected timeout error");
                        RedisClientError::Timeout
                    }
                }
            } else if err.is_elapsed() {
                error!("redis client timeout error {:?}", err);
                RedisClientError::Timeout
            } else if err.is_timer() {
                error!("redis client timer error {:?}", err);
                RedisClientError::Timeout
            } else {
                error!("redis client unexpected error {:?}", err);
                RedisClientError::Timeout
            }
        });
        Box::new(f)
    }
}

pub struct PooledRedisClientFactory {
    capacity: usize,
    // TODO: need to cleanup unused pools.
    pool_map: CHashMap<String, Pool<Box<RedisClientConnection>>>,
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

    fn create_conn(
        &self,
        address: String,
    ) -> Box<dyn Future<Item = Box<RedisClientConnection>, Error = RedisClientError> + Send + 'static>
    {
        let sock_address = match revolve_first_address(&address) {
            Some(address) => address,
            None => return Box::new(future::err(RedisClientError::InvalidAddress)),
        };
        let conn_fut = TcpStream::connect(&sock_address)
            .map_err(RedisClientError::Io)
            .map(|sock| {
                let (rx, tx) = sock.split();
                let conn = RedisClientConnection {
                    reader: io::BufReader::new(rx),
                    writer: tx,
                };
                Box::new(conn)
            });
        Box::new(conn_fut)
    }
}

impl RedisClientFactory for PooledRedisClientFactory {
    type Client = PooledRedisClient;

    fn create_client(
        &self,
        address: String,
    ) -> Box<dyn Future<Item = Self::Client, Error = RedisClientError> + Send + 'static> {
        let mut existing_conn: Option<RedisClientConnectionHandle> = None;
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
            return Box::new(future::ok(PooledRedisClient::new(
                conn_handle,
                self.timeout,
            )));
        }

        let timeout = self.timeout;

        match new_reclaim_sender.take(Ordering::SeqCst) {
            Some(reclaim_sender) => Box::new(
                self.create_conn(address)
                    .timeout(timeout)
                    .map(move |conn| {
                        let conn_handle = PoolItemHandle {
                            item: conn,
                            reclaim_sender: *reclaim_sender,
                        };
                        PooledRedisClient::new(conn_handle, timeout)
                    })
                    .map_err(|timeout_err| {
                        debug!("connection error: {:?}", timeout_err);
                        RedisClientError::Timeout
                    }),
            ),
            None => {
                error!("invalid state, can't get the reclaim_sender");
                Box::new(future::err(RedisClientError::InitError))
            }
        }
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
