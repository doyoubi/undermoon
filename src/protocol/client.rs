use super::resp::{BinSafeStr, RespVec};
use crate::common::utils::{resolve_first_address, ThreadSafe};
use crate::protocol::{
    new_optional_multi_packet_codec, EncodeError, OptionalMulti, OptionalMultiPacketDecoder,
    OptionalMultiPacketEncoder, RespCodec,
};
use crossbeam_channel;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use either::Either;
use futures::{Future, SinkExt, StreamExt};
use mockall::automock;
use std::error::Error;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time;
use tokio_util::codec::{Decoder, Framed};

#[allow(clippy::ptr_arg)]
mod client_trait {
    use super::*;

    #[automock]
    pub trait RedisClient: Send {
        fn execute_single<'s>(
            &'s mut self,
            command: Vec<BinSafeStr>,
        ) -> Pin<Box<dyn Future<Output = Result<RespVec, RedisClientError>> + Send + 's>> {
            let fut = async move {
                match self.execute(OptionalMulti::Single(command)).await {
                    Ok(opt_mul_resp) => match opt_mul_resp {
                        OptionalMulti::Single(t) => Ok(t),
                        OptionalMulti::Multi(v) => {
                            error!("PooledRedisClient::execute expected single result, found multi: {:?}", v);
                            Err(RedisClientError::InvalidState)
                        }
                    },
                    Err(err) => Err(err),
                }
            };
            Box::pin(fut)
        }

        fn execute_multi<'s>(
            &'s mut self,
            commands: Vec<Vec<BinSafeStr>>,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<RespVec>, RedisClientError>> + Send + 's>>
        {
            let fut = async move {
                match self.execute(OptionalMulti::Multi(commands)).await {
                    Ok(opt_mul_resp) => match opt_mul_resp {
                        OptionalMulti::Single(t) => {
                            error!("PooledRedisClient::execute expected single result, found multi: {:?}", t);
                            Err(RedisClientError::InvalidState)
                        }
                        OptionalMulti::Multi(v) => Ok(v),
                    },
                    Err(err) => Err(err),
                }
            };
            Box::pin(fut)
        }

        fn execute<'s>(
            &'s mut self,
            command: OptionalMulti<Vec<BinSafeStr>>,
        ) -> Pin<
            Box<dyn Future<Output = Result<OptionalMulti<RespVec>, RedisClientError>> + Send + 's>,
        >;
    }
}

pub use client_trait::RedisClient;

pub trait RedisClientFactory: ThreadSafe {
    type Client: RedisClient;

    fn create_client<'s>(
        &'s self,
        address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Client, RedisClientError>> + Send + 's>>;
}

// For unit tests.
pub struct DummyRedisClientFactory<C: RedisClient + Clone + Sync + 'static> {
    client: C,
}

impl<C: RedisClient + Clone + Sync + 'static> RedisClientFactory for DummyRedisClientFactory<C> {
    type Client = C;

    fn create_client<'s>(
        &'s self,
        _address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Client, RedisClientError>> + Send + 's>> {
        let client = self.client.clone();
        Box::pin(async move { Ok(client) })
    }
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

type ClientCodec =
    RespCodec<OptionalMultiPacketEncoder<Vec<BinSafeStr>>, OptionalMultiPacketDecoder<RespVec>>;

struct RedisClientConnectionHandle {
    frame: Framed<TcpStream, ClientCodec>,
    reclaim_sender: Arc<crossbeam_channel::Sender<RedisClientConnection>>,
}

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

    async fn execute_cmd(
        &mut self,
        command: OptionalMulti<Vec<BinSafeStr>>,
    ) -> Result<OptionalMulti<RespVec>, RedisClientError> {
        let frame = match &mut self.conn_handle {
            Some(RedisClientConnectionHandle { frame, .. }) => frame,
            None => {
                // Should not reach here. self.conn should only get taken in drop.
                return Err(RedisClientError::Closed);
            }
        };

        frame.send(command).await.map_err(|err| match err {
            EncodeError::Io(err) => RedisClientError::Io(err),
            EncodeError::NotReady(_) => RedisClientError::InvalidState,
        })?;
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
        command: OptionalMulti<Vec<BinSafeStr>>,
    ) -> Result<OptionalMulti<RespVec>, RedisClientError> {
        let timeout = self.timeout;
        let exec_cut = self.execute_cmd(command);
        let r = match time::timeout(timeout, exec_cut).await {
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
            let conn = RedisClientConnection {
                sock: frame.into_inner(),
            };
            match reclaim_sender.try_send(conn) {
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
        command: OptionalMulti<Vec<BinSafeStr>>,
    ) -> Pin<Box<dyn Future<Output = Result<OptionalMulti<RespVec>, RedisClientError>> + Send + 's>>
    {
        Box::pin(self.execute_cmd_with_timeout(command))
    }
}

pub struct PooledRedisClientFactory {
    capacity: usize,
    // TODO: need to cleanup unused pools.
    pool_map: DashMap<String, Pool<RedisClientConnection>>,
    timeout: Duration,
}

impl PooledRedisClientFactory {
    pub fn new(capacity: usize, timeout: Duration) -> Self {
        Self {
            capacity,
            pool_map: DashMap::new(),
            timeout,
        }
    }

    async fn create_conn(
        &self,
        address: String,
    ) -> Result<RedisClientConnection, RedisClientError> {
        let sock_address = match resolve_first_address(&address) {
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
        let state = match self.pool_map.entry(address.clone()) {
            Entry::Occupied(mut entry) => {
                let pool = entry.get_mut();
                match pool.get() {
                    Ok(Some(conn_handle)) => Either::Left(conn_handle),
                    Ok(None) => Either::Right(pool.get_reclaim_sender()),
                    Err(()) => {
                        *pool = Pool::new(self.capacity);
                        Either::Right(pool.get_reclaim_sender())
                    }
                }
            }
            Entry::Vacant(entry) => {
                let pool = Pool::new(self.capacity);
                let reclaim_sender = pool.get_reclaim_sender();
                entry.insert(pool);
                Either::Right(reclaim_sender)
            }
        };

        let reclaim_sender = match state {
            Either::Left(conn_handle) => {
                let PoolItemHandle {
                    item,
                    reclaim_sender,
                } = conn_handle;
                let (encoder, decoder) = new_optional_multi_packet_codec();
                let conn_handle = RedisClientConnectionHandle {
                    frame: ClientCodec::new(encoder, decoder).framed(item.into()),
                    reclaim_sender,
                };
                return Ok(PooledRedisClient::new(conn_handle, self.timeout));
            }
            Either::Right(reclaim_sender) => reclaim_sender,
        };

        let timeout = self.timeout;

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
                let (encoder, decoder) = new_optional_multi_packet_codec();
                let conn_handle = RedisClientConnectionHandle {
                    frame: ClientCodec::new(encoder, decoder).framed(conn.into()),
                    reclaim_sender,
                };
                Ok(PooledRedisClient::new(conn_handle, timeout))
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
    InvalidState,
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
