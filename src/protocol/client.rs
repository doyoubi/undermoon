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

// Suppress warning from automock.
#[allow(clippy::ptr_arg)]
#[allow(clippy::indexing_slicing)]
mod client_trait {
    use super::*;
    use futures::FutureExt;

    #[automock]
    pub trait RedisClient: Send {
        fn execute_single<'s>(
            &'s mut self,
            command: Vec<BinSafeStr>,
        ) -> Pin<Box<dyn Future<Output = Result<RespVec, RedisClientError>> + Send + 's>> {
            Box::pin(
                self.execute(OptionalMulti::Single(command))
                    .map(process_single_cmd_result),
            )
        }

        fn execute_multi<'s>(
            &'s mut self,
            commands: Vec<Vec<BinSafeStr>>,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<RespVec>, RedisClientError>> + Send + 's>>
        {
            let commands_num = commands.len();
            Box::pin(
                self.execute(OptionalMulti::Multi(commands))
                    .map(move |r| process_multi_cmd_result(r, commands_num)),
            )
        }

        fn execute<'s>(
            &'s mut self,
            command: OptionalMulti<Vec<BinSafeStr>>,
        ) -> Pin<
            Box<dyn Future<Output = Result<OptionalMulti<RespVec>, RedisClientError>> + Send + 's>,
        >;
    }
}

pub use client_trait::{MockRedisClient, RedisClient};

fn process_single_cmd_result(
    result: Result<OptionalMulti<RespVec>, RedisClientError>,
) -> Result<RespVec, RedisClientError> {
    match result {
        Ok(opt_mul_resp) => match opt_mul_resp {
            OptionalMulti::Single(t) => Ok(t),
            OptionalMulti::Multi(v) => {
                error!(
                    "PooledRedisClient::execute expected single result, found multi: {:?}",
                    v
                );
                Err(RedisClientError::InvalidState)
            }
        },
        Err(err) => Err(err),
    }
}

fn process_multi_cmd_result(
    result: Result<OptionalMulti<RespVec>, RedisClientError>,
    commands_num: usize,
) -> Result<Vec<RespVec>, RedisClientError> {
    match result {
        Ok(opt_mul_resp) => match opt_mul_resp {
            OptionalMulti::Single(t) => {
                error!(
                    "PooledRedisClient::execute expected single result, found multi: {:?}",
                    t
                );
                Err(RedisClientError::InvalidState)
            }
            OptionalMulti::Multi(v) => {
                if v.len() != commands_num {
                    error!(
                        "PooledRedisClient::execute reply number {}, found {} {:?}",
                        commands_num,
                        v.len(),
                        v
                    );
                }
                Ok(v)
            }
        },
        Err(err) => Err(err),
    }
}

pub trait RedisClientFactory: ThreadSafe {
    type Client: RedisClient;

    fn create_client<'s>(
        &'s self,
        address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Client, RedisClientError>> + Send + 's>>;
}

// For unit tests.
pub struct DummyRedisClientFactory<C, F>
where
    C: RedisClient + Sync + 'static,
    F: Fn() -> C + ThreadSafe,
{
    create_func: F,
}

impl<C, F> DummyRedisClientFactory<C, F>
where
    C: RedisClient + Sync + 'static,
    F: Fn() -> C + ThreadSafe,
{
    pub fn new(create_func: F) -> Self {
        Self { create_func }
    }
}

impl<C, F> RedisClientFactory for DummyRedisClientFactory<C, F>
where
    C: RedisClient + Sync + 'static,
    F: Fn() -> C + ThreadSafe,
{
    type Client = C;

    fn create_client<'s>(
        &'s self,
        _address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Client, RedisClientError>> + Send + 's>> {
        let client = (self.create_func)();
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
    simple_client: Option<SimpleRedisClient>,
    reclaim_sender: Arc<crossbeam_channel::Sender<RedisClientConnection>>,
    err: bool,
}

impl PooledRedisClient {
    fn new(conn_handle: RedisClientConnectionHandle, timeout: Duration) -> Self {
        let RedisClientConnectionHandle {
            frame,
            reclaim_sender,
        } = conn_handle;
        let simple_client = SimpleRedisClient::new(frame, timeout);
        Self {
            simple_client: Some(simple_client),
            reclaim_sender,
            err: false,
        }
    }

    async fn execute_cmd_with_timeout(
        &mut self,
        command: OptionalMulti<Vec<BinSafeStr>>,
    ) -> Result<OptionalMulti<RespVec>, RedisClientError> {
        let simple_client = match &mut self.simple_client {
            Some(client) => client,
            None => {
                error!("Invalid state, conn should be taken in drop function");
                return Err(RedisClientError::Closed);
            }
        };

        simple_client.execute_cmd_with_timeout(command).await
    }

    async fn execute_cmd_with_err_guard(
        &mut self,
        command: OptionalMulti<Vec<BinSafeStr>>,
    ) -> Result<OptionalMulti<RespVec>, RedisClientError> {
        if self.err {
            return Err(RedisClientError::StaleClient);
        }
        // If we only set this error later,
        // there's a corner case which could result in getting an old response from the last `execute`:
        // - The client send the request successfully.
        // - Before receiving the reply, the whole future is canceled.
        // - the error is not set and there's no error log.
        // - The `frame` with the unconsumed response get reclaimed.
        // - The next `execute` will get this old response.
        self.err = true;
        let res = self.execute_cmd_with_timeout(command).await;
        self.err = res.is_err();
        res
    }
}

impl Drop for PooledRedisClient {
    fn drop(&mut self) {
        if self.err {
            return;
        }
        if let Some(client) = self.simple_client.take() {
            let SimpleRedisClient { frame, .. } = client;
            let conn = RedisClientConnection {
                sock: frame.into_inner(),
            };
            match self.reclaim_sender.try_send(conn) {
                Ok(()) => (),
                Err(crossbeam_channel::TrySendError::Full(_)) => debug!("pool is full"),
                Err(crossbeam_channel::TrySendError::Disconnected(_)) => debug!("pool is down"),
            }
        }
    }
}

impl RedisClient for PooledRedisClient {
    fn execute_single<'s>(
        &'s mut self,
        command: Vec<BinSafeStr>,
    ) -> Pin<Box<dyn Future<Output = Result<RespVec, RedisClientError>> + Send + 's>> {
        let fut = async move {
            if self.err {
                return Err(RedisClientError::StaleClient);
            }
            self.err = true;
            let r = self
                .execute_cmd_with_timeout(OptionalMulti::Single(command))
                .await;
            let r = process_single_cmd_result(r);
            self.err = r.is_err();
            r
        };
        Box::pin(fut)
    }

    fn execute_multi<'s>(
        &'s mut self,
        commands: Vec<Vec<BinSafeStr>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RespVec>, RedisClientError>> + Send + 's>> {
        let fut = async move {
            if self.err {
                return Err(RedisClientError::StaleClient);
            }
            let commands_num = commands.len();
            self.err = true;
            let r = self
                .execute_cmd_with_timeout(OptionalMulti::Multi(commands))
                .await;
            let r = process_multi_cmd_result(r, commands_num);
            self.err = r.is_err();
            r
        };
        Box::pin(fut)
    }

    fn execute<'s>(
        &'s mut self,
        command: OptionalMulti<Vec<BinSafeStr>>,
    ) -> Pin<Box<dyn Future<Output = Result<OptionalMulti<RespVec>, RedisClientError>> + Send + 's>>
    {
        Box::pin(self.execute_cmd_with_err_guard(command))
    }
}

pub struct PooledRedisClientFactory {
    capacity: usize,
    // TODO: need to cleanup unused pools.
    pool_map: DashMap<String, Pool<RedisClientConnection>>,
    timeout: Duration,
    simple_factory: SimpleRedisClientFactory,
}

impl PooledRedisClientFactory {
    pub fn new(capacity: usize, timeout: Duration) -> Self {
        Self {
            capacity,
            pool_map: DashMap::new(),
            timeout,
            simple_factory: SimpleRedisClientFactory::new(timeout),
        }
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

        let SimpleRedisClient { frame, timeout } =
            self.simple_factory.create_client_impl(address).await?;
        let conn_handle = RedisClientConnectionHandle {
            frame,
            reclaim_sender,
        };
        Ok(PooledRedisClient::new(conn_handle, timeout))
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

pub struct SimpleRedisClient {
    frame: Framed<TcpStream, ClientCodec>,
    timeout: Duration,
}

impl SimpleRedisClient {
    pub fn new(frame: Framed<TcpStream, ClientCodec>, timeout: Duration) -> Self {
        Self { frame, timeout }
    }

    async fn execute_cmd(
        &mut self,
        command: OptionalMulti<Vec<BinSafeStr>>,
    ) -> Result<OptionalMulti<RespVec>, RedisClientError> {
        self.frame.send(command).await.map_err(|err| match err {
            EncodeError::Io(err) => RedisClientError::Io(err),
            EncodeError::NotReady(_) => {
                error!("Invalid Encoder state");
                RedisClientError::InvalidState
            }
        })?;

        match self.frame.next().await {
            Some(Ok(resp)) => Ok(resp),
            Some(Err(err)) => {
                error!("redis client failed to get reply: {:?}", err);
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
        let exec_fut = self.execute_cmd(command);
        match time::timeout(timeout, exec_fut).await {
            Err(err) => {
                warn!("redis client timeout: {:?}", err);
                Err(RedisClientError::Timeout)
            }
            Ok(Err(err)) => Err(err),
            Ok(Ok(resp)) => Ok(resp),
        }
    }
}

impl RedisClient for SimpleRedisClient {
    fn execute<'s>(
        &'s mut self,
        command: OptionalMulti<Vec<BinSafeStr>>,
    ) -> Pin<Box<dyn Future<Output = Result<OptionalMulti<RespVec>, RedisClientError>> + Send + 's>>
    {
        Box::pin(self.execute_cmd_with_timeout(command))
    }
}

pub struct SimpleRedisClientFactory {
    timeout: Duration,
}

impl SimpleRedisClientFactory {
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    async fn create_conn(&self, address: String) -> Result<TcpStream, RedisClientError> {
        let sock_address = match resolve_first_address(&address) {
            Some(address) => address,
            None => return Err(RedisClientError::InvalidAddress),
        };
        let sock = match TcpStream::connect(&sock_address).await {
            Ok(conn) => conn,
            Err(io_err) => return Err(RedisClientError::Io(io_err)),
        };
        Ok(sock)
    }

    async fn create_client_impl(
        &self,
        address: String,
    ) -> Result<SimpleRedisClient, RedisClientError> {
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
                let frame = ClientCodec::new(encoder, decoder).framed(conn);
                Ok(SimpleRedisClient::new(frame, timeout))
            }
        }
    }
}

impl RedisClientFactory for SimpleRedisClientFactory {
    type Client = SimpleRedisClient;

    fn create_client<'s>(
        &'s self,
        address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Client, RedisClientError>> + Send + 's>> {
        Box::pin(self.create_client_impl(address))
    }
}

pub struct PreCheckRedisClientFactory<F: RedisClientFactory> {
    inner_factory: Arc<F>,
    retry_times: usize,
}

impl<F: RedisClientFactory> PreCheckRedisClientFactory<F> {
    pub fn new(inner_factory: Arc<F>, retry_times: usize) -> Self {
        Self {
            inner_factory,
            retry_times,
        }
    }

    async fn create_client_impl(&self, address: String) -> Result<F::Client, RedisClientError> {
        let mut err_res = Err(RedisClientError::InitError);
        for _ in 0..=self.retry_times {
            let mut client = self.inner_factory.create_client(address.clone()).await?;
            match client.execute_single(vec![b"PING".to_vec()]).await {
                Err(err) => {
                    err_res = Err(err);
                }
                Ok(_) => return Ok(client),
            }
        }
        err_res
    }
}

impl<F: RedisClientFactory> RedisClientFactory for PreCheckRedisClientFactory<F> {
    type Client = F::Client;

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
    StaleClient,
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
