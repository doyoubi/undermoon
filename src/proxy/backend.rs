use super::command::{CommandError, CommandResult};
use super::service::ServerProxyConfig;
use super::slowlog::TaskEvent;
use crate::common::batch::TryChunksTimeoutStreamExt;
use crate::common::track::TrackedFutureRegistry;
use crate::common::utils::{gen_moved, get_slot, resolve_first_address, ThreadSafe};
use crate::protocol::{
    new_simple_packet_codec, DecodeError, EncodeError, EncodedPacket, FromResp, MonoPacket,
    OptionalMulti, Packet, Resp, RespCodec, RespVec,
};
use futures::channel::mpsc;
use futures::{select, stream, Future, FutureExt, Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use futures_timer::Delay;
use std::boxed::Box;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::result::Result;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio;
use tokio::net::TcpStream;
use tokio_util::codec::Decoder;

pub type BackendResult<T> = Result<T, BackendError>;
pub type CmdTaskResult = Result<RespVec, CommandError>;

pub trait CmdTaskResultHandler: Send + Sync + 'static {
    type Task: CmdTask;

    fn handle_task(
        &self,
        cmd_task: Self::Task,
        result: BackendResult<<Self::Task as CmdTask>::Pkt>,
    );
}

pub trait CmdTaskResultHandlerFactory: ThreadSafe {
    type Handler: CmdTaskResultHandler;

    fn create(&self) -> Self::Handler;
}

pub trait CmdTask: ThreadSafe {
    type Pkt: Packet + Send;

    fn get_key(&self) -> Option<&[u8]>;
    fn set_result(self, result: CommandResult<Self::Pkt>);
    fn get_packet(&self) -> Self::Pkt;

    fn set_resp_result(self, result: Result<RespVec, CommandError>)
    where
        Self: Sized;

    fn log_event(&self, event: TaskEvent);
}

pub trait CmdTaskFactory {
    type Task: CmdTask;

    fn create_with(
        &self,
        // TODO: make it an general context
        another_task: &Self::Task,
        resp: RespVec,
    ) -> (
        Self::Task,
        // TODO: return indexed resp
        Pin<Box<dyn Future<Output = CmdTaskResult> + Send + 'static>>,
    );
}

// Type alias can't work with trait bound so we need to define again,
// or we can use OptionalMulti.
pub enum ReqTask<T: CmdTask> {
    Simple(T),
    Multi(Vec<T>),
}

impl<T: CmdTask> From<T> for ReqTask<T> {
    fn from(t: T) -> ReqTask<T> {
        ReqTask::Simple(t)
    }
}

impl<T: CmdTask> CmdTask for ReqTask<T> {
    type Pkt = OptionalMulti<T::Pkt>;

    fn get_key(&self) -> Option<&[u8]> {
        match self {
            Self::Simple(t) => t.get_key(),
            Self::Multi(v) => {
                for t in v.iter() {
                    let opt = t.get_key();
                    if opt.is_some() {
                        return opt;
                    }
                }
                None
            }
        }
    }

    fn set_result(self, result: CommandResult<Self::Pkt>) {
        match self {
            Self::Simple(t) => match result {
                Ok(res) => match *res {
                    OptionalMulti::Single(r) => t.set_result(Ok(Box::new(r))),
                    OptionalMulti::Multi(_) => t.set_result(Err(CommandError::InnerError)),
                },
                Err(err) => t.set_result(Err(err)),
            },
            Self::Multi(v) => match result {
                Err(err) => {
                    for t in v.into_iter() {
                        t.set_result(Err(err.clone()))
                    }
                }
                Ok(res) => match *res {
                    OptionalMulti::Single(_) => {
                        for t in v {
                            t.set_result(Err(CommandError::InnerError))
                        }
                    }
                    OptionalMulti::Multi(results) => {
                        if v.len() != results.len() {
                            for t in v.into_iter() {
                                t.set_result(Err(CommandError::InnerError));
                            }
                            return;
                        }
                        for (t, r) in v.into_iter().zip(results) {
                            t.set_result(Ok(Box::new(r)));
                        }
                    }
                },
            },
        }
    }

    fn get_packet(&self) -> Self::Pkt {
        match self {
            Self::Simple(t) => OptionalMulti::Single(t.get_packet()),
            Self::Multi(v) => {
                let packets = v.iter().map(|t| t.get_packet()).collect();
                OptionalMulti::Multi(packets)
            }
        }
    }

    fn set_resp_result(self, result: Result<RespVec, CommandError>)
    where
        Self: Sized,
    {
        let hint = self.get_packet().get_hint();
        self.set_result(result.map(|resp| Box::new(Self::Pkt::from_resp(resp, hint))))
    }

    fn log_event(&self, event: TaskEvent) {
        match self {
            Self::Simple(t) => t.log_event(event),
            Self::Multi(v) => {
                for t in v.iter() {
                    t.log_event(event);
                }
            }
        }
    }
}

pub trait CmdTaskSender {
    type Task: CmdTask;

    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError>;
}

pub trait CmdTaskSenderFactory {
    type Sender: CmdTaskSender;

    fn create(&self, address: String) -> Self::Sender;
}

pub struct RecoverableBackendNode<F: CmdTaskResultHandlerFactory> {
    address: String,
    node: BackendNode<<F as CmdTaskResultHandlerFactory>::Handler>,
}

pub struct RecoverableBackendNodeFactory<F: CmdTaskResultHandlerFactory, CF: ConnFactory>
where
    <F::Handler as CmdTaskResultHandler>::Task: CmdTask<Pkt = CF::Pkt>,
    CF::Pkt: Send,
{
    config: Arc<ServerProxyConfig>,
    handler_factory: Arc<F>,
    conn_factory: Arc<CF>,
    future_registry: Arc<TrackedFutureRegistry>,
}

impl<F: CmdTaskResultHandlerFactory, CF: ConnFactory> RecoverableBackendNodeFactory<F, CF>
where
    <F::Handler as CmdTaskResultHandler>::Task: CmdTask<Pkt = CF::Pkt>,
    CF::Pkt: Send,
{
    pub fn new(
        config: Arc<ServerProxyConfig>,
        handler_factory: Arc<F>,
        conn_factory: Arc<CF>,
        future_registry: Arc<TrackedFutureRegistry>,
    ) -> Self {
        Self {
            config,
            handler_factory,
            conn_factory,
            future_registry,
        }
    }
}

impl<F: CmdTaskResultHandlerFactory, CF: ConnFactory> CmdTaskSenderFactory
    for RecoverableBackendNodeFactory<F, CF>
where
    <F::Handler as CmdTaskResultHandler>::Task: CmdTask<Pkt = CF::Pkt>,
    CF::Pkt: Send,
{
    type Sender = RecoverableBackendNode<F>;

    fn create(&self, address: String) -> Self::Sender {
        let (node, fut) = BackendNode::new(
            address.clone(),
            Arc::new(self.handler_factory.create()),
            self.config.clone(),
            self.conn_factory.clone(),
        );
        let desc = format!("backend::RecoverableBackendNode: address={}", address);
        let fut = TrackedFutureRegistry::wrap(self.future_registry.clone(), fut, desc);
        tokio::spawn(fut);
        Self::Sender { address, node }
    }
}

impl<F: CmdTaskResultHandlerFactory> CmdTaskSender for RecoverableBackendNode<F> {
    type Task = <<F as CmdTaskResultHandlerFactory>::Handler as CmdTaskResultHandler>::Task;

    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError> {
        self.node.send(cmd_task).map_err(|e| {
            let cmd_task = e.into_inner();
            cmd_task.set_resp_result(Ok(Resp::Error(
                format!("backend connection failed: {}", self.address).into_bytes(),
            )));
            error!("backend node is closed");
            BackendError::Canceled
        })
    }
}

#[derive(Debug)]
pub struct BackendSendError<T>(T);

impl<T> BackendSendError<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

pub struct BackendNode<H: CmdTaskResultHandler> {
    tx: mpsc::UnboundedSender<H::Task>,
    conn_failed: Arc<AtomicBool>,
}

impl<H: CmdTaskResultHandler> BackendNode<H> {
    pub fn new<CF>(
        address: String,
        handler: Arc<H>,
        config: Arc<ServerProxyConfig>,
        conn_factory: Arc<CF>,
    ) -> (
        BackendNode<H>,
        impl Future<Output = Result<(), BackendError>> + Send,
    )
    where
        CF: ConnFactory + Send + Sync + 'static,
        CF::Pkt: Send,
        <H as CmdTaskResultHandler>::Task: CmdTask<Pkt = CF::Pkt>,
    {
        let (tx, rx) = mpsc::unbounded();
        let conn_failed = Arc::new(AtomicBool::new(true));
        let handle_backend_fut = handle_backend(
            handler,
            rx,
            conn_failed.clone(),
            address,
            config.backend_batch_min_time,
            config.backend_batch_max_time,
            config.backend_batch_buf,
            conn_factory,
        );
        (Self { tx, conn_failed }, handle_backend_fut)
    }

    pub fn send(&self, cmd_task: H::Task) -> Result<(), BackendSendError<H::Task>> {
        cmd_task.log_event(TaskEvent::SentToWritingQueue);
        if self.conn_failed.load(Ordering::SeqCst) {
            return Err(BackendSendError(cmd_task));
        }
        self.tx
            .unbounded_send(cmd_task)
            .map(|_| ())
            .map_err(|e| BackendSendError(e.into_inner()))
    }

    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

type ConnSink<T> = Pin<Box<dyn Sink<T, Error = BackendError> + Send>>;
type ConnStream<T> = Pin<Box<dyn Stream<Item = Result<T, BackendError>> + Send>>;
type CreateConnResult<T> = Result<(ConnSink<T>, ConnStream<T>), BackendError>;

pub trait ConnFactory: ThreadSafe {
    type Pkt: Packet;

    fn create_conn(
        &self,
        addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = CreateConnResult<Self::Pkt>> + Send>>;
}

pub struct DefaultConnFactory<P>(PhantomData<P>);

impl<P> Default for DefaultConnFactory<P> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<P: MonoPacket> ConnFactory for DefaultConnFactory<P> {
    type Pkt = P;

    fn create_conn(
        &self,
        addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = CreateConnResult<Self::Pkt>> + Send>> {
        Box::pin(create_conn(addr))
    }
}

async fn create_conn<T>(address: SocketAddr) -> CreateConnResult<T>
where
    T: MonoPacket,
{
    let socket = match TcpStream::connect(address).await {
        Ok(socket) => socket,
        Err(err) => {
            error!("failed to connect: {:?}", err);
            return Err(BackendError::Io(err));
        }
    };

    let (encoder, decoder) = new_simple_packet_codec::<T, T>();

    let frame = RespCodec::new(encoder, decoder).framed(socket);
    let (writer, reader) = frame.split();
    let writer = writer.sink_map_err(|e| match e {
        EncodeError::Io(err) => BackendError::Io(err),
        EncodeError::NotReady(_) => BackendError::InvalidState,
    });
    let reader = reader.map_err(|e| match e {
        DecodeError::InvalidProtocol => {
            error!("backend: invalid protocol");
            BackendError::InvalidProtocol
        }
        DecodeError::Io(e) => {
            error!("backend: io error: {:?}", e);
            BackendError::Io(e)
        }
    });

    Ok((Box::pin(writer), Box::pin(reader)))
}

const MAX_BACKEND_RETRY: usize = 3;

struct RetryState<T: CmdTask> {
    retry_times: usize,
    tasks: Vec<T>,
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_backend<H, F>(
    handler: Arc<H>,
    task_receiver: mpsc::UnboundedReceiver<H::Task>,
    conn_failed: Arc<AtomicBool>,
    address: String,
    backend_batch_min_time: usize,
    backend_batch_max_time: usize,
    backend_batch_buf: NonZeroUsize,
    conn_factory: Arc<F>,
) -> Result<(), BackendError>
where
    H: CmdTaskResultHandler,
    F: ConnFactory<Pkt = <H::Task as CmdTask>::Pkt> + Send + Sync + 'static,
{
    // TODO: move this to upper layer.
    let sock_address = match resolve_first_address(&address) {
        Some(addr) => addr,
        None => {
            error!("invalid address: {:?}", address);
            return Err(BackendError::InvalidAddress);
        }
    };

    let mut retry_state: Option<RetryState<H::Task>> = None;

    let batch_min_time = Duration::from_nanos(backend_batch_min_time as u64);
    let batch_max_time = Duration::from_nanos(backend_batch_max_time as u64);
    let mut task_receiver = task_receiver
        .try_chunks_timeout(backend_batch_buf, batch_min_time, batch_max_time)
        .fuse();

    loop {
        conn_failed.store(true, Ordering::SeqCst);
        let (writer, reader) = match conn_factory.create_conn(sock_address).await {
            Ok(conn) => conn,
            Err(err) => {
                error!("failed to connect: {:?}", err);
                retry_state.take();

                let mut timeout_fut = Delay::new(Duration::from_secs(1)).fuse();
                loop {
                    let mut tasks_fut = task_receiver.next().fuse();
                    let tasks_opt = select! {
                        () = timeout_fut => break,
                        tasks_opt = tasks_fut => tasks_opt,
                    };
                    let tasks = match tasks_opt {
                        Some(tasks) => tasks,
                        None => break,
                    };
                    for task in tasks.into_iter() {
                        task.set_resp_result(Ok(Resp::Error(
                            format!("failed to connect to {}", address).into_bytes(),
                        )))
                    }
                }
                continue;
            }
        };
        conn_failed.store(false, Ordering::SeqCst);

        let res = handle_conn(
            writer,
            reader,
            &mut task_receiver,
            handler.clone(),
            backend_batch_buf,
            retry_state.take(),
        )
        .await;
        match res {
            Ok(()) => {
                error!("task receiver is closed");
                return Err(BackendError::Canceled);
            }
            Err((err, state)) => {
                error!("connection is closed: {:?}", err);
                retry_state = state;
                continue;
            }
        }
    }
}

async fn handle_conn<H, S>(
    mut writer: ConnSink<<<H as CmdTaskResultHandler>::Task as CmdTask>::Pkt>,
    mut reader: ConnStream<<<H as CmdTaskResultHandler>::Task as CmdTask>::Pkt>,
    task_receiver: &mut S,
    handler: Arc<H>,
    backend_batch_buf: NonZeroUsize,
    mut retry_state_opt: Option<RetryState<H::Task>>,
) -> Result<(), (BackendError, Option<RetryState<H::Task>>)>
where
    H: CmdTaskResultHandler,
    S: Stream<Item = Vec<H::Task>> + Unpin,
{
    let mut packets = Vec::with_capacity(backend_batch_buf.get());

    loop {
        let (retry_times_opt, tasks) = match retry_state_opt.take() {
            Some(RetryState { retry_times, tasks }) => (Some(retry_times), tasks),
            None => {
                let tasks = match task_receiver.next().await {
                    Some(tasks) => tasks,
                    None => return Ok(()),
                };
                (None, tasks)
            }
        };

        for task in &tasks {
            task.log_event(TaskEvent::WritingQueueReceived);
            packets.push(task.get_packet());
        }

        let mut batch = stream::iter(packets.drain(..)).map(Ok);
        let res = writer.send_all(&mut batch).await;

        for task in tasks.iter() {
            task.log_event(TaskEvent::SentToBackend);
        }

        if let Err(err) = res {
            error!("backend write error: {}", err);
            let retry_state = handle_conn_err(retry_times_opt, tasks, &err);
            return Err((err, retry_state));
        }

        let mut tasks_iter = tasks.into_iter();
        // `while let` will consume ownership.
        #[allow(clippy::while_let_loop)]
        loop {
            let task = match tasks_iter.next() {
                Some(task) => task,
                None => break,
            };
            let packet_res = match reader.next().await {
                Some(pkt) => pkt,
                None => {
                    error!("Failed to read packet. Connection is closed.");
                    let mut failed_tasks = vec![task];
                    failed_tasks.extend(tasks_iter);
                    let err = BackendError::Io(io::Error::from(io::ErrorKind::BrokenPipe));
                    let retry_state = handle_conn_err(retry_times_opt, failed_tasks, &err);
                    return Err((err, retry_state));
                }
            };

            task.log_event(TaskEvent::ReceivedFromBackend);
            handler.handle_task(task, packet_res);
        }
    }
}

fn handle_conn_err<T: CmdTask>(
    retry_times_opt: Option<usize>,
    tasks: Vec<T>,
    err: &BackendError,
) -> Option<RetryState<T>> {
    let retry_times = retry_times_opt.unwrap_or(0);
    if retry_times >= MAX_BACKEND_RETRY {
        for task in tasks.into_iter() {
            let cmd_err = match err {
                BackendError::Io(e) => CommandError::Io(io::Error::from(e.kind())),
                others => {
                    error!("unexpected backend error: {:?}", others);
                    CommandError::InnerError
                }
            };
            task.set_result(Err(cmd_err));
        }
        None
    } else {
        let retry_state = RetryState {
            retry_times: retry_times + 1,
            tasks,
        };
        Some(retry_state)
    }
}

#[derive(Debug)]
pub enum BackendError {
    Io(io::Error),
    NodeNotFound,
    InvalidProtocol,
    InvalidAddress,
    Canceled,
    InvalidState,
}

impl fmt::Display for BackendError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for BackendError {
    fn description(&self) -> &str {
        "backend error"
    }

    fn cause(&self) -> Option<&dyn Error> {
        match self {
            BackendError::Io(err) => Some(err),
            _ => None,
        }
    }
}

pub struct ReqAdaptorSender<S: CmdTaskSender> {
    sender: S,
}

pub struct ReqAdaptorSenderFactory<F: CmdTaskSenderFactory> {
    inner_factory: F,
}

impl<F: CmdTaskSenderFactory> ReqAdaptorSenderFactory<F> {
    pub fn new(inner_factory: F) -> Self {
        Self { inner_factory }
    }
}

impl<S: CmdTaskSender> ReqAdaptorSender<S> {
    pub fn new(sender: S) -> Self {
        Self { sender }
    }

    pub fn inner_sender(&self) -> &S {
        &self.sender
    }
}

impl<S: CmdTaskSender> CmdTaskSender for ReqAdaptorSender<S> {
    type Task = ReqTask<S::Task>;

    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError> {
        match cmd_task {
            ReqTask::Simple(t) => self.sender.send(t),
            ReqTask::Multi(ts) => {
                for t in ts.into_iter() {
                    self.sender.send(t)?;
                }
                Ok(())
            }
        }
    }
}

impl<F: CmdTaskSenderFactory> CmdTaskSenderFactory for ReqAdaptorSenderFactory<F> {
    type Sender = ReqAdaptorSender<F::Sender>;

    fn create(&self, address: String) -> Self::Sender {
        let sender = self.inner_factory.create(address);
        Self::Sender { sender }
    }
}

// Round robin sender.
pub struct RRSenderGroup<S: CmdTaskSender> {
    senders: Vec<S>,
    cursor: AtomicUsize,
}

pub struct RRSenderGroupFactory<F: CmdTaskSenderFactory> {
    group_size: NonZeroUsize,
    inner_factory: F,
}

impl<F: CmdTaskSenderFactory> RRSenderGroupFactory<F> {
    pub fn new(group_size: NonZeroUsize, inner_factory: F) -> Self {
        Self {
            group_size,
            inner_factory,
        }
    }
}

impl<F: CmdTaskSenderFactory> CmdTaskSenderFactory for RRSenderGroupFactory<F> {
    type Sender = RRSenderGroup<F::Sender>;

    fn create(&self, address: String) -> Self::Sender {
        let mut senders = Vec::new();
        for _ in 0..self.group_size.get() {
            senders.push(self.inner_factory.create(address.clone()));
        }
        Self::Sender {
            senders,
            cursor: AtomicUsize::new(0),
        }
    }
}

impl<S: CmdTaskSender> CmdTaskSender for RRSenderGroup<S> {
    type Task = S::Task;

    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError> {
        let index = self.cursor.fetch_add(1, Ordering::SeqCst);
        let sender = match self.senders.get(index % self.senders.len()) {
            Some(s) => s,
            None => return Err(BackendError::NodeNotFound),
        };
        sender.send(cmd_task)
    }
}

pub struct CachedSender<S: CmdTaskSender> {
    inner_sender: Arc<S>,
}

// TODO: support cleanup here to avoid memory leak.
pub struct CachedSenderFactory<F: CmdTaskSenderFactory> {
    inner_factory: F,
    cached_senders: Arc<RwLock<HashMap<String, Arc<F::Sender>>>>,
}

impl<F: CmdTaskSenderFactory> CachedSenderFactory<F> {
    pub fn new(inner_factory: F) -> Self {
        Self {
            inner_factory,
            cached_senders: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl<F: CmdTaskSenderFactory> CmdTaskSenderFactory for CachedSenderFactory<F> {
    type Sender = CachedSender<F::Sender>;

    fn create(&self, address: String) -> Self::Sender {
        if let Some(sender) = self
            .cached_senders
            .read()
            .expect("CachedSenderFactory::create")
            .get(&address)
        {
            return CachedSender {
                inner_sender: sender.clone(),
            };
        }

        // Acceptable race condition here. Multiple threads might be creating at the same time.
        let inner_sender = Arc::new(self.inner_factory.create(address.clone()));
        let inner_sender_clone = {
            let mut guard = self
                .cached_senders
                .write()
                .expect("CachedSenderFactory::create");
            guard.entry(address).or_insert(inner_sender).clone()
        };

        CachedSender {
            inner_sender: inner_sender_clone,
        }
    }
}

impl<S: CmdTaskSender> CmdTaskSender for CachedSender<S> {
    type Task = S::Task;

    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError> {
        self.inner_sender.send(cmd_task)
    }
}

pub struct RedirectionSender<T: CmdTask> {
    redirection_address: String,
    phantom: PhantomData<T>,
}

impl<T: CmdTask> CmdTaskSender for RedirectionSender<T> {
    type Task = T;

    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError> {
        let key = match cmd_task.get_key() {
            Some(key) => key,
            None => {
                let resp = Resp::Error("missing key".to_string().into_bytes());
                cmd_task.set_resp_result(Ok(resp));
                return Ok(());
            }
        };
        let resp =
            Resp::Error(gen_moved(get_slot(key), self.redirection_address.clone()).into_bytes());
        cmd_task.set_resp_result(Ok(resp));
        Ok(())
    }
}

pub struct RedirectionSenderFactory<T: CmdTask>(PhantomData<T>);

impl<T: CmdTask> Default for RedirectionSenderFactory<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T: CmdTask> CmdTaskSenderFactory for RedirectionSenderFactory<T> {
    type Sender = RedirectionSender<T>;

    fn create(&self, address: String) -> Self::Sender {
        RedirectionSender {
            redirection_address: address,
            phantom: PhantomData,
        }
    }
}

pub type BackendSenderFactory<F, CF> =
    CachedSenderFactory<RRSenderGroupFactory<RecoverableBackendNodeFactory<F, CF>>>;

pub fn gen_sender_factory<F: CmdTaskResultHandlerFactory, CF: ConnFactory>(
    config: Arc<ServerProxyConfig>,
    reply_handler_factory: Arc<F>,
    conn_factory: Arc<CF>,
    future_registry: Arc<TrackedFutureRegistry>,
) -> BackendSenderFactory<F, CF>
where
    <F::Handler as CmdTaskResultHandler>::Task: CmdTask<Pkt = CF::Pkt>,
    CF::Pkt: Send,
{
    CachedSenderFactory::new(RRSenderGroupFactory::new(
        config.backend_conn_num,
        RecoverableBackendNodeFactory::new(
            config.clone(),
            reply_handler_factory,
            conn_factory,
            future_registry,
        ),
    ))
}

pub type MigrationBackendSenderFactory<F, CF> = CachedSenderFactory<
    RRSenderGroupFactory<ReqAdaptorSenderFactory<RecoverableBackendNodeFactory<F, CF>>>,
>;

pub fn gen_migration_sender_factory<F: CmdTaskResultHandlerFactory, CF: ConnFactory>(
    config: Arc<ServerProxyConfig>,
    reply_handler_factory: Arc<F>,
    conn_factory: Arc<CF>,
    future_registry: Arc<TrackedFutureRegistry>,
) -> MigrationBackendSenderFactory<F, CF>
where
    <F::Handler as CmdTaskResultHandler>::Task: CmdTask<Pkt = CF::Pkt>,
    CF::Pkt: Send,
{
    CachedSenderFactory::new(RRSenderGroupFactory::new(
        config.backend_conn_num,
        ReqAdaptorSenderFactory::new(RecoverableBackendNodeFactory::new(
            config.clone(),
            reply_handler_factory,
            conn_factory,
            future_registry,
        )),
    ))
}
