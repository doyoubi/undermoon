use super::command::{CommandError, CommandResult};
use super::service::ServerProxyConfig;
use super::slowlog::TaskEvent;
use crate::common::utils::{gen_moved, get_slot, resolve_first_address, ThreadSafe};
use crate::protocol::{DecodeError, Packet, Resp, RespCodec, RespVec};
use futures::channel::mpsc;
use futures::{stream, Future, Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use futures_batch::ChunksTimeoutStreamExt;
use futures_timer::Delay;
use std::boxed::Box;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::pin::Pin;
use std::result::Result;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio;
use tokio::net::TcpStream;
use tokio_util::codec::Decoder;

pub type BackendResult<T> = Result<T, BackendError>;

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

pub trait CmdTask: ThreadSafe + fmt::Debug {
    type Pkt: Packet + Send;

    fn get_key(&self) -> Option<&[u8]>;
    fn set_result(self, result: CommandResult<Self::Pkt>);
    fn get_packet(&self) -> Self::Pkt;

    fn set_resp_result(self, result: Result<RespVec, CommandError>)
    where
        Self: Sized,
    {
        self.set_result(result.map(|resp| Box::new(Self::Pkt::from(resp))))
    }

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
        Pin<Box<dyn Future<Output = Result<RespVec, CommandError>> + Send + 'static>>,
    );
}

// Type alias can't work with trait bound so we need to define again.
pub enum ReqTask<T: CmdTask> {
    Simple(T),
    Multi(Vec<T>),
}

impl<T: CmdTask> From<T> for ReqTask<T> {
    fn from(t: T) -> ReqTask<T> {
        ReqTask::Simple(t)
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

pub trait ReqTaskSender {
    type Task: CmdTask;

    fn send(&self, cmd_task: ReqTask<Self::Task>) -> Result<(), BackendError>;
}

pub trait ReqTaskSenderFactory {
    type Sender: ReqTaskSender;

    fn create(&self, address: String) -> Self::Sender;
}

// TODO: change to use AtomicOption
pub struct RecoverableBackendNode<F: CmdTaskResultHandlerFactory> {
    node: BackendNode<<F as CmdTaskResultHandlerFactory>::Handler>,
}

impl<F: CmdTaskResultHandlerFactory + ThreadSafe> ThreadSafe for RecoverableBackendNode<F> {}

pub struct RecoverableBackendNodeFactory<F: CmdTaskResultHandlerFactory> {
    config: Arc<ServerProxyConfig>,
    handler_factory: Arc<F>,
}

impl<F: CmdTaskResultHandlerFactory + ThreadSafe> ThreadSafe for RecoverableBackendNodeFactory<F> {}

impl<F: CmdTaskResultHandlerFactory> RecoverableBackendNodeFactory<F> {
    pub fn new(config: Arc<ServerProxyConfig>, handler_factory: Arc<F>) -> Self {
        Self {
            config,
            handler_factory,
        }
    }
}

impl<F: CmdTaskResultHandlerFactory> CmdTaskSenderFactory for RecoverableBackendNodeFactory<F> {
    type Sender = RecoverableBackendNode<F>;

    fn create(&self, address: String) -> Self::Sender {
        let (node, fut) = BackendNode::new(
            address,
            Arc::new(self.handler_factory.create()),
            self.config.clone(),
            |address| Box::pin(create_conn(address)),
        );
        tokio::spawn(fut);
        Self::Sender { node }
    }
}

impl<F: CmdTaskResultHandlerFactory> CmdTaskSender for RecoverableBackendNode<F> {
    type Task = <<F as CmdTaskResultHandlerFactory>::Handler as CmdTaskResultHandler>::Task;

    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError> {
        self.node.send(cmd_task).map_err(|_e| {
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
}

impl<H: CmdTaskResultHandler> BackendNode<H> {
    pub fn new<F>(
        address: String,
        handler: Arc<H>,
        config: Arc<ServerProxyConfig>,
        create_conn: F,
    ) -> (
        BackendNode<H>,
        impl Future<Output = Result<(), BackendError>> + Send,
    )
    where
        F: Fn(
                SocketAddr,
            )
                -> Pin<Box<dyn Future<Output = CreateConnResult<<H::Task as CmdTask>::Pkt>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        let (tx, rx) = mpsc::unbounded();
        let handle_backend_fut = handle_backend(
            handler,
            rx,
            address,
            config.backend_channel_size,
            config.backend_batch_min_time,
            config.backend_batch_max_time,
            config.backend_batch_buf,
            create_conn,
        );
        (Self { tx }, handle_backend_fut)
    }

    pub fn send(&self, cmd_task: H::Task) -> Result<(), BackendSendError<H::Task>> {
        cmd_task.log_event(TaskEvent::SentToWritingQueue);
        self.tx
            .unbounded_send(cmd_task)
            .map(|_| ())
            .map_err(|e| BackendSendError(e.into_inner()))
    }

    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

type ConnSink<T> = Pin<Box<dyn Sink<T, Error = io::Error> + Send>>;
type ConnStream<T> = Pin<Box<dyn Stream<Item = Result<T, BackendError>> + Send>>;
type CreateConnResult<T> = Result<(ConnSink<T>, ConnStream<T>), BackendError>;

async fn create_conn<T: Packet + Send + 'static>(address: SocketAddr) -> CreateConnResult<T> {
    let socket = match TcpStream::connect(address).await {
        Ok(socket) => socket,
        Err(err) => {
            error!("failed to connect: {:?}", err);
            return Err(BackendError::Io(err));
        }
    };

    let frame = RespCodec::default().framed(socket);
    let (writer, reader) = frame.split();
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

pub async fn handle_backend<H, F: Send + 'static>(
    handler: Arc<H>,
    task_receiver: mpsc::UnboundedReceiver<H::Task>,
    address: String,
    _channel_size: usize,
    backend_batch_min_time: usize,
    _backend_batch_max_time: usize,
    backend_batch_buf: usize,
    create_conn: F,
) -> Result<(), BackendError>
where
    H: CmdTaskResultHandler,
    F: Fn(
            SocketAddr,
        )
            -> Pin<Box<dyn Future<Output = CreateConnResult<<H::Task as CmdTask>::Pkt>> + Send>>
        + Send
        + Sync
        + 'static,
{
    // TODO: move this to upper layer.
    let address = match resolve_first_address(&address) {
        Some(addr) => addr,
        None => {
            error!("invalid address: {:?}", address);
            return Err(BackendError::InvalidAddress);
        }
    };

    let batch_min_time = Duration::from_nanos(backend_batch_min_time as u64);
    let mut task_receiver = task_receiver.chunks_timeout(backend_batch_buf, batch_min_time);

    loop {
        let (writer, reader) = match create_conn(address).await {
            Ok(conn) => conn,
            Err(err) => {
                error!("failed to connect: {:?}", err);
                Delay::new(Duration::from_secs(3)).await;
                continue;
            }
        };

        let res = handle_conn(
            writer,
            reader,
            &mut task_receiver,
            handler.clone(),
            backend_batch_buf,
        )
        .await;
        match res {
            Ok(()) => {
                error!("task receiver is closed");
                return Err(BackendError::Canceled);
            }
            Err(err) => {
                error!("connection is closed: {:?}", err);
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
    backend_batch_buf: usize,
) -> Result<(), BackendError>
where
    H: CmdTaskResultHandler,
    S: Stream<Item = Vec<H::Task>> + Unpin,
{
    let mut packets = Vec::with_capacity(backend_batch_buf);

    while let Some(tasks) = task_receiver.next().await {
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
            for task in tasks.into_iter() {
                task.set_result(Err(CommandError::Io(io::Error::from(err.kind()))));
            }
            return Err(BackendError::Io(err));
        }

        for task in tasks.into_iter() {
            let packet_res = match reader.next().await {
                Some(pkt) => pkt,
                None => {
                    // The remaining tasks might leak here
                    // It's up to the tasks inside Receiver to gracefully drop themselves in destructor.
                    error!("Failed to read packet. Connection is closed.");
                    return Err(BackendError::Canceled);
                }
            };

            task.log_event(TaskEvent::ReceivedFromBackend);
            handler.handle_task(task, packet_res);
        }
    }
    Ok(())
}

#[derive(Debug)]
pub enum BackendError {
    Io(io::Error),
    NodeNotFound,
    InvalidProtocol,
    InvalidAddress,
    Canceled,
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

impl<S: CmdTaskSender + ThreadSafe> ThreadSafe for ReqAdaptorSender<S> {}

pub struct ReqAdaptorSenderFactory<F: CmdTaskSenderFactory> {
    inner_factory: F,
}

impl<F: CmdTaskSenderFactory> ReqAdaptorSenderFactory<F> {
    pub fn new(inner_factory: F) -> Self {
        Self { inner_factory }
    }
}

impl<F: CmdTaskSenderFactory + ThreadSafe> ThreadSafe for ReqAdaptorSenderFactory<F> {}

impl<S: CmdTaskSender> ReqTaskSender for ReqAdaptorSender<S> {
    type Task = S::Task;

    fn send(&self, cmd_task: ReqTask<Self::Task>) -> Result<(), BackendError> {
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

impl<F: CmdTaskSenderFactory> ReqTaskSenderFactory for ReqAdaptorSenderFactory<F> {
    type Sender = ReqAdaptorSender<F::Sender>;

    fn create(&self, address: String) -> Self::Sender {
        let sender = self.inner_factory.create(address);
        Self::Sender { sender }
    }
}

// Round robin sender.
pub struct RRSenderGroup<S: ReqTaskSender> {
    senders: Vec<S>,
    cursor: AtomicUsize,
}

impl<S: ReqTaskSender + ThreadSafe> ThreadSafe for RRSenderGroup<S> {}

pub struct RRSenderGroupFactory<F: ReqTaskSenderFactory> {
    group_size: usize,
    inner_factory: F,
}

impl<F: ReqTaskSenderFactory + ThreadSafe> ThreadSafe for RRSenderGroupFactory<F> {}

impl<F: ReqTaskSenderFactory> RRSenderGroupFactory<F> {
    pub fn new(group_size: usize, inner_factory: F) -> Self {
        Self {
            group_size,
            inner_factory,
        }
    }
}

impl<F: ReqTaskSenderFactory> ReqTaskSenderFactory for RRSenderGroupFactory<F> {
    type Sender = RRSenderGroup<F::Sender>;

    fn create(&self, address: String) -> Self::Sender {
        let mut senders = Vec::new();
        for _ in 0..self.group_size {
            senders.push(self.inner_factory.create(address.clone()));
        }
        Self::Sender {
            senders,
            cursor: AtomicUsize::new(0),
        }
    }
}

impl<S: ReqTaskSender> ReqTaskSender for RRSenderGroup<S> {
    type Task = S::Task;

    fn send(&self, cmd_task: ReqTask<Self::Task>) -> Result<(), BackendError> {
        let index = self.cursor.fetch_add(1, Ordering::SeqCst);
        let sender = match self.senders.get(index % self.senders.len()) {
            Some(s) => s,
            None => return Err(BackendError::NodeNotFound),
        };
        sender.send(cmd_task)
    }
}

pub struct CachedSender<S: ReqTaskSender> {
    inner_sender: Arc<S>,
}

impl<S: ReqTaskSender + ThreadSafe> ThreadSafe for CachedSender<S> {}

// TODO: support cleanup here to avoid memory leak.
pub struct CachedSenderFactory<F: ReqTaskSenderFactory> {
    inner_factory: F,
    cached_senders: Arc<RwLock<HashMap<String, Arc<F::Sender>>>>,
}

impl<F> ThreadSafe for CachedSenderFactory<F>
where
    F: ReqTaskSenderFactory + ThreadSafe,
    <F as ReqTaskSenderFactory>::Sender: ThreadSafe,
{
}

impl<F: ReqTaskSenderFactory> CachedSenderFactory<F> {
    pub fn new(inner_factory: F) -> Self {
        Self {
            inner_factory,
            cached_senders: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl<F: ReqTaskSenderFactory> ReqTaskSenderFactory for CachedSenderFactory<F> {
    type Sender = CachedSender<F::Sender>;

    fn create(&self, address: String) -> Self::Sender {
        if let Some(sender) = self.cached_senders.read().unwrap().get(&address) {
            return CachedSender {
                inner_sender: sender.clone(),
            };
        }

        // Acceptable race condition here. Multiple threads might be creating at the same time.
        let inner_sender = Arc::new(self.inner_factory.create(address.clone()));
        let inner_sender_clone = {
            let mut guard = self.cached_senders.write().unwrap();
            guard.entry(address).or_insert(inner_sender).clone()
        };

        CachedSender {
            inner_sender: inner_sender_clone,
        }
    }
}

pub type BackendSenderFactory<F> = CachedSenderFactory<
    RRSenderGroupFactory<ReqAdaptorSenderFactory<RecoverableBackendNodeFactory<F>>>,
>;

pub fn gen_sender_factory<F: CmdTaskResultHandlerFactory>(
    config: Arc<ServerProxyConfig>,
    reply_handler_factory: Arc<F>,
) -> BackendSenderFactory<F> {
    CachedSenderFactory::new(RRSenderGroupFactory::new(
        config.backend_conn_num,
        ReqAdaptorSenderFactory::new(RecoverableBackendNodeFactory::new(
            config.clone(),
            reply_handler_factory,
        )),
    ))
}

impl<S: ReqTaskSender> ReqTaskSender for CachedSender<S> {
    type Task = S::Task;

    fn send(&self, cmd_task: ReqTask<Self::Task>) -> Result<(), BackendError> {
        self.inner_sender.send(cmd_task)
    }
}

pub struct RedirectionSender<T: CmdTask> {
    redirection_address: String,
    phantom: PhantomData<T>,
}

impl<T: CmdTask> ThreadSafe for RedirectionSender<T> {}

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

impl<T: CmdTask> ThreadSafe for RedirectionSenderFactory<T> {}

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
