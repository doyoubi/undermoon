use super::command::{CmdType, CommandError, CommandResult, DataCmdType};
use super::service::ServerProxyConfig;
use super::slowlog::{Slowlog, TaskEvent};
use crate::common::batching::Chunks;
use common::future_group::new_future_group;
use common::utils::{gen_moved, get_slot, revolve_first_address, ThreadSafe};
use futures::sync::mpsc;
use futures::Sink;
use futures::{future, stream, Future, Stream};
use protocol::{DecodeError, Resp, RespCodec, RespPacket, RespSlice, RespVec};
use std::boxed::Box;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::result::Result;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio;
use tokio::codec::Decoder;
use tokio::net::TcpStream;

pub type BackendResult = Result<Box<RespPacket>, BackendError>;

pub trait CmdTaskResultHandler: Send + 'static {
    type Task: CmdTask;

    fn handle_task(&self, cmd_task: Self::Task, result: BackendResult);
}

pub trait CmdTaskResultHandlerFactory: ThreadSafe {
    type Handler: CmdTaskResultHandler;

    fn create(&self) -> Self::Handler;
}

pub trait CmdTask: ThreadSafe + fmt::Debug {
    fn get_key(&self) -> Option<&[u8]>;
    fn get_resp_slice(&self) -> RespSlice;
    fn get_cmd_type(&self) -> CmdType;
    fn get_data_cmd_type(&self) -> DataCmdType;
    fn set_result(self, result: CommandResult);
    fn get_packet(&self) -> RespPacket;

    fn set_resp_result(self, result: Result<RespVec, CommandError>)
    where
        Self: Sized,
    {
        self.set_result(result.map(|resp| Box::new(RespPacket::from_resp_vec(resp))))
    }

    fn get_slowlog(&self) -> &Slowlog;
}

pub trait CmdTaskFactory {
    type Task: CmdTask;

    fn create_with(
        &self,
        another_task: &Self::Task,
        resp: RespVec,
    ) -> (
        Self::Task,
        Box<dyn Future<Item = RespVec, Error = CommandError> + Send>,
    );
}

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
    addr: Arc<String>,
    node: Arc<RwLock<Option<BackendNode<<F as CmdTaskResultHandlerFactory>::Handler>>>>,
    config: Arc<ServerProxyConfig>,
    handler_factory: Arc<F>,
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
        Self::Sender {
            addr: Arc::new(address),
            node: Arc::new(RwLock::new(None)),
            config: self.config.clone(),
            handler_factory: self.handler_factory.clone(),
        }
    }
}

impl<F: CmdTaskResultHandlerFactory> CmdTaskSender for RecoverableBackendNode<F> {
    type Task = <<F as CmdTaskResultHandlerFactory>::Handler as CmdTaskResultHandler>::Task;

    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError> {
        let retry_times = 3;
        let mut cmd_task = cmd_task;

        for _ in 0..retry_times {
            let need_init = self.node.read().unwrap().is_none();
            // Race condition here. Multiple threads might be creating new connection at the same time.
            // Maybe it's just fine. If not, lock the creating connection phrase.
            if need_init {
                let node_arc = self.node.clone();
                let address = match revolve_first_address(&self.addr) {
                    Some(address) => address,
                    None => return Err(BackendError::InvalidAddress),
                };

                let config = self.config.clone();
                let handler = Box::new(self.handler_factory.create());

                let sock = TcpStream::connect(&address);
                let fut = sock.then(move |res| {
                    debug!("sock result: {:?}", res);
                    match res {
                        Ok(sock) => {
                            let (node, reader_handler, writer_handler) =
                                BackendNode::<F::Handler>::new(sock, handler, config.clone());
                            let (reader_handler, writer_handler) =
                                new_future_group(reader_handler, writer_handler);

                            let (spawn_new, res) = {
                                let mut guard = node_arc.write().unwrap();
                                let empty = guard.is_none();
                                let inner_node = guard.get_or_insert(node);
                                (
                                    empty,
                                    inner_node
                                        .send(cmd_task)
                                        .map_err(|_e| BackendError::Canceled),
                                )
                            };

                            if let Err(e) = res {
                                error!("failed to forward cmd {:?}", e);
                            }

                            if spawn_new {
                                tokio::spawn(
                                    reader_handler
                                        .map(|()| error!("backend read IO closed"))
                                        .map_err(|e| error!("backend read IO error {:?}", e)),
                                );
                                tokio::spawn(
                                    writer_handler
                                        .map(|()| error!("backend write IO closed"))
                                        .map_err(|e| error!("backend write IO error {:?}", e)),
                                );
                            }
                            future::ok(())
                        }
                        Err(e) => {
                            error!("sock err: {:?}", e);
                            cmd_task.set_result(Err(CommandError::Io(io::Error::from(e.kind()))));
                            future::err(())
                        }
                    }
                });
                // If this future fails, cmd_task will be lost. Let itself send back an error response.
                tokio::spawn(fut);
                return Ok(());
            }

            let res = match self.node.read().unwrap().as_ref() {
                Some(n) => n.send(cmd_task),
                None => {
                    cmd_task.set_result(Err(CommandError::InnerError));
                    return Err(BackendError::NodeNotFound);
                }
            };
            cmd_task = match res {
                Ok(()) => return Ok(()),
                Err(e) => {
                    // if it fails, remove this connection.
                    {
                        let mut node = self.node.write().unwrap();
                        if let Some(true) = node.as_ref().map(BackendNode::is_closed) {
                            node.take();
                        }
                    }
                    error!("reset backend connection {}", *self.addr);
                    e.into_inner()
                }
            };
        }

        Err(BackendError::Canceled)
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
    pub fn new(
        sock: TcpStream,
        handler: Box<H>,
        config: Arc<ServerProxyConfig>,
    ) -> (
        BackendNode<H>,
        impl Future<Item = (), Error = BackendError> + Send,
        impl Future<Item = (), Error = BackendError> + Send,
    ) {
        let (tx, rx) = mpsc::unbounded();
        let (reader_handler, writer_handler) = handle_backend(
            handler,
            rx,
            sock,
            config.backend_channel_size,
            config.backend_batch_min_time,
            config.backend_batch_max_time,
            config.backend_batch_buf,
        );
        (Self { tx }, reader_handler, writer_handler)
    }

    pub fn send(&self, cmd_task: H::Task) -> Result<(), BackendSendError<H::Task>> {
        cmd_task
            .get_slowlog()
            .log_event(TaskEvent::SentToWritingQueue);
        self.tx
            .unbounded_send(cmd_task)
            .map(|_| ())
            .map_err(|e| BackendSendError(e.into_inner()))
    }

    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

pub fn handle_backend<H>(
    handler: Box<H>,
    task_receiver: mpsc::UnboundedReceiver<H::Task>,
    sock: TcpStream,
    channel_size: usize,
    backend_batch_min_time: usize,
    backend_batch_max_time: usize,
    backend_batch_buf: usize,
) -> (
    impl Future<Item = (), Error = BackendError> + Send,
    impl Future<Item = (), Error = BackendError> + Send,
)
where
    H: CmdTaskResultHandler,
{
    let (writer, reader) = RespCodec {}.framed(sock).split();

    let (tx, rx) = mpsc::channel(channel_size);

    let batch_min_time = Duration::from_nanos(backend_batch_min_time as u64);
    let batch_max_time = Duration::from_nanos(backend_batch_max_time as u64);
    let task_receiver = Chunks::new(
        task_receiver,
        backend_batch_buf,
        batch_min_time,
        batch_max_time,
    )
    .map_err(|_| ());

    let writer_handler = handle_write(task_receiver, writer, tx);
    let reader_handler = handle_read(handler, reader, rx);

    // May need to use future_group. The tx <=> rx between them may not be able to shutdown futures.
    (reader_handler, writer_handler)
}

fn handle_write<S, W, T>(
    task_receiver: S,
    writer: W,
    tx: mpsc::Sender<T>,
) -> impl Future<Item = (), Error = BackendError> + Send
where
    S: Stream<Item = Vec<T>, Error = ()> + Send + 'static,
    W: Sink<SinkItem = Box<RespPacket>, SinkError = io::Error> + Send + 'static,
    T: CmdTask,
{
    task_receiver
        .map_err(|()| BackendError::Canceled)
        .fold((writer, tx), |(writer, tx), tasks| {
            for task in tasks.iter() {
                task.get_slowlog()
                    .log_event(TaskEvent::WritingQueueReceived);
            }

            let items: Vec<Box<RespPacket>> = tasks
                .iter()
                .map(|task| Box::new(task.get_packet()))
                .collect();

            writer
                .send_all(stream::iter_ok::<_, io::Error>(items))
                .then(move |res| {
                    for task in tasks.iter() {
                        task.get_slowlog().log_event(TaskEvent::SentToBackend);
                    }

                    let fut: Box<dyn Future<Item = _, Error = BackendError> + Send> = match res {
                        Ok((writer, empty_stream)) => {
                            debug_assert!(empty_stream
                                .collect()
                                .wait()
                                .expect("invalid empty_stream")
                                .is_empty());
                            let fut = tx
                                .send_all(stream::iter_ok(tasks))
                                .map(move |(tx, empty_stream)| {
                                    debug_assert!(empty_stream
                                        .collect()
                                        .wait()
                                        .expect("invalid empty_stream")
                                        .is_empty());
                                    (writer, tx)
                                })
                                .map_err(|e| {
                                    error!("backend handle_write rx closed {:?}", e);
                                    BackendError::Canceled
                                });
                            Box::new(fut)
                        }
                        Err(e) => {
                            error!("Failed to write");
                            for task in tasks {
                                task.set_result(Err(CommandError::Io(io::Error::from(e.kind()))));
                            }
                            Box::new(future::err(BackendError::Io(e)))
                        }
                    };
                    fut
                })
        })
        .map(|_| ())
}

fn handle_read<H, R>(
    handler: Box<H>,
    reader: R,
    rx: mpsc::Receiver<H::Task>,
) -> impl Future<Item = (), Error = BackendError> + Send
where
    R: Stream<Item = Box<RespPacket>, Error = DecodeError> + Send + 'static,
    H: CmdTaskResultHandler,
{
    let rx = rx.into_future();
    reader
        .map_err(|e| match e {
            DecodeError::InvalidProtocol => {
                error!("backend: invalid protocol");
                BackendError::InvalidProtocol
            }
            DecodeError::Io(e) => {
                error!("backend: io error: {:?}", e);
                BackendError::Io(e)
            }
        })
        .fold((handler, rx), move |(handler, rx), packet| {
            rx.map_err(|((), _receiver)| {
                // The remaining tasks in _receiver might leak here
                // It's up to the tasks inside Receiver to gracefully drop themselves in destructor.
                error!("backend: unexpected read");
                BackendError::Canceled
            })
            .and_then(|(task_opt, rx)| match task_opt {
                Some(task) => {
                    task.get_slowlog().log_event(TaskEvent::ReceivedFromBackend);
                    handler.handle_task(task, Ok(packet));
                    future::ok((handler, rx.into_future()))
                }
                None => future::err(BackendError::Canceled),
            })
        })
        .map(|_| ())
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
