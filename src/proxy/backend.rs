use super::command::{CommandError, CommandResult};
use super::service::ServerProxyConfig;
use super::slowlog::{Slowlog, TaskEvent};
use bytes::BytesMut;
use common::future_group::new_future_group;
use common::utils::{gen_moved, get_key, get_slot, revolve_first_address, ThreadSafe};
use futures::sync::mpsc;
use futures::Sink;
use futures::{future, Future, Stream};
use protocol::{Array, DecodeError, Resp, RespCodec, RespPacket};
use std::boxed::Box;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::result::Result;
use std::sync;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio;
use tokio::codec::Decoder;
use tokio::net::TcpStream;

pub type BackendResult = Result<Resp, BackendError>;

pub trait ReplyHandler<T: CmdTask>: Send + 'static {
    fn handle_reply(&self, cmd_task: T, result: BackendResult);
}

pub trait CmdTask: ThreadSafe + fmt::Debug {
    fn get_resp(&self) -> &Resp;
    fn set_result(self, result: CommandResult);
    fn drain_packet_data(&self) -> Option<BytesMut>;

    fn set_resp_result(self, result: Result<Resp, CommandError>)
    where
        Self: Sized,
    {
        self.set_result(result.map(|resp| Box::new(RespPacket::new(resp))))
    }

    fn get_slowlog(&self) -> &Slowlog;
}

pub trait CmdTaskSender {
    type Task: CmdTask;

    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError>;
}

pub trait CmdTaskSenderFactory {
    type Sender: CmdTaskSender;

    fn create(&self, address: String) -> Self::Sender;
}

// TODO: change to use AtomicOption
pub struct RecoverableBackendNode<T: CmdTask> {
    addr: sync::Arc<String>,
    node: sync::Arc<sync::RwLock<Option<BackendNode<T>>>>,
    config: sync::Arc<ServerProxyConfig>,
}

pub struct RecoverableBackendNodeFactory<T: CmdTask> {
    phantom: PhantomData<T>,
    config: sync::Arc<ServerProxyConfig>,
}

impl<T: CmdTask> RecoverableBackendNodeFactory<T> {
    pub fn new(config: sync::Arc<ServerProxyConfig>) -> Self {
        Self {
            phantom: PhantomData,
            config,
        }
    }
}

impl<T: CmdTask> CmdTaskSenderFactory for RecoverableBackendNodeFactory<T> {
    type Sender = RecoverableBackendNode<T>;

    fn create(&self, address: String) -> Self::Sender {
        Self::Sender {
            addr: sync::Arc::new(address),
            node: sync::Arc::new(sync::RwLock::new(None)),
            config: self.config.clone(),
        }
    }
}

impl<T: CmdTask> CmdTaskSender for RecoverableBackendNode<T> {
    type Task = T;

    fn send(&self, cmd_task: T) -> Result<(), BackendError> {
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

            let sock = TcpStream::connect(&address);
            let fut = sock.then(move |res| {
                debug!("sock result: {:?}", res);
                match res {
                    Ok(sock) => {
                        let (node, reader_handler, writer_handler) =
                            BackendNode::<T>::new(sock, ReplyCommitHandler {}, config.clone());
                        let (reader_handler, writer_handler) =
                            new_future_group(reader_handler, writer_handler);

                        let (spawn_new, res) = {
                            let mut guard = node_arc.write().unwrap();
                            let empty = guard.is_none();
                            let inner_node = guard.get_or_insert(node);
                            (empty, inner_node.send(cmd_task))
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
        match res {
            Ok(()) => Ok(()),
            Err(e) => {
                // if it fails, remove this connection.
                self.node.write().unwrap().take();
                error!("reset backend connecton {:?}", e);
                Err(e)
            }
        }
    }
}

// TODO: Remove this. Use handler to do some statistics.
pub struct ReplyCommitHandler {}

impl<T: CmdTask> ReplyHandler<T> for ReplyCommitHandler {
    fn handle_reply(&self, _cmd_task: T, _result: BackendResult) {}
}

pub struct BackendNode<T: CmdTask> {
    tx: mpsc::UnboundedSender<T>,
}

impl<T: CmdTask> BackendNode<T> {
    pub fn new<H: ReplyHandler<T>>(
        sock: TcpStream,
        handler: H,
        config: sync::Arc<ServerProxyConfig>,
    ) -> (
        BackendNode<T>,
        impl Future<Item = (), Error = BackendError> + Send,
        impl Future<Item = (), Error = BackendError> + Send,
    ) {
        let (tx, rx) = mpsc::unbounded();
        let (reader_handler, writer_handler) =
            handle_backend(handler, rx, sock, config.backend_channel_size);
        (Self { tx }, reader_handler, writer_handler)
    }

    pub fn send(&self, cmd_task: T) -> Result<(), BackendError> {
        cmd_task
            .get_slowlog()
            .log_event(TaskEvent::SentToWritingQueue);
        self.tx
            .unbounded_send(cmd_task)
            .map(|_| ())
            .map_err(|_e| BackendError::Canceled)
    }
}

pub fn handle_backend<H, T>(
    handler: H,
    task_receiver: mpsc::UnboundedReceiver<T>,
    sock: TcpStream,
    channel_size: usize,
) -> (
    impl Future<Item = (), Error = BackendError> + Send,
    impl Future<Item = (), Error = BackendError> + Send,
)
where
    H: ReplyHandler<T>,
    T: CmdTask,
{
    let (writer, reader) = RespCodec {}.framed(sock).split();

    let (tx, rx) = mpsc::channel(channel_size);

    let writer_handler = handle_write(task_receiver, writer, tx);
    let reader_handler = handle_read(handler, reader, rx);

    (reader_handler, writer_handler)
}

fn handle_write<W, T>(
    task_receiver: mpsc::UnboundedReceiver<T>,
    writer: W,
    tx: mpsc::Sender<T>,
) -> impl Future<Item = (), Error = BackendError> + Send
where
    W: Sink<SinkItem = Box<RespPacket>, SinkError = io::Error> + Send + 'static,
    T: CmdTask,
{
    task_receiver
        .map_err(|()| BackendError::Canceled)
        .fold((writer, tx), |(writer, tx), task| {
            task.get_slowlog()
                .log_event(TaskEvent::WritingQueueReceived);

            let item = match task.drain_packet_data() {
                Some(data) => {
                    // Tricky code here. The nil array will be ignored when encoded.
                    // TODO: Refactor it by using enum to differentiate this two packet type.
                    RespPacket::new_with_buf(Resp::Arr(Array::Nil), data)
                }
                None => {
                    // TODO: remove the clone
                    let resp = task.get_resp().clone();
                    RespPacket::new(resp)
                }
            };
            writer.send(Box::new(item)).then(|res| {
                task.get_slowlog().log_event(TaskEvent::SentToBackend);

                let fut: Box<Future<Item = _, Error = BackendError> + Send> = match res {
                    Ok(writer) => {
                        let fut = tx.send(task).map(move |tx| (writer, tx)).map_err(|e| {
                            error!("backend handle_write rx closed {:?}", e);
                            BackendError::Canceled
                        });
                        Box::new(fut)
                    }
                    Err(e) => {
                        error!("Failed to write");
                        task.set_result(Err(CommandError::Io(io::Error::from(e.kind()))));
                        Box::new(future::err(BackendError::Io(e)))
                    }
                };
                fut
            })
        })
        .map(|_| ())
}

fn handle_read<H, T, R>(
    handler: H,
    reader: R,
    rx: mpsc::Receiver<T>,
) -> impl Future<Item = (), Error = BackendError> + Send
where
    R: Stream<Item = Box<RespPacket>, Error = DecodeError> + Send + 'static,
    H: ReplyHandler<T>,
    T: CmdTask,
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
                    task.set_result(Ok(packet));
                    // TODO: call handler
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

    fn cause(&self) -> Option<&Error> {
        match self {
            BackendError::Io(err) => Some(err),
            _ => None,
        }
    }
}

const DEFAULT_GROUP_SIZE: usize = 4;

pub struct RRSenderGroup<S: CmdTaskSender> {
    senders: Vec<S>,
    cursor: AtomicUsize,
}

pub struct RRSenderGroupFactory<F: CmdTaskSenderFactory> {
    inner_factory: F,
}

impl<F: CmdTaskSenderFactory> RRSenderGroupFactory<F> {
    pub fn new(inner_factory: F) -> Self {
        Self { inner_factory }
    }
}

impl<F: CmdTaskSenderFactory> CmdTaskSenderFactory for RRSenderGroupFactory<F> {
    type Sender = RRSenderGroup<F::Sender>;

    fn create(&self, address: String) -> Self::Sender {
        let mut senders = Vec::new();
        for _ in 0..DEFAULT_GROUP_SIZE {
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
    inner_sender: sync::Arc<S>,
}

// TODO: support cleanup here to avoid memory leak.
pub struct CachedSenderFactory<F: CmdTaskSenderFactory> {
    inner_factory: F,
    cached_senders: sync::Arc<sync::RwLock<HashMap<String, sync::Arc<F::Sender>>>>,
}

impl<F: CmdTaskSenderFactory> CachedSenderFactory<F> {
    pub fn new(inner_factory: F) -> Self {
        Self {
            inner_factory,
            cached_senders: sync::Arc::new(sync::RwLock::new(HashMap::new())),
        }
    }
}

impl<F: CmdTaskSenderFactory> CmdTaskSenderFactory for CachedSenderFactory<F> {
    type Sender = CachedSender<F::Sender>;

    fn create(&self, address: String) -> Self::Sender {
        if let Some(sender) = self.cached_senders.read().unwrap().get(&address) {
            return CachedSender {
                inner_sender: sender.clone(),
            };
        }

        // Acceptable race condition here. Multiple threads might be creating at the same time.
        let inner_sender = sync::Arc::new(self.inner_factory.create(address.clone()));
        let inner_sender_clone = {
            let mut guard = self.cached_senders.write().unwrap();
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
        let key = match get_key(cmd_task.get_resp()) {
            Some(key) => key,
            None => {
                let resp = Resp::Error("missing key".to_string().into_bytes());
                cmd_task.set_resp_result(Ok(resp));
                return Ok(());
            }
        };
        let resp =
            Resp::Error(gen_moved(get_slot(&key), self.redirection_address.clone()).into_bytes());
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
