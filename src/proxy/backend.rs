use std::io;
use std::iter;
use std::fmt;
use std::sync;
use std::error::Error;
use std::result::Result;
use std::boxed::Box;
use std::sync::atomic::{AtomicUsize, Ordering};
use bytes::BytesMut;
use futures::{future, Future, stream, Stream};
use futures::sync::mpsc;
use futures::Sink;
use tokio;
use tokio::codec::Decoder;
use tokio::net::TcpStream;
use tokio::io::{write_all, AsyncRead, AsyncWrite};
use common::future_group::new_future_group;
use protocol::{Resp, Array, DecodeError, RespCodec, RespPacket};
use super::command::{CommandError, CommandResult};

pub type BackendResult = Result<Resp, BackendError>;

pub trait ReplyHandler<T: CmdTask> : Send + 'static {
    fn handle_reply(&self, cmd_task: T, result: BackendResult);
}

pub trait CmdTask : Send + 'static + fmt::Debug {
    fn get_resp(&self) -> &Resp;
    fn set_result(self, result: CommandResult);
    fn drain_packet_data(&self) -> Option<BytesMut>;

    fn set_resp_result(self, result: Result<Resp, CommandError>) where Self: Sized {
        self.set_result(result.map(|resp| Box::new(RespPacket::new(resp))))
    }
}

pub trait CmdTaskSender {
    type Task: CmdTask;

    fn new(addr: String) -> Self;
    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError>;
}

// TODO: change to use AtomicOption
pub struct RecoverableBackendNode<T: CmdTask> {
    addr: sync::Arc<String>,
    node: sync::Arc<sync::RwLock<Option<BackendNode<T>>>>,
}

impl<T: CmdTask> CmdTaskSender for RecoverableBackendNode<T> {
    type Task = T;

    fn new(addr: String) -> RecoverableBackendNode<T> {
        Self{
            addr: sync::Arc::new(addr),
            node: sync::Arc::new(sync::RwLock::new(None)),
        }
    }

    fn send(&self, cmd_task: T) -> Result<(), BackendError> {
        let need_init = self.node.read().unwrap().is_none();
        // Race condition here. Multiple threads might be creating new connection at the same time.
        // Maybe it's just fine. If not, lock the creating connection phrase.
        // TODO: use existing connection when there already is.
        if need_init {
            let node_arc = self.node.clone();
            let node_arc2 = self.node.clone();
            let addr = self.addr.parse().unwrap();
            let sock = TcpStream::connect(&addr);
            let fut = sock.then(move |res| {
                debug!("sock result: {:?}", res);
                match res {
                    Ok(sock) => {
                        let (node, reader_handler, writer_handler) = BackendNode::<T>::new(sock, ReplyCommitHandler{});
                        let (reader_handler, writer_handler) = new_future_group(reader_handler, writer_handler);
                        node.send(cmd_task).unwrap();  // must not fail
                        node_arc.write().unwrap().get_or_insert(node);
                        tokio::spawn(reader_handler
                            .map(|()| info!("backend read IO closed"))
                            .map_err(|e| error!("backend read IO error {:?}", e)));
                        tokio::spawn(writer_handler
                            .map(|()| info!("backend write IO closed"))
                            .map_err(|e| error!("backend write IO error {:?}", e)));
                        future::ok(())
                    },
                    Err(e) => {
                        error!("sock err: {:?}", e);
                        cmd_task.set_result(Err(CommandError::Io(io::Error::from(e.kind()))));
                        future::err(())
                    },
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
    fn handle_reply(&self, _cmd_task: T, _result: BackendResult) {
    }
}

pub struct BackendNode<T: CmdTask> {
    tx: mpsc::UnboundedSender<T>
}

impl<T: CmdTask> BackendNode<T> {
    pub fn new<H: ReplyHandler<T>>(sock: TcpStream, handler: H) -> (
            BackendNode<T>,
            impl Future<Item = (), Error = BackendError> + Send,
            impl Future<Item = (), Error = BackendError> + Send) {
        let (tx, rx) = mpsc::unbounded();
        let (reader_handler, writer_handler) = handle_backend(handler, rx, sock);
        (Self{tx}, reader_handler, writer_handler)
    }

    pub fn send(&self, cmd_task: T) -> Result<(), BackendError> {
        self.tx.unbounded_send(cmd_task)
            .map(|_| ())
            .map_err(|_e| BackendError::Canceled)
    }
}

pub fn handle_backend <H, T>(handler: H, task_receiver: mpsc::UnboundedReceiver<T>, sock: TcpStream) -> (
        impl Future<Item = (), Error = BackendError> + Send,
        impl Future<Item = (), Error = BackendError> + Send)
    where H: ReplyHandler<T>, T: CmdTask
{
    let (writer, reader) = RespCodec{}.framed(sock).split();

    let (tx, rx) = mpsc::channel(1024);

    let writer_handler = handle_write(task_receiver, writer, tx);
    let reader_handler = handle_read(handler, reader, rx);

    (reader_handler, writer_handler)
}

fn handle_write<W, T>(task_receiver: mpsc::UnboundedReceiver<T>, writer: W, tx: mpsc::Sender<T>) -> impl Future<Item = (), Error = BackendError> + Send
    where W: Sink<SinkItem = Box<RespPacket>, SinkError = io::Error> + Send + 'static, T: CmdTask
{
    task_receiver.map_err(|()| BackendError::Canceled)
        .fold((writer, tx), |(writer, tx), task| {
            let item = match task.drain_packet_data() {
                Some(data) => {
                    // Tricky code here. The nil array will be ignored when encoded.
                    // TODO: Refactor it by using enum to differentiate this two packet type.
                    RespPacket::new_with_buf(Resp::Arr(Array::Nil), data)
                },
                None => {
                    // TODO: remove the clone
                    let resp = task.get_resp().clone();
                    RespPacket::new(resp)
                },
            };
            writer.send(Box::new(item))
                .then(|res| {
                    let fut : Box<Future<Item=_, Error=BackendError> + Send> = match res {
                        Ok(writer) => {
                            let fut = tx.send(task)
                                .map(move |tx| (writer, tx))
                                .map_err(|e| {
                                    error!("rx closed {:?}", e);
                                    BackendError::Canceled
                                });
                            Box::new(fut)
                        },
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

fn handle_read<H, T, R>(handler: H, reader: R, rx: mpsc::Receiver<T>) -> impl Future<Item = (), Error = BackendError> + Send
    where R: Stream<Item = Box<RespPacket>, Error = DecodeError> + Send + 'static, H: ReplyHandler<T>, T: CmdTask
{
    let rx = rx.into_future();
    reader
        .map_err(|e| {
            match e {
                DecodeError::InvalidProtocol => {
                    error!("backend: invalid protocol");
                    BackendError::InvalidProtocol
                },
                DecodeError::Io(e) => {
                    error!("backend: io error: {:?}", e);
                    BackendError::Io(e)
                },
            }
        })
        .fold((handler, rx), move |(handler, rx), packet| {
            rx.map_err(|((), _receiver)| {
                // The remaining tasks in _receiver might leak here
                // It's up to the tasks inside Receiver to gracefully drop themselves in destructor.
                error!("backend: unexpected read");
                BackendError::Canceled
            })
                .and_then(|(task_opt, rx)| {
                    // TODO: remove this unwrap
                    let task = task_opt.unwrap();
                    task.set_result(Ok(packet));
                    // TODO: call handler
                    future::ok((handler, rx.into_future()))
                })
        })
        .map(|_| ())
}

#[derive(Debug)]
pub enum BackendError {
    Io(io::Error),
    NodeNotFound,
    InvalidProtocol,
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

impl<S: CmdTaskSender> CmdTaskSender for RRSenderGroup<S> {
    type Task = S::Task;

    fn new(address: String) -> Self {
        let mut senders = Vec::new();
        for _ in 0..DEFAULT_GROUP_SIZE {
            senders.push(S::new(address.clone()));
        }
        Self {
            senders,
            cursor: AtomicUsize::new(0),
        }
    }

    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError> {
        let index = self.cursor.fetch_add(1, Ordering::SeqCst);
        let sender = match self.senders.get(index % self.senders.len()) {
            Some(s) => s,
            None => return Err(BackendError::NodeNotFound),
        };
        sender.send(cmd_task)
    }
}
