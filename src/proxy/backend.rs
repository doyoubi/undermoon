use std::io;
use std::iter;
use std::fmt;
use std::sync;
use std::error::Error;
use std::result::Result;
use std::boxed::Box;
use futures::{future, Future, stream, Stream};
use futures::sync::mpsc;
use futures::Sink;
use tokio;
use tokio::net::TcpStream;
use tokio::io::{write_all, AsyncRead, AsyncWrite};
use protocol::{Resp, decode_resp, DecodeError, resp_to_buf};
use super::command::{CommandError, CommandResult};

pub type BackendResult = Result<Resp, BackendError>;

pub trait ReplyHandler<T: CmdTask> : Send + 'static {
    fn handle_reply(&self, cmd_task: T, result: BackendResult);
}

pub trait CmdTask : Send + 'static + fmt::Debug {
    fn get_resp(&self) -> &Resp;
    fn set_result(self, result: CommandResult);
}

pub trait CmdTaskSender {
    type Task: CmdTask;

    fn new(addr: String) -> Self;
    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError>;
}

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
        if need_init {
            let node_arc = self.node.clone();
            let node_arc2 = self.node.clone();
            let addr = self.addr.parse().unwrap();
            let sock = TcpStream::connect(&addr);
            let fut = sock.then(move |res| {
                let fut : Box<Future<Item=_, Error=BackendError> + Send> = match res {
                    Ok(sock) => {
                        let (node, node_fut) = BackendNode::<T>::new_pair(sock, ReplyCommitHandler{});
                        node.send(cmd_task).unwrap();  // must not fail
                        node_arc.write().unwrap().get_or_insert(node);
                        Box::new(node_fut)
                    },
                    Err(e) => {
                        cmd_task.set_result(Err(CommandError::Io(io::Error::from(e.kind()))));
                        Box::new(future::err(BackendError::Io(e)))
                    },
                };
                fut.then(move |r| {
                    println!("backend exited with result {:?}", r);
                    node_arc2.write().unwrap().take();
                    future::ok(())
                })
            });
            // If this future fails, cmd_task will be lost. Let itself send back an error response.
            tokio::spawn(fut);
            return Ok(());
        }
        let res = self.node.read().unwrap().as_ref().unwrap().send(cmd_task);
        match res {
            Ok(()) => Ok(()),
            Err(e) => {
                // if it fails, remove this connection.
                self.node.write().unwrap().take();
                println!("reset backend connecton {:?}", e);
                Err(e)
            }
        }
    }
}

// TODO: Remove this. Retry can be built with a CmdTask wrapper
pub struct ReplyCommitHandler {}

impl<T: CmdTask> ReplyHandler<T> for ReplyCommitHandler {
    fn handle_reply(&self, _cmd_task: T, _result: BackendResult) {
    }
}

pub struct BackendNode<T: CmdTask> {
    tx: mpsc::UnboundedSender<T>
}

impl<T: CmdTask> BackendNode<T> {
    pub fn new_pair<H: ReplyHandler<T>>(sock: TcpStream, handler: H) -> (BackendNode<T>, impl Future<Item = (), Error = BackendError> + Send) {
        let (tx, rx) = mpsc::unbounded();
        (Self{tx: tx}, handle_backend(handler, rx, sock))
    }

    pub fn send(&self, cmd_task: T) -> Result<(), BackendError> {
        self.tx.unbounded_send(cmd_task)
            .map(|_| ())
            .map_err(|_e| BackendError::Canceled)
    }
}

pub fn handle_backend <H, T>(handler: H, task_receiver: mpsc::UnboundedReceiver<T>, sock: TcpStream) -> impl Future<Item = (), Error = BackendError> + Send
    where H: ReplyHandler<T>, T: CmdTask
{
    let (reader, writer) = sock.split();
    let reader = io::BufReader::new(reader);

    let (tx, rx) = mpsc::channel(1024);

    let writer_handler = handle_write(task_receiver, writer, tx);
    let reader_handler = handle_read(handler, reader, rx);

    let handler = reader_handler.select(writer_handler)
        .then(move |res| {
            println!("Backend connection closed.");
            match res {
                Ok(((), _another_future)) => {
                    Result::Ok::<(), BackendError>(())
                },
                Err((e, _another_future)) => {
                    Result::Err(e)
                },
            }
        });
    handler
}

fn handle_write<W, T>(task_receiver: mpsc::UnboundedReceiver<T>, writer: W, tx: mpsc::Sender<T>) -> impl Future<Item = (), Error = BackendError> + Send
    where W: AsyncWrite + Send + 'static, T: CmdTask
{
    let handler = task_receiver
        .map_err(|()| BackendError::Canceled)
        .fold((writer, tx), |(writer, tx), task| {
            let mut buf = vec![];
            resp_to_buf(&mut buf, task.get_resp());
            write_all(writer, buf)
                .then(|res| {
                    let fut : Box<Future<Item=_, Error=BackendError> + Send> = match res {
                        Ok((writer, _)) => {
                            let fut = tx.send(task)
                                .map(move |tx| (writer, tx))
                                .map_err(|e| {
                                    println!("rx closed {:?}", e);
                                    BackendError::Canceled
                                });
                            Box::new(fut)
                        },
                        Err(e) => {
                            println!("Failed to write");
                            task.set_result(Err(CommandError::Io(io::Error::from(e.kind()))));
                            Box::new(future::err(BackendError::Io(e)))
                        },
                    };
                    fut
                })
        });
    handler.map(|_| {
        println!("write future closed")
    }).map_err(|e| {
        println!("write future closed with error {:?}", e);
        e
    })
}

fn handle_read<H, T, R>(handler: H, reader: R, rx: mpsc::Receiver<T>) -> impl Future<Item = (), Error = BackendError> + Send
    where R: AsyncRead + io::BufRead + Send + 'static, H: ReplyHandler<T>, T: CmdTask
{
    let rx = rx.into_future();
    let reader_stream = stream::iter_ok(iter::repeat(()));
    let read_handler = reader_stream.fold((handler, rx, reader), move |(handler, rx, reader), _| {
        decode_resp(reader)
            .then(|res| {
                let fut : Box<Future<Item=_, Error=BackendError> + Send> = match res {
                    Ok((reader, resp)) => {
                        let send_fut = rx
                            .map_err(|((), _receiver)| {
                                // TODO: The remaining tasks in _receiver might leak here
                                // It's up to the tasks inside Receiver to gracefully drop themselves in destructor.
                                println!("backend: unexpected read");
                                BackendError::Canceled
                            })
                            .and_then(|(task_opt, rx)| {
                                let task = task_opt.unwrap();
                                task.set_result(Ok(resp));
                                // TODO: call handler
                                future::ok((handler, rx.into_future(), reader))
                            });
                        Box::new(send_fut)
                    },
                    Err(DecodeError::InvalidProtocol) => {
                        println!("backend: invalid protocol");
                        Box::new(future::err(BackendError::InvalidProtocol))
                    },
                    Err(DecodeError::Io(e)) => {
                        println!("backend: io error: {:?}", e);
                        Box::new(future::err(BackendError::Io(e)))
                    },
                };
                fut
            })
    });
    read_handler.map(|_| {
        println!("read future closed");
    }).map_err(|e| {
        println!("read future closed with error {:?}", e);
        e
    })
}

#[derive(Debug)]
pub enum BackendError {
    Io(io::Error),
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