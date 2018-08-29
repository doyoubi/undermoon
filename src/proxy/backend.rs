use std::io;
use std::iter;
use std::fmt;
use std::error::Error;
use std::result::Result;
use futures::{future, Future, stream, Stream, BoxFuture};
use futures::sync::mpsc;
use futures::Sink;
use tokio::net::TcpStream;
use tokio::io::{write_all, AsyncRead, AsyncWrite};
use protocol::{Resp, Array, BulkStr, decode_resp, DecodeError, resp_to_buf};
use super::command::{CommandResult, CommandError};

pub type BackendResult = Result<Resp, BackendError>;

pub trait ReplyHandler<T: CmdTask> {
    fn handle_reply(&self, cmd_task: T, result: BackendResult);
}

pub trait CmdTask {
    fn get_resp(&self) -> &Resp;
    fn set_result(self, result: CommandResult);
}

pub struct BackendNode<T: CmdTask + Send + 'static> {
    tx: mpsc::UnboundedSender<T>
}

impl<T: CmdTask + Send + 'static> BackendNode<T> {
    pub fn new_pair<H: ReplyHandler<T> + Send + 'static>(sock: TcpStream, handler: H) -> (BackendNode<T>, impl Future<Item = (), Error = BackendError> + Send) {
        let (tx, rx) = mpsc::unbounded();
        (Self{tx: tx}, handle_backend(handler, rx, sock))
    }

    pub fn send(&self, cmd_task: T) -> Result<(), BackendError> {
        self.tx.unbounded_send(cmd_task)
            .map(|_| ())
            .map_err(|_e| BackendError::Canceled)
    }
}

pub fn handle_backend <H, T>(handler: H, taskReceiver: mpsc::UnboundedReceiver<T>, sock: TcpStream) -> impl Future<Item = (), Error = BackendError> + Send
    where H: ReplyHandler<T> + Send + 'static, T: CmdTask + Send + 'static
{
    let (reader, writer) = sock.split();
    let reader = io::BufReader::new(reader);

    let (tx, rx) = mpsc::channel(1024);

    let writer_handler = handle_write(taskReceiver, writer, tx);
    let reader_handler = handle_read(handler, reader, rx);

    let handler = reader_handler.select(writer_handler)
        .then(move |_| {
            println!("Connection closed.");
            Result::Ok::<(), BackendError>(())
        });
    handler
}

fn handle_write<W, T>(taskReceiver: mpsc::UnboundedReceiver<T>, writer: W, tx: mpsc::Sender<T>) -> impl Future<Item = (), Error = BackendError> + Send
    where W: AsyncWrite + Send + 'static, T: CmdTask + Send + 'static
{
    let handler = taskReceiver
        .map_err(|e| BackendError::Canceled)
        .fold((writer, tx), |(writer, tx), task| {
            let mut buf = vec![];
            resp_to_buf(&mut buf, task.get_resp());
            write_all(writer, buf)
                .then(|res| {
                    let task = task;
                    let fut : BoxFuture<_, BackendError> = match res {
                        Ok((writer, _)) => {
                            let fut = tx.send(task)
                                .map(move |tx| (writer, tx))
                                .map_err(|e| {
                                    println!("rx closed");
                                    BackendError::Canceled
                                });
                            Box::new(fut)
                        },
                        Err(e) => {
                            task.set_result(Err(CommandError::Io(io::Error::from(e.kind()))));
                            Box::new(future::err(BackendError::Io(e)))
                        },
                    };
                    fut
                })
        });
    handler.map(|_| ())
}

fn handle_read<H, T, R>(handler: H, reader: R, rx: mpsc::Receiver<T>) -> impl Future<Item = (), Error = BackendError> + Send
    where R: AsyncRead + io::BufRead + Send + 'static, H: ReplyHandler<T> + Send + 'static, T: CmdTask + Send + 'static
{
    let rx = rx.into_future();
    let reader_stream = stream::iter_ok(iter::repeat(()));
    let handler = reader_stream.fold((handler, rx, reader), move |(handler, rx, reader), _| {
        decode_resp(reader)
            .then(|res| {
                let fut : BoxFuture<_, BackendError> = match res {
                    Ok((reader, resp)) => {
                        let sendFut = rx
                            .map_err(|e| {
                                // TODO: task will leak here
                                println!("backend: unexpected read");
                                BackendError::Canceled
                            })
                            .and_then(|(taskOpt, rx)| {
                                let task = taskOpt.unwrap();
                                task.set_result(Ok(resp));
                                // TODO: call handler
                                future::ok((handler, rx.into_future(), reader))
                            });
                        Box::new(sendFut)
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
    handler.map(|_| ())
}

#[derive(Debug)]
pub enum BackendError {
    Io(io::Error),
    InvalidProtocol,
    Canceled,
    Other,  // TODO: remove this
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