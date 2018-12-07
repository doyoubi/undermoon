use std::io;
use std::sync;
use std::iter;
use std::fmt;
use std::error::Error;
use std::result::Result;
use std::boxed::Box;
use futures::{future, Future, stream, Stream};
use futures::sync::mpsc;
use futures::Sink;
use tokio::net::TcpStream;
use tokio::io::{write_all, AsyncRead, AsyncWrite};
use protocol::{Resp, Array, decode_resp, DecodeError, resp_to_buf};
use super::command::{CmdReplySender, CmdReplyReceiver, CommandResult, Command, new_command_pair, CommandError};
use super::backend::CmdTask;
use super::database::{DEFAULT_DB, DBTag};

pub trait CmdHandler {
    fn handle_cmd(&mut self, sender: CmdReplySender);
}

pub trait CmdCtxHandler {
    fn handle_cmd_ctx(&self, cmd_ctx: CmdCtx);
}

#[derive(Debug)]
pub struct CmdCtx {
    db: sync::Arc<sync::RwLock<String>>,
    reply_sender: CmdReplySender,
}

impl CmdCtx {
    fn new(db: sync::Arc<sync::RwLock<String>>, reply_sender: CmdReplySender) -> CmdCtx {
        CmdCtx{
            db: db,
            reply_sender: reply_sender,
        }
    }

    pub fn get_cmd(&self) -> &Command {
        self.reply_sender.get_cmd()
    }
}

// Make sure that ctx will always be sent back.
impl Drop for CmdCtx {
    fn drop(&mut self) {
        self.reply_sender.try_send(Err(CommandError::Dropped));
    }
}

impl CmdTask for CmdCtx {
    fn get_resp(&self) -> &Resp {
        self.reply_sender.get_cmd().get_resp()
    }

    fn set_result(self, result: CommandResult) {
        let res = self.reply_sender.send(result);
        if let Err(e) = res {
            println!("Failed to send result {:?}", e);
        }
    }
}

impl DBTag for CmdCtx {
    fn get_db_name(&self) -> String {
        return self.db.read().unwrap().clone()
    }

    fn set_db_name(&self, db: String) {
        *self.db.write().unwrap() = db
    }
}

pub struct Session<H: CmdCtxHandler> {
    db: sync::Arc<sync::RwLock<String>>,
    cmd_ctx_handler: H,
}

impl<H: CmdCtxHandler> Session<H> {
    pub fn new(cmd_ctx_handler: H) -> Self {
        Session{
            db: sync::Arc::new(sync::RwLock::new(DEFAULT_DB.to_string())),
            cmd_ctx_handler: cmd_ctx_handler,
        }
    }
}

impl<H: CmdCtxHandler> CmdHandler for Session<H> {
    fn handle_cmd(&mut self, reply_sender: CmdReplySender) {
        self.cmd_ctx_handler.handle_cmd_ctx(CmdCtx::new(self.db.clone(), reply_sender));
    }
}

pub fn handle_conn<H>(handler: H, sock: TcpStream) -> impl Future<Item = (), Error = SessionError> + Send
   where H: CmdHandler + Send + 'static
{
    let (reader, writer) = sock.split();
    let reader = io::BufReader::new(reader);

    let (tx, rx) = mpsc::channel(1024);

    let reader_handler = handle_read(handler, reader, tx);
    let writer_handler = handle_write(writer, rx);

    let handler = reader_handler.select(writer_handler)
        .then(move |_| {
            println!("Session Connection closed.");
            Result::Ok::<(), SessionError>(())
        });
    handler
}

fn handle_read<H, R>(handler: H, reader: R, tx: mpsc::Sender<CmdReplyReceiver>) -> impl Future<Item = (), Error = SessionError> + Send
    where R: AsyncRead + io::BufRead + Send + 'static, H: CmdHandler + Send + 'static
{
    let reader_stream = stream::iter_ok(iter::repeat(()));
    let handler = reader_stream.fold((handler, tx, reader), move |(handler, tx, reader), _| {
        decode_resp(reader)
            .then(|res| {
                let fut : Box<Future<Item=_, Error=SessionError> + Send> = match res {
                    Ok((reader, resp)) => {
                        let (reply_sender, reply_receiver) = new_command_pair(Command::new(resp));

                        let mut handler = handler;
                        handler.handle_cmd(reply_sender);

                        let send_fut = tx.send(reply_receiver)
                            .map(move |tx| (handler, tx, reader))
                            .map_err(|e| {
                                println!("rx closed, {:?}", e);
                                SessionError::Canceled
                            });
                        Box::new(send_fut)
                    },
                    Err(DecodeError::InvalidProtocol) => {
                        let (reply_sender, reply_receiver) = new_command_pair(Command::new(Resp::Arr(Array::Nil)));

                        let reply = Resp::Error(String::from("Err invalid protocol").into_bytes());
                        reply_sender.send(Ok(reply)).unwrap();

                        let send_fut = tx.send(reply_receiver)
                            .map_err(|e| {
                                println!("rx closed {:?}", e);
                                SessionError::Canceled
                            })
                            .and_then(move |_tx| future::err(SessionError::InvalidProtocol));
                        Box::new(send_fut)
                    },
                    Err(DecodeError::Io(e)) => {
                        println!("io error: {:?}", e);
                        Box::new(future::err(SessionError::Io(e)))
                    },
                };
                fut
            })
    });
    handler.map(|_| ())
}

fn handle_write<W>(writer: W, rx: mpsc::Receiver<CmdReplyReceiver>) -> impl Future<Item = (), Error = SessionError> + Send
    where W: AsyncWrite + Send + 'static
{
    let handler = rx
        .map_err(|()| SessionError::Canceled)
        .fold(writer, |writer, reply_receiver| {
            reply_receiver.wait_response()
                .map_err(SessionError::CmdErr)
                .then(|res| {
                    let fut : Box<Future<Item=_, Error=SessionError> + Send> = match res {
                        Ok(resp) => {
                            let mut buf = vec![];
                            resp_to_buf(&mut buf, &resp);
                            let write_fut = write_all(writer, buf)
                                .map(move |(writer, _)| writer)
                                .map_err(SessionError::Io);
                            Box::new(write_fut)
                        },
                        Err(e) => {
                            // TODO: display error here
                            let err_msg = format!("-Err cmd error {:?}\r\n", e);
                            let write_fut = write_all(writer, err_msg.into_bytes())
                                .map(move |(writer, _)| writer)
                                .map_err(SessionError::Io);
                            Box::new(write_fut)
                        },
                    };
                    fut
                })
        });
    handler.map(|_| ())
}

#[derive(Debug)]
pub enum SessionError {
    Io(io::Error),
    CmdErr(CommandError),
    InvalidProtocol,
    Canceled,
    Other,  // TODO: remove this
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for SessionError {
    fn description(&self) -> &str {
        "session error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            SessionError::Io(err) => Some(err),
            SessionError::CmdErr(err) => Some(err),
            _ => None,
        }
    }
}
