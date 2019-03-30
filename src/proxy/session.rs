use super::backend::CmdTask;
use super::command::{
    new_command_pair, CmdReplyReceiver, CmdReplySender, Command, CommandError, CommandResult,
};
use super::database::{DBTag, DEFAULT_DB};
use bytes::BytesMut;
use futures::sync::mpsc;
use futures::{Future, Sink, Stream};
use protocol::{DecodeError, Resp, RespCodec, RespPacket};
use std::boxed::Box;
use std::error::Error;
use std::fmt;
use std::io;
use std::sync;
use tokio::codec::Decoder;
use tokio::net::TcpStream;

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
        CmdCtx { db, reply_sender }
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
            error!("Failed to send result {:?}", e);
        }
    }

    fn drain_packet_data(&self) -> Option<BytesMut> {
        self.reply_sender.get_cmd().drain_packet_data()
    }
}

impl DBTag for CmdCtx {
    fn get_db_name(&self) -> String {
        return self.db.read().unwrap().clone();
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
        Session {
            db: sync::Arc::new(sync::RwLock::new(DEFAULT_DB.to_string())),
            cmd_ctx_handler,
        }
    }
}

impl<H: CmdCtxHandler> CmdHandler for Session<H> {
    fn handle_cmd(&mut self, reply_sender: CmdReplySender) {
        self.cmd_ctx_handler
            .handle_cmd_ctx(CmdCtx::new(self.db.clone(), reply_sender));
    }
}

pub fn handle_conn<H>(
    handler: H,
    sock: TcpStream,
) -> (
    impl Future<Item = (), Error = SessionError> + Send,
    impl Future<Item = (), Error = SessionError> + Send,
)
where
    H: CmdHandler + Send + 'static,
{
    let (writer, reader) = RespCodec {}.framed(sock).split();

    let (tx, rx) = mpsc::channel(1024);

    let reader_handler = handle_read(handler, reader, tx);
    let writer_handler = handle_write(writer, rx);

    (reader_handler, writer_handler)
}

fn handle_read<H, R>(
    handler: H,
    reader: R,
    tx: mpsc::Sender<CmdReplyReceiver>,
) -> impl Future<Item = (), Error = SessionError> + Send
where
    R: Stream<Item = Box<RespPacket>, Error = DecodeError> + Send + 'static,
    H: CmdHandler + Send + 'static,
{
    reader
        .map_err(|e| match e {
            DecodeError::Io(e) => SessionError::Io(e),
            DecodeError::InvalidProtocol => SessionError::Canceled,
        })
        .fold((handler, tx), move |(handler, tx), resp| {
            handle_read_resp(handler, tx, resp)
        })
        .map(|_| ())
}

fn handle_read_resp<H>(
    handler: H,
    tx: mpsc::Sender<CmdReplyReceiver>,
    resp: Box<RespPacket>,
) -> impl Future<Item = (H, mpsc::Sender<CmdReplyReceiver>), Error = SessionError> + Send
where
    H: CmdHandler + Send + 'static,
{
    let (reply_sender, reply_receiver) = new_command_pair(Command::new(resp));

    let mut handler = handler;
    handler.handle_cmd(reply_sender);

    tx.send(reply_receiver)
        .map(move |tx| (handler, tx))
        .map_err(|e| {
            warn!("rx closed, {:?}", e);
            SessionError::Canceled
        })
}

fn handle_write<W>(
    writer: W,
    rx: mpsc::Receiver<CmdReplyReceiver>,
) -> impl Future<Item = (), Error = SessionError> + Send
where
    W: Sink<SinkItem = Box<RespPacket>, SinkError = io::Error> + Send + 'static,
{
    rx.map_err(|()| SessionError::Canceled)
        .fold(writer, handle_write_resp)
        .map(|_| ())
}

fn handle_write_resp<W>(
    writer: W,
    reply_receiver: CmdReplyReceiver,
) -> impl Future<Item = W, Error = SessionError> + Send
where
    W: Sink<SinkItem = Box<RespPacket>, SinkError = io::Error> + Send + 'static,
{
    reply_receiver
        .wait_response()
        .map_err(SessionError::CmdErr)
        .then(|res| match res {
            Ok(packet) => writer.send(packet).map_err(SessionError::Io),
            Err(e) => {
                let err_msg = format!("Err cmd error {:?}", e);
                error!("{}", err_msg);
                let resp = Resp::Error(err_msg.into_bytes());
                let packet = Box::new(RespPacket::new(resp));
                writer.send(packet).map_err(SessionError::Io)
            }
        })
}

#[derive(Debug)]
pub enum SessionError {
    Io(io::Error),
    CmdErr(CommandError),
    InvalidProtocol,
    Canceled,
    Other, // TODO: remove this
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
