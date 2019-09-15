use super::backend::CmdTask;
use super::command::TaskReply;
use super::command::{
    new_command_pair, CmdReplyReceiver, CmdReplySender, CmdType, Command, CommandError,
    CommandResult, DataCmdType,
};
use super::database::{DBTag, DEFAULT_DB};
use super::slowlog::{SlowRequestLogger, Slowlog, TaskEvent};
use ::common::utils::ThreadSafe;
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
    fn handle_cmd(&self, sender: CmdReplySender);
    fn handle_slowlog(&self, slowlog: sync::Arc<Slowlog>);
}

pub trait CmdCtxHandler {
    fn handle_cmd_ctx(&self, cmd_ctx: CmdCtx);
}

#[derive(Debug)]
pub struct CmdCtx {
    db: sync::Arc<sync::RwLock<String>>,
    reply_sender: CmdReplySender,
    slowlog: sync::Arc<Slowlog>,
}

impl ThreadSafe for CmdCtx {}

impl CmdCtx {
    fn new(
        db: sync::Arc<sync::RwLock<String>>,
        reply_sender: CmdReplySender,
        session_id: usize,
    ) -> CmdCtx {
        let slowlog = sync::Arc::new(Slowlog::from_command(reply_sender.get_cmd(), session_id));
        CmdCtx {
            db,
            reply_sender,
            slowlog,
        }
    }

    pub fn get_cmd(&self) -> &Command {
        self.reply_sender.get_cmd()
    }

    pub fn change_cmd_element(&mut self, index: usize, data: Vec<u8>) -> bool {
        self.reply_sender.get_mut_cmd().change_element(index, data)
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

    fn get_cmd_type(&self) -> CmdType {
        self.reply_sender.get_cmd().get_type()
    }

    fn get_data_cmd_type(&self) -> DataCmdType {
        self.reply_sender.get_cmd().get_data_cmd_type()
    }

    fn set_result(self, result: CommandResult) {
        let slowlog = self.slowlog.clone();
        let task_result = result.map(|packet| Box::new(TaskReply::new(packet, slowlog)));
        let res = self.reply_sender.send(task_result);
        if let Err(e) = res {
            error!("Failed to send result {:?}", e);
        }
    }

    fn drain_packet_data(&self) -> Option<BytesMut> {
        self.reply_sender.get_cmd().drain_packet_data()
    }

    fn get_slowlog(&self) -> &Slowlog {
        &self.slowlog
    }
}

impl DBTag for CmdCtx {
    fn get_db_name(&self) -> String {
        self.db.read().unwrap().clone()
    }

    fn set_db_name(&self, db: String) {
        *self.db.write().unwrap() = db
    }
}

pub struct Session<H: CmdCtxHandler> {
    session_id: usize,
    db: sync::Arc<sync::RwLock<String>>,
    cmd_ctx_handler: H,
    slow_request_logger: sync::Arc<SlowRequestLogger>,
}

impl<H: CmdCtxHandler> Session<H> {
    pub fn new(
        session_id: usize,
        cmd_ctx_handler: H,
        slow_request_logger: sync::Arc<SlowRequestLogger>,
    ) -> Self {
        Session {
            session_id,
            db: sync::Arc::new(sync::RwLock::new(DEFAULT_DB.to_string())),
            cmd_ctx_handler,
            slow_request_logger,
        }
    }
}

impl<H: CmdCtxHandler> CmdHandler for Session<H> {
    fn handle_cmd(&self, reply_sender: CmdReplySender) {
        let cmd_ctx = CmdCtx::new(self.db.clone(), reply_sender, self.session_id);
        cmd_ctx.get_slowlog().log_event(TaskEvent::Created);
        self.cmd_ctx_handler.handle_cmd_ctx(cmd_ctx);
    }

    fn handle_slowlog(&self, slowlog: sync::Arc<Slowlog>) {
        self.slow_request_logger.add_slow_log(slowlog)
    }
}

pub fn handle_conn<H>(
    handler: sync::Arc<H>,
    sock: TcpStream,
    channel_size: usize,
) -> (
    impl Future<Item = (), Error = SessionError> + Send,
    impl Future<Item = (), Error = SessionError> + Send,
)
where
    H: CmdHandler + Send + Sync + 'static,
{
    let (writer, reader) = RespCodec {}.framed(sock).split();

    let (tx, rx) = mpsc::channel(channel_size);

    let reader_handler = handle_read(handler.clone(), reader, tx);
    let writer_handler = handle_write(handler, writer, rx);

    (reader_handler, writer_handler)
}

fn handle_read<H, R>(
    handler: sync::Arc<H>,
    reader: R,
    tx: mpsc::Sender<CmdReplyReceiver>,
) -> impl Future<Item = (), Error = SessionError> + Send
where
    R: Stream<Item = Box<RespPacket>, Error = DecodeError> + Send + 'static,
    H: CmdHandler + Send + Sync + 'static,
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
    handler: sync::Arc<H>,
    tx: mpsc::Sender<CmdReplyReceiver>,
    resp: Box<RespPacket>,
) -> impl Future<Item = (sync::Arc<H>, mpsc::Sender<CmdReplyReceiver>), Error = SessionError> + Send
where
    H: CmdHandler + Send + Sync + 'static,
{
    let (reply_sender, reply_receiver) = new_command_pair(Command::new(resp));

    handler.handle_cmd(reply_sender);

    tx.send(reply_receiver)
        .map(move |tx| (handler, tx))
        .map_err(|e| {
            warn!("rx closed, {:?}", e);
            SessionError::Canceled
        })
}

fn handle_write<H, W>(
    handler: sync::Arc<H>,
    writer: W,
    rx: mpsc::Receiver<CmdReplyReceiver>,
) -> impl Future<Item = (), Error = SessionError> + Send
where
    H: CmdHandler + Send + Sync + 'static,
    W: Sink<SinkItem = Box<RespPacket>, SinkError = io::Error> + Send + 'static,
{
    rx.map_err(|()| SessionError::Canceled)
        .fold((handler, writer), |(handler, writer), reply_receiver| {
            handle_write_resp(handler, writer, reply_receiver)
        })
        .map(|_| ())
}

fn handle_write_resp<H, W>(
    handler: sync::Arc<H>,
    writer: W,
    reply_receiver: CmdReplyReceiver,
) -> impl Future<Item = (sync::Arc<H>, W), Error = SessionError> + Send
where
    H: CmdHandler + Send + Sync + 'static,
    W: Sink<SinkItem = Box<RespPacket>, SinkError = io::Error> + Send + 'static,
{
    reply_receiver
        .wait_response()
        .map_err(SessionError::CmdErr)
        .then(|res| {
            let packet = match res {
                Ok(task_reply) => {
                    let (packet, slowlog) = (*task_reply).into_inner();
                    slowlog.log_event(TaskEvent::WaitDone);
                    handler.handle_slowlog(slowlog);
                    packet
                }
                Err(e) => {
                    let err_msg = format!("Err cmd error {:?}", e);
                    error!("{}", err_msg);
                    let resp = Resp::Error(err_msg.into_bytes());
                    Box::new(RespPacket::new(resp))
                }
            };
            writer
                .send(packet)
                .map_err(SessionError::Io)
                .map(|writer| (handler, writer))
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

    fn cause(&self) -> Option<&dyn Error> {
        match self {
            SessionError::Io(err) => Some(err),
            SessionError::CmdErr(err) => Some(err),
            _ => None,
        }
    }
}
