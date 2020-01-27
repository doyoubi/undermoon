use super::backend::{CmdTask, CmdTaskFactory};
use super::command::TaskReply;
use super::command::{
    new_command_pair, CmdReplyReceiver, CmdReplySender, CmdType, Command, CommandError,
    CommandResult, DataCmdType,
};
use super::database::{DBTag, DEFAULT_DB};
use super::slowlog::{SlowRequestLogger, Slowlog, TaskEvent};
use ::common::utils::ThreadSafe;
use common::batching;
use futures::sync::mpsc;
use futures::{future, stream, Future, Sink, Stream};
use protocol::{DecodeError, Resp, RespCodec, RespPacket, RespSlice, RespVec};
use std::boxed::Box;
use std::error::Error;
use std::fmt;
use std::io;
use std::sync;
use std::time::Duration;
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
    pub fn new(
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

    pub fn get_db(&self) -> sync::Arc<sync::RwLock<String>> {
        self.db.clone()
    }

    pub fn get_session_id(&self) -> usize {
        self.slowlog.get_session_id()
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
    fn get_key(&self) -> Option<&[u8]> {
        self.get_cmd().get_key()
    }

    fn get_resp_slice(&self) -> RespSlice {
        self.reply_sender.get_cmd().get_resp_slice()
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
            error!("Failed to send result: {:?}", e);
        }
    }

    fn get_packet(&self) -> RespPacket {
        self.reply_sender.get_cmd().get_packet()
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

pub struct CmdCtxFactory;

impl ThreadSafe for CmdCtxFactory {}

impl Default for CmdCtxFactory {
    fn default() -> Self {
        Self
    }
}

impl CmdTaskFactory for CmdCtxFactory {
    type Task = CmdCtx;

    fn create_with(
        &self,
        another_task: &Self::Task,
        resp: RespVec,
    ) -> (
        Self::Task,
        Box<dyn Future<Item = RespVec, Error = CommandError> + Send>,
    ) {
        let packet = Box::new(RespPacket::from_resp_vec(resp));
        let (reply_sender, reply_receiver) = new_command_pair(Command::new(packet));
        let cmd_ctx = CmdCtx::new(
            another_task.get_db(),
            reply_sender,
            another_task.get_session_id(),
        );
        let fut = reply_receiver
            .wait_response()
            .map(|reply| reply.into_resp_vec());
        (cmd_ctx, Box::new(fut))
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
    session_batch_min_time: usize,
    session_batch_max_time: usize,
    session_batch_buf: usize,
) -> (
    impl Future<Item = (), Error = SessionError> + Send,
    impl Future<Item = (), Error = SessionError> + Send,
)
where
    H: CmdHandler + Send + Sync + 'static,
{
    let (writer, reader) = RespCodec::default().framed(sock).split();

    let (tx, rx) = mpsc::channel(channel_size);

    let reader_handler = handle_read(handler.clone(), reader, tx);
    let writer_handler = handle_write(
        handler,
        writer,
        rx,
        session_batch_min_time,
        session_batch_max_time,
        session_batch_buf,
    );

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
    session_batch_min_time: usize,
    session_batch_max_time: usize,
    session_batch_buf: usize,
) -> impl Future<Item = (), Error = SessionError> + Send
where
    H: CmdHandler + Send + Sync + 'static,
    W: Sink<SinkItem = Box<RespPacket>, SinkError = io::Error> + Send + 'static,
{
    let handler_clone = handler.clone();
    let packets_stream = rx
        .map_err(|()| SessionError::Canceled)
        .and_then(|reply_receiver| reply_receiver.wait_response().map_err(SessionError::CmdErr))
        .then(move |res| match res {
            Ok(task_reply) => {
                let (packet, slowlog) = (*task_reply).into_inner();
                slowlog.log_event(TaskEvent::WaitDone);
                handler_clone.handle_slowlog(slowlog);
                future::ok(packet)
            }
            Err(e) => {
                let err_msg = format!("Err cmd error {:?}", e);
                error!("{}", err_msg);
                let resp = Resp::Error(err_msg.into_bytes());
                future::ok(Box::new(RespPacket::from_resp_vec(resp)))
            }
        });

    let batch_min_time = Duration::from_nanos(session_batch_min_time as u64);
    let batch_max_time = Duration::from_nanos(session_batch_max_time as u64);
    batching::Chunks::new(
        packets_stream,
        session_batch_buf,
        batch_min_time,
        batch_max_time,
    )
    .map_err(|err: batching::Error<SessionError>| {
        error!("batching error {:?}", err);
        SessionError::Canceled
    })
    .fold((handler, writer), |(handler, writer), packets| {
        writer
            .send_all(stream::iter_ok::<_, io::Error>(packets))
            .map_err(SessionError::Io)
            .map(|(writer, empty_stream)| {
                debug_assert!(empty_stream
                    .collect()
                    .wait()
                    .expect("invalid empty_stream")
                    .is_empty());
                (handler, writer)
            })
    })
    .map(|_| ())
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
