use super::backend::{CmdTask, CmdTaskFactory, TaskResult};
use super::command::TaskReply;
use super::command::{
    new_command_pair, CmdReplySender, CmdType, Command, CommandError, CommandResult, DataCmdType,
};
use super::database::{DBTag, DEFAULT_DB};
use super::slowlog::{SlowRequestLogger, Slowlog, TaskEvent};
use crate::common::utils::ThreadSafe;
use crate::protocol::{DecodeError, Resp, RespCodec, RespPacket, RespVec};
use futures::{stream, Future, TryFutureExt};
use futures::{SinkExt, StreamExt, TryStreamExt};
use futures_batch::ChunksTimeoutStreamExt;
use std::boxed::Box;
use std::error::Error;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_util::codec::Decoder;

// TODO: Let it return future to support multi-key commands.
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

    pub fn get_cmd_type(&self) -> CmdType {
        self.reply_sender.get_cmd().get_type()
    }

    pub fn get_data_cmd_type(&self) -> DataCmdType {
        self.reply_sender.get_cmd().get_data_cmd_type()
    }
}

// Make sure that ctx will always be sent back.
impl Drop for CmdCtx {
    fn drop(&mut self) {
        self.reply_sender.try_send(Err(CommandError::Dropped));
    }
}

impl CmdTask for CmdCtx {
    type Pkt = RespPacket;

    fn get_key(&self) -> Option<&[u8]> {
        self.get_cmd().get_key()
    }

    fn set_result(self, result: CommandResult<Self::Pkt>) {
        let slowlog = self.slowlog.clone();
        let task_result = result.map(|packet| Box::new(TaskReply::new(packet, slowlog)));
        let res = self.reply_sender.send(task_result);
        if let Err(e) = res {
            error!("Failed to send result: {:?}", e);
        }
    }

    fn get_packet(&self) -> Self::Pkt {
        self.reply_sender.get_cmd().get_packet()
    }

    fn log_event(&self, event: TaskEvent) {
        self.slowlog.log_event(event);
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
        Pin<Box<dyn Future<Output = TaskResult> + Send + 'static>>,
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
            .map_ok(|reply| reply.into_resp_vec());
        (cmd_ctx, Box::pin(fut))
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
        cmd_ctx.log_event(TaskEvent::Created);
        self.cmd_ctx_handler.handle_cmd_ctx(cmd_ctx);
    }

    fn handle_slowlog(&self, slowlog: sync::Arc<Slowlog>) {
        self.slow_request_logger.add_slow_log(slowlog)
    }
}

pub async fn handle_session<H>(
    handler: sync::Arc<H>,
    sock: TcpStream,
    _channel_size: usize,
    session_batch_min_time: usize,
    _session_batch_max_time: usize,
    session_batch_buf: usize,
) -> Result<(), SessionError>
where
    H: CmdHandler + Send + Sync + 'static,
{
    let (mut writer, reader) = RespCodec::default().framed(sock).split();
    let mut reader = reader
        .map_err(|e| match e {
            DecodeError::Io(e) => SessionError::Io(e),
            DecodeError::InvalidProtocol => SessionError::Canceled,
        })
        .chunks_timeout(
            session_batch_buf,
            Duration::from_nanos(session_batch_min_time as u64),
        );

    let mut reply_receiver_list = Vec::with_capacity(session_batch_buf);
    let mut replies = Vec::with_capacity(session_batch_buf);

    while let Some(reqs) = reader.next().await {
        for req in reqs.into_iter() {
            let packet = match req {
                Ok(packet) => packet,
                Err(err) => {
                    error!("session reader error {:?}", err);
                    return Err(err);
                }
            };
            let (reply_sender, reply_receiver) = new_command_pair(Command::new(packet));

            handler.handle_cmd(reply_sender);
            reply_receiver_list.push(reply_receiver);
        }

        for reply_receiver in reply_receiver_list.drain(..) {
            let res = reply_receiver
                .wait_response()
                .await
                .map_err(SessionError::CmdErr);
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
                    Box::new(RespPacket::from_resp_vec(resp))
                }
            };

            replies.push(packet);
        }

        let mut batch = stream::iter(replies.drain(..)).map(Ok);
        if let Err(err) = writer.send_all(&mut batch).await {
            error!("writer error: {}", err);
            return Err(SessionError::Io(err));
        }
    }

    Ok(())
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
