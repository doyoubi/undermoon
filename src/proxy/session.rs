use super::backend::{CmdTask, CmdTaskFactory, CmdTaskResult};
use super::command::{
    new_command_pair, CmdReplyReceiver, CmdReplySender, CmdType, Command, CommandError,
    CommandResult, DataCmdType, TaskReply, TaskResult,
};
use super::service::ServerProxyConfig;
use super::slowlog::{SlowRequestLogger, Slowlog, TaskEvent};
use crate::protocol::{
    new_simple_packet_codec, BinSafeStr, DecodeError, EncodeError, Resp, RespCodec, RespPacket,
    RespVec,
};
use futures::task::{Context, Poll};
use futures::{future, Future, Sink, Stream, TryFutureExt};
use futures::{StreamExt, TryStreamExt};
use std::boxed::Box;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_util::codec::Decoder;
use std::sync::atomic::AtomicBool;

// CmdReplyReceiver is the fast path without heap allocation.
pub type CmdReplyFuture<'a> =
    future::Either<CmdReplyReceiver, Pin<Box<dyn Future<Output = TaskResult> + Send + 'a>>>;

pub trait CmdHandler {
    fn handle_cmd(&self, cmd: Command) -> CmdReplyFuture;
    fn handle_slowlog(&self, request: Box<RespPacket>, slowlog: Slowlog);
}

pub trait CmdCtxHandler {
    fn handle_cmd_ctx(
        &self,
        cmd_ctx: CmdCtx,
        result_receiver: CmdReplyReceiver,
        authenticated: &AtomicBool,
    ) -> CmdReplyFuture;
}

#[derive(Debug)]
pub struct CmdCtx {
    cmd: Command,
    reply_sender: CmdReplySender,
    slowlog: Slowlog,
    redirection_times: Option<usize>,
}

impl CmdCtx {
    pub fn new(
        cmd: Command,
        reply_sender: CmdReplySender,
        session_id: usize,
        slowlog_enabled: bool,
    ) -> CmdCtx {
        let slowlog = Slowlog::new(session_id, slowlog_enabled);
        CmdCtx {
            cmd,
            reply_sender,
            slowlog,
            redirection_times: None,
        }
    }

    pub fn get_cmd(&self) -> &Command {
        &self.cmd
    }

    pub fn get_session_id(&self) -> usize {
        self.slowlog.get_session_id()
    }

    pub fn change_cmd_element(&mut self, index: usize, data: Vec<u8>) -> bool {
        self.cmd.change_element(index, data)
    }

    // Returns remaining elements
    pub fn extract_inner_cmd(&mut self, removed_num: usize) -> Option<usize> {
        self.cmd.extract_inner_cmd(removed_num)
    }

    pub fn wrap_cmd(&mut self, preceding_element: Vec<BinSafeStr>) -> bool {
        self.cmd.wrap_cmd(preceding_element)
    }

    pub fn get_cmd_type(&self) -> CmdType {
        self.cmd.get_type()
    }

    pub fn get_data_cmd_type(&self) -> DataCmdType {
        self.cmd.get_data_cmd_type()
    }

    pub fn set_redirection_times(&mut self, redirection_times: usize) {
        self.redirection_times = Some(redirection_times)
    }

    pub fn get_redirection_times(&self) -> Option<usize> {
        self.redirection_times
    }
}

pub struct SessionContext {
    session_id: usize,
    slowlog_enabled: bool,
}

impl CmdTask for CmdCtx {
    type Pkt = RespPacket;
    type TaskType = (CmdType, DataCmdType);
    type Context = SessionContext;

    fn get_key(&self) -> Option<&[u8]> {
        self.get_cmd().get_key()
    }

    fn get_slot(&self) -> Option<usize> {
        self.get_cmd().get_slot()
    }

    fn set_result(self, result: CommandResult<Self::Pkt>) {
        let Self {
            cmd,
            mut reply_sender,
            slowlog,
            ..
        } = self;
        let task_result =
            result.map(|packet| Box::new(TaskReply::new(cmd.into_packet(), packet, slowlog)));
        let res = reply_sender.send(task_result);
        if let Err(e) = res {
            error!("Failed to send result: {:?}", e);
        }
    }

    fn get_packet(&self) -> Self::Pkt {
        self.cmd.get_packet()
    }

    fn get_type(&self) -> Self::TaskType {
        (self.cmd.get_type(), self.cmd.get_data_cmd_type())
    }

    fn get_context(&self) -> Self::Context {
        SessionContext {
            session_id: self.get_session_id(),
            slowlog_enabled: self.slowlog.is_enabled(),
        }
    }

    fn set_resp_result(self, result: Result<RespVec, CommandError>)
    where
        Self: Sized,
    {
        self.set_result(result.map(|resp| Box::new(RespPacket::from(resp))))
    }

    fn log_event(&mut self, event: TaskEvent) {
        self.slowlog.log_event(event);
    }
}

pub struct CmdCtxFactory;

impl Default for CmdCtxFactory {
    fn default() -> Self {
        Self
    }
}

impl CmdTaskFactory for CmdCtxFactory {
    type Task = CmdCtx;

    fn create_with_ctx(
        &self,
        context: <Self::Task as CmdTask>::Context,
        resp: RespVec,
    ) -> (
        Self::Task,
        Pin<Box<dyn Future<Output = CmdTaskResult> + Send + 'static>>,
    ) {
        let packet = Box::new(RespPacket::from_resp_vec(resp));
        let cmd = Command::new(packet);
        let (reply_sender, reply_receiver) = new_command_pair(&cmd);
        let SessionContext {
            session_id,
            slowlog_enabled,
        } = context;
        let cmd_ctx = CmdCtx::new(cmd, reply_sender, session_id, slowlog_enabled);
        let fut = reply_receiver.map_ok(|reply| reply.into_resp_vec());
        (cmd_ctx, Box::pin(fut))
    }
}

pub struct Session<H: CmdCtxHandler> {
    session_id: usize,
    authenticated: AtomicBool,
    cmd_ctx_handler: H,
    slow_request_logger: sync::Arc<SlowRequestLogger>,
    config: Arc<ServerProxyConfig>,
}

impl<H: CmdCtxHandler> Session<H> {
    pub fn new(
        session_id: usize,
        cmd_ctx_handler: H,
        slow_request_logger: sync::Arc<SlowRequestLogger>,
        config: Arc<ServerProxyConfig>,
    ) -> Self {
        Session {
            session_id,
            authenticated: AtomicBool::new(false),
            cmd_ctx_handler,
            slow_request_logger,
            config,
        }
    }
}

impl<H: CmdCtxHandler> CmdHandler for Session<H> {
    fn handle_cmd(&self, cmd: Command) -> CmdReplyFuture {
        let (reply_sender, reply_receiver) = new_command_pair(&cmd);

        let slowlog_enabled = self
            .slow_request_logger
            .limit_rate(self.config.get_slowlog_sample_rate());
        let mut cmd_ctx = CmdCtx::new(
            cmd,
            reply_sender,
            self.session_id,
            slowlog_enabled,
        );
        cmd_ctx.log_event(TaskEvent::Created);
        self.cmd_ctx_handler
            .handle_cmd_ctx(cmd_ctx, reply_receiver, &self.authenticated)
    }

    fn handle_slowlog(&self, request: Box<RespPacket>, slowlog: Slowlog) {
        self.slow_request_logger.add_slow_log(request, slowlog)
    }
}

pub async fn handle_session<H>(handler: sync::Arc<H>, sock: TcpStream) -> Result<(), SessionError>
where
    H: CmdHandler + Send + Sync + 'static,
{
    let (encoder, decoder) = new_simple_packet_codec::<Box<RespPacket>, Box<RespPacket>>();
    let (mut writer, reader) = RespCodec::new(encoder, decoder).framed(sock).split();
    let mut reader = reader.map_err(|e| match e {
        DecodeError::Io(e) => SessionError::Io(e),
        DecodeError::InvalidProtocol => SessionError::Canceled,
    });

    const SESSION_BATCH_BUF: usize = 64;
    let mut reply_receiver_list = VecDeque::<CmdReplyFuture>::with_capacity(SESSION_BATCH_BUF);
    let mut replies = VecDeque::<Box<RespPacket>>::with_capacity(SESSION_BATCH_BUF);

    future::poll_fn(|cx: &mut Context<'_>| -> Poll<Result<(), SessionError>> {
        loop {
            match Pin::new(&mut reader).poll_next(cx) {
                Poll::Ready(None) => {
                    info!("Session is closed by peer");
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Some(req)) => {
                    let packet = match req {
                        Ok(packet) => packet,
                        Err(err) => {
                            error!("session reader error {:?}", err);
                            return Poll::Ready(Err(err));
                        }
                    };
                    let cmd = Command::new(packet);

                    let fut = handler.handle_cmd(cmd);
                    reply_receiver_list.push_back(fut);
                }
                Poll::Pending => {
                    break;
                }
            }
        }

        // For blocking commands, any later command won't run until the blocking commands finish.
        while let Some(reply_receiver) = reply_receiver_list.front_mut() {
            match Pin::new(reply_receiver).poll(cx) {
                Poll::Pending => {
                    break;
                }
                Poll::Ready(res) => {
                    if reply_receiver_list.pop_front().is_none() {
                        error!("invalid state when popping reply_receiver_list");
                        return Poll::Ready(Err(SessionError::InvalidState));
                    }

                    let packet = match res {
                        Ok(task_reply) => {
                            let (request, packet, mut slowlog) = (*task_reply).into_inner();
                            slowlog.log_event(TaskEvent::WaitDone);
                            handler.handle_slowlog(request, slowlog);
                            packet
                        }
                        Err(e) => {
                            let err_msg = format!("Err cmd error {:?}", e);
                            error!("{}", err_msg);
                            let resp = Resp::Error(err_msg.into_bytes());
                            Box::new(RespPacket::from_resp_vec(resp))
                        }
                    };

                    replies.push_back(packet);
                }
            }
        }

        let poll_res = loop {
            match Pin::new(&mut writer).poll_ready(cx) {
                Poll::Pending => break Poll::Pending,
                Poll::Ready(Ok(())) => (),
                Poll::Ready(Err(err)) => break Poll::Ready(Err(err)),
            }

            match replies.pop_front() {
                Some(reply) => {
                    if let Err(err) = Pin::new(&mut writer).start_send(reply) {
                        break Poll::Ready(Err(err));
                    }
                }
                None => {
                    // Even we don't call `start_send` this time,
                    // the former execution of this polling function may have
                    // a Pending result for poll_flush. We need to flush anyway.
                    break Pin::new(&mut writer).poll_flush(cx);
                }
            };
        };

        match poll_res {
            Poll::Pending | Poll::Ready(Ok(())) => Poll::Pending,
            Poll::Ready(Err(err)) => {
                let err = match err {
                    EncodeError::Io(err) => SessionError::Io(err),
                    EncodeError::NotReady(_) => SessionError::InvalidState,
                };
                Poll::Ready(Err(err))
            }
        }
    })
    .await
}

#[derive(Debug)]
pub enum SessionError {
    Io(io::Error),
    CmdErr(CommandError),
    InvalidProtocol,
    Canceled,
    InvalidState,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Array, BulkStr, Resp};
    use tokio;

    #[tokio::test]
    async fn test_cmd_ctx_auto_send() {
        let request = RespPacket::Data(Resp::Arr(Array::Arr(vec![Resp::Bulk(BulkStr::Str(
            b"PING".to_vec(),
        ))])));
        let cmd = Command::new(Box::new(request));
        let (sender, receiver) = new_command_pair(&cmd);
        let cmd_ctx = CmdCtx::new(cmd, sender, 7799, true);
        drop(cmd_ctx);
        let err = match receiver.await {
            Ok(_) => panic!(),
            Err(err) => err,
        };
        assert!(matches!(err, CommandError::Dropped));
    }
}
