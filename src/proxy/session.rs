use super::backend::{CmdTask, CmdTaskFactory, CmdTaskResult};
use super::cluster::{ClusterTag, DEFAULT_CLUSTER};
use super::command::{
    new_command_pair, CmdReplyReceiver, CmdReplySender, CmdType, Command, CommandError,
    CommandResult, DataCmdType, TaskReply, TaskResult,
};
use super::service::ServerProxyConfig;
use super::slowlog::{SlowRequestLogger, Slowlog, TaskEvent};
use crate::common::batch::TryChunksTimeoutStreamExt;
use crate::common::cluster::ClusterName;
use crate::protocol::{
    new_simple_packet_codec, BinSafeStr, DecodeError, EncodeError, Resp, RespCodec, RespPacket,
    RespVec,
};
use futures::{future, stream, Future, TryFutureExt};
use futures::{SinkExt, StreamExt, TryStreamExt};
use std::boxed::Box;
use std::cmp::min;
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::error::Error;
use std::fmt;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_util::codec::Decoder;

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
        session_cluster_name: &sync::RwLock<ClusterName>,
    ) -> CmdReplyFuture;
}

#[derive(Debug)]
pub struct CmdCtx {
    cmd: Command,
    reply_sender: CmdReplySender,
    slowlog: Slowlog,
    cluster_name: ClusterName,
    redirection_times: Option<usize>,
}

impl CmdCtx {
    pub fn new(
        cluster_name: ClusterName,
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
            cluster_name,
            redirection_times: None,
        }
    }

    pub fn get_cmd(&self) -> &Command {
        &self.cmd
    }

    pub fn get_cluster(&self) -> ClusterName {
        self.cluster_name.clone()
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
    cluster_name: ClusterName,
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
            cluster_name: self.cluster_name.clone(),
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

impl ClusterTag for CmdCtx {
    fn get_cluster_name(&self) -> &ClusterName {
        &self.cluster_name
    }

    fn set_cluster_name(&mut self, cluster_name: ClusterName) {
        self.cluster_name = cluster_name;
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
            cluster_name,
            session_id,
            slowlog_enabled,
        } = context;
        let cmd_ctx = CmdCtx::new(cluster_name, cmd, reply_sender, session_id, slowlog_enabled);
        let fut = reply_receiver.map_ok(|reply| reply.into_resp_vec());
        (cmd_ctx, Box::pin(fut))
    }
}

pub struct Session<H: CmdCtxHandler> {
    session_id: usize,
    cluster_name: sync::Arc<sync::RwLock<ClusterName>>,
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
        let cluster_name = ClusterName::try_from(DEFAULT_CLUSTER).expect("Session::new");
        Session {
            session_id,
            cluster_name: sync::Arc::new(sync::RwLock::new(cluster_name)),
            cmd_ctx_handler,
            slow_request_logger,
            config,
        }
    }
}

impl<H: CmdCtxHandler> CmdHandler for Session<H> {
    fn handle_cmd(&self, cmd: Command) -> CmdReplyFuture {
        let (reply_sender, reply_receiver) = new_command_pair(&cmd);
        let cluster_name = self
            .cluster_name
            .read()
            .expect("Session::handle_cmd")
            .clone();

        let slowlog_enabled = self
            .slow_request_logger
            .limit_rate(self.config.get_slowlog_sample_rate());
        let mut cmd_ctx = CmdCtx::new(
            cluster_name,
            cmd,
            reply_sender,
            self.session_id,
            slowlog_enabled,
        );
        cmd_ctx.log_event(TaskEvent::Created);
        self.cmd_ctx_handler
            .handle_cmd_ctx(cmd_ctx, reply_receiver, &(*self.cluster_name))
    }

    fn handle_slowlog(&self, request: Box<RespPacket>, slowlog: Slowlog) {
        self.slow_request_logger.add_slow_log(request, slowlog)
    }
}

pub async fn handle_session<H>(
    handler: sync::Arc<H>,
    sock: TcpStream,
    _channel_size: usize,
    session_batch_min_time: usize,
    session_batch_max_time: usize,
    session_batch_buf: NonZeroUsize,
) -> Result<(), SessionError>
where
    H: CmdHandler + Send + Sync + 'static,
{
    let (encoder, decoder) = new_simple_packet_codec::<Box<RespPacket>, Box<RespPacket>>();
    let (mut writer, reader) = RespCodec::new(encoder, decoder).framed(sock).split();
    let mut reader = reader
        .map_err(|e| match e {
            DecodeError::Io(e) => SessionError::Io(e),
            DecodeError::InvalidProtocol => SessionError::Canceled,
        })
        .try_chunks_timeout(
            session_batch_buf,
            Duration::from_nanos(session_batch_min_time as u64),
            Duration::from_nanos(session_batch_max_time as u64),
        );

    let mut reply_receiver_list = Vec::with_capacity(session_batch_buf.get());
    let mut replies = Vec::with_capacity(session_batch_buf.get());
    let mut read_buf = VecDeque::with_capacity(session_batch_buf.get());

    loop {
        let reqs = if read_buf.is_empty() {
            match reader.next().await {
                Some(reqs) => reqs,
                None => return Ok(()),
            }
        } else {
            read_buf
                .drain(..min(read_buf.len(), session_batch_buf.get()))
                .collect()
        };

        for req in reqs.into_iter() {
            let packet = match req {
                Ok(packet) => packet,
                Err(err) => {
                    error!("session reader error {:?}", err);
                    return Err(err);
                }
            };
            let cmd = Command::new(packet);

            let fut = handler.handle_cmd(cmd);
            reply_receiver_list.push(fut);
        }

        for reply_receiver in reply_receiver_list.drain(..) {
            let res = {
                // reply_fut may block forever for some commands, such as BLPOP, BRPOP, BRPOPLPUSH.
                // Then even the connection is closed, this future won't exit.
                // We need to select it with tcp stream read to detect closed connection.
                let mut reply_fut = Some(reply_receiver);
                let res = loop {
                    let fut = reply_fut.take().ok_or_else(|| {
                        error!("session invalid state: cannot get reply_fut.");
                        SessionError::InvalidState
                    })?;
                    match future::select(fut, reader.next()).await {
                        future::Either::Left((res, read_fut)) => {
                            let _ = read_fut; // can be dropped without losing any item.
                            break res;
                        }
                        future::Either::Right((read_result, fut)) => {
                            reply_fut = Some(fut);
                            match read_result {
                                Some(reqs) => read_buf.extend(reqs),
                                None => return Ok(()),
                            }
                            continue;
                        }
                    }
                };
                res.map_err(SessionError::CmdErr)
            };

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

            replies.push(packet);
        }

        let mut batch = stream::iter(replies.drain(..)).map(Ok);
        if let Err(err) = writer.send_all(&mut batch).await {
            error!("writer error: {}", err);
            let err = match err {
                EncodeError::Io(err) => SessionError::Io(err),
                EncodeError::NotReady(_) => SessionError::InvalidState,
            };
            return Err(err);
        }
    }
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
    use std::convert::TryFrom;
    use tokio;

    #[tokio::test]
    async fn test_cmd_ctx_auto_send() {
        let request = RespPacket::Data(Resp::Arr(Array::Arr(vec![Resp::Bulk(BulkStr::Str(
            b"PING".to_vec(),
        ))])));
        let cluster_name = ClusterName::try_from("mycluster").unwrap();
        let cmd = Command::new(Box::new(request));
        let (sender, receiver) = new_command_pair(&cmd);
        let cmd_ctx = CmdCtx::new(cluster_name, cmd, sender, 7799, true);
        drop(cmd_ctx);
        let err = match receiver.await {
            Ok(_) => panic!(),
            Err(err) => err,
        };
        assert!(matches!(err, CommandError::Dropped));
    }
}
