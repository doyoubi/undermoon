use super::command::{CommandError, CommandResult};
use super::service::ServerProxyConfig;
use super::slowlog::TaskEvent;
use crate::common::batch::TryChunksTimeoutStreamExt;
use crate::common::utils::{resolve_first_address, ThreadSafe};
use crate::protocol::{
    new_simple_packet_codec, DecodeError, EncodeError, EncodedPacket, FromResp, MonoPacket,
    OptionalMulti, Packet, Resp, RespCodec, RespVec,
};
use futures::channel::mpsc;
use futures::{select, stream, Future, FutureExt, Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use futures_timer::Delay;
use std::boxed::Box;
use std::error::Error;
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::result::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio;
use tokio::net::TcpStream;
use tokio_util::codec::Decoder;

pub type BackendResult<T> = Result<T, BackendError>;
pub type CmdTaskResult = Result<RespVec, CommandError>;

pub trait CmdTaskResultHandler: Send + Sync + 'static {
    type Task: CmdTask;

    fn handle_task(
        &self,
        cmd_task: Self::Task,
        result: BackendResult<<Self::Task as CmdTask>::Pkt>,
    );
}

pub trait CmdTaskResultHandlerFactory: ThreadSafe {
    type Handler: CmdTaskResultHandler;

    fn create(&self) -> Self::Handler;
}

pub trait CmdTask: ThreadSafe {
    type Pkt: Packet + Send;
    type TaskType;
    type Context: ThreadSafe;

    fn get_key(&self) -> Option<&[u8]>;
    fn get_slot(&self) -> Option<usize>;
    fn set_result(self, result: CommandResult<Self::Pkt>);
    fn get_packet(&self) -> Self::Pkt;
    fn get_type(&self) -> Self::TaskType;
    fn get_context(&self) -> Self::Context;

    fn set_resp_result(self, result: Result<RespVec, CommandError>)
    where
        Self: Sized;

    fn log_event(&mut self, event: TaskEvent);
}

pub trait IntoTask<T: CmdTask>: CmdTask {
    fn into_task(self) -> T;
}

impl<T: CmdTask> IntoTask<T> for T {
    fn into_task(self) -> T {
        self
    }
}

pub trait CmdTaskFactory {
    type Task: CmdTask;

    fn create_with_ctx(
        &self,
        context: <Self::Task as CmdTask>::Context,
        resp: RespVec,
    ) -> (
        Self::Task,
        // TODO: return indexed resp
        Pin<Box<dyn Future<Output = CmdTaskResult> + Send + 'static>>,
    );
}

// Type alias can't work with trait bound so we need to define again,
// or we can use OptionalMulti.
pub enum ReqTask<T: CmdTask> {
    Simple(T),
    Multi(Vec<T>),
}

impl<T: CmdTask> From<T> for ReqTask<T> {
    fn from(t: T) -> ReqTask<T> {
        ReqTask::Simple(t)
    }
}

impl<T: CmdTask> CmdTask for ReqTask<T> {
    type Pkt = OptionalMulti<T::Pkt>;
    type TaskType = OptionalMulti<T::TaskType>;
    type Context = OptionalMulti<T::Context>;

    fn get_key(&self) -> Option<&[u8]> {
        match self {
            Self::Simple(t) => t.get_key(),
            Self::Multi(v) => {
                for t in v.iter() {
                    let opt = t.get_key();
                    if opt.is_some() {
                        return opt;
                    }
                }
                None
            }
        }
    }

    fn get_slot(&self) -> Option<usize> {
        match self {
            Self::Simple(t) => t.get_slot(),
            Self::Multi(v) => {
                for t in v.iter() {
                    let opt = t.get_slot();
                    if opt.is_some() {
                        return opt;
                    }
                }
                None
            }
        }
    }

    fn set_result(self, result: CommandResult<Self::Pkt>) {
        match self {
            Self::Simple(t) => match result {
                Ok(res) => match *res {
                    OptionalMulti::Single(r) => t.set_result(Ok(Box::new(r))),
                    OptionalMulti::Multi(_) => t.set_result(Err(CommandError::InnerError)),
                },
                Err(err) => t.set_result(Err(err)),
            },
            Self::Multi(v) => match result {
                Err(err) => {
                    for t in v.into_iter() {
                        t.set_result(Err(err.clone()))
                    }
                }
                Ok(res) => match *res {
                    OptionalMulti::Single(_) => {
                        for t in v {
                            t.set_result(Err(CommandError::InnerError))
                        }
                    }
                    OptionalMulti::Multi(results) => {
                        if v.len() != results.len() {
                            for t in v.into_iter() {
                                t.set_result(Err(CommandError::InnerError));
                            }
                            return;
                        }
                        for (t, r) in v.into_iter().zip(results) {
                            t.set_result(Ok(Box::new(r)));
                        }
                    }
                },
            },
        }
    }

    fn get_packet(&self) -> Self::Pkt {
        match self {
            Self::Simple(t) => OptionalMulti::Single(t.get_packet()),
            Self::Multi(v) => {
                let packets = v.iter().map(|t| t.get_packet()).collect();
                OptionalMulti::Multi(packets)
            }
        }
    }

    fn get_type(&self) -> Self::TaskType {
        match self {
            Self::Simple(task) => OptionalMulti::Single(task.get_type()),
            Self::Multi(tasks) => {
                OptionalMulti::Multi(tasks.iter().map(|t| t.get_type()).collect())
            }
        }
    }

    fn get_context(&self) -> Self::Context {
        match self {
            Self::Simple(task) => OptionalMulti::Single(task.get_context()),
            Self::Multi(tasks) => {
                OptionalMulti::Multi(tasks.iter().map(|t| t.get_context()).collect())
            }
        }
    }

    fn set_resp_result(self, result: Result<RespVec, CommandError>)
    where
        Self: Sized,
    {
        let hint = self.get_packet().get_hint();
        self.set_result(result.map(|resp| Box::new(Self::Pkt::from_resp(resp, hint))))
    }

    fn log_event(&mut self, event: TaskEvent) {
        match self {
            Self::Simple(t) => t.log_event(event),
            Self::Multi(v) => {
                for t in v.iter_mut() {
                    t.log_event(event);
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct BackendSendError<T>(T);

impl<T> BackendSendError<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

pub struct BackendNode<H: CmdTaskResultHandler> {
    tx: mpsc::UnboundedSender<H::Task>,
    conn_failed: Arc<AtomicBool>,
}

impl<H: CmdTaskResultHandler> BackendNode<H> {
    pub fn new<CF>(
        address: String,
        handler: Arc<H>,
        config: Arc<ServerProxyConfig>,
        conn_factory: Arc<CF>,
    ) -> (
        BackendNode<H>,
        impl Future<Output = Result<(), BackendError>> + Send,
    )
    where
        CF: ConnFactory + Send + Sync + 'static,
        CF::Pkt: Send,
        <H as CmdTaskResultHandler>::Task: CmdTask<Pkt = CF::Pkt>,
    {
        let (tx, rx) = mpsc::unbounded();
        let conn_failed = Arc::new(AtomicBool::new(false));
        let handle_backend_fut = handle_backend(
            handler,
            rx,
            conn_failed.clone(),
            address,
            config.backend_batch_min_time,
            config.backend_batch_max_time,
            config.backend_batch_buf,
            conn_factory,
        );
        (Self { tx, conn_failed }, handle_backend_fut)
    }

    pub fn send(&self, mut cmd_task: H::Task) -> Result<(), BackendSendError<H::Task>> {
        cmd_task.log_event(TaskEvent::SentToWritingQueue);
        if self.conn_failed.load(Ordering::SeqCst) {
            return Err(BackendSendError(cmd_task));
        }
        self.tx
            .unbounded_send(cmd_task)
            .map(|_| ())
            .map_err(|e| BackendSendError(e.into_inner()))
    }

    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

pub type ConnSink<T> = Pin<Box<dyn Sink<T, Error = BackendError> + Send>>;
pub type ConnStream<T> = Pin<Box<dyn Stream<Item = Result<T, BackendError>> + Send>>;
pub type CreateConnResult<T> = Result<(ConnSink<T>, ConnStream<T>), BackendError>;

pub trait ConnFactory: ThreadSafe {
    type Pkt: Packet;

    fn create_conn(
        &self,
        addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = CreateConnResult<Self::Pkt>> + Send>>;
}

pub struct DefaultConnFactory<P>(PhantomData<P>);

impl<P> Default for DefaultConnFactory<P> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<P: MonoPacket> ConnFactory for DefaultConnFactory<P> {
    type Pkt = P;

    fn create_conn(
        &self,
        addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = CreateConnResult<Self::Pkt>> + Send>> {
        Box::pin(create_conn(addr))
    }
}

async fn create_conn<T>(address: SocketAddr) -> CreateConnResult<T>
where
    T: MonoPacket,
{
    let socket = match TcpStream::connect(address).await {
        Ok(socket) => socket,
        Err(err) => {
            error!("failed to connect: {} {:?}", address, err);
            return Err(BackendError::Io(err));
        }
    };

    let (encoder, decoder) = new_simple_packet_codec::<T, T>();

    let frame = RespCodec::new(encoder, decoder).framed(socket);
    let (writer, reader) = frame.split();
    let writer = writer.sink_map_err(|e| match e {
        EncodeError::Io(err) => BackendError::Io(err),
        EncodeError::NotReady(_) => BackendError::InvalidState,
    });
    let reader = reader.map_err(|e| match e {
        DecodeError::InvalidProtocol => {
            error!("backend: invalid protocol");
            BackendError::InvalidProtocol
        }
        DecodeError::Io(e) => {
            error!("backend: io error: {:?}", e);
            BackendError::Io(e)
        }
    });

    Ok((Box::pin(writer), Box::pin(reader)))
}

const MAX_BACKEND_RETRY: usize = 3;

struct RetryState<T: CmdTask> {
    retry_times: usize,
    tasks: Vec<T>,
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_backend<H, F>(
    handler: Arc<H>,
    task_receiver: mpsc::UnboundedReceiver<H::Task>,
    conn_failed: Arc<AtomicBool>,
    address: String,
    backend_batch_min_time: usize,
    backend_batch_max_time: usize,
    backend_batch_buf: NonZeroUsize,
    conn_factory: Arc<F>,
) -> Result<(), BackendError>
where
    H: CmdTaskResultHandler,
    F: ConnFactory<Pkt = <H::Task as CmdTask>::Pkt> + Send + Sync + 'static,
{
    // TODO: move this to upper layer.
    let sock_address = match resolve_first_address(&address) {
        Some(addr) => addr,
        None => {
            error!("invalid address: {:?}", address);
            return Err(BackendError::InvalidAddress);
        }
    };

    let mut retry_state: Option<RetryState<H::Task>> = None;

    let batch_min_time = Duration::from_nanos(backend_batch_min_time as u64);
    let batch_max_time = Duration::from_nanos(backend_batch_max_time as u64);
    let mut task_receiver = task_receiver
        .try_chunks_timeout(backend_batch_buf, batch_min_time, batch_max_time)
        .fuse();

    loop {
        let (writer, reader) = match conn_factory.create_conn(sock_address).await {
            Ok(conn) => conn,
            Err(err) => {
                conn_failed.store(true, Ordering::SeqCst);
                error!("failed to connect: {} {:?}", address, err);
                retry_state.take();

                let mut timeout_fut = Delay::new(Duration::from_secs(1)).fuse();
                loop {
                    let mut tasks_fut = task_receiver.next().fuse();
                    let tasks_opt = select! {
                        () = timeout_fut => break,
                        tasks_opt = tasks_fut => tasks_opt,
                    };
                    let tasks = match tasks_opt {
                        Some(tasks) => tasks,
                        None => {
                            warn!("backend sender is closed. Exit backend connection handling.");
                            return Err(BackendError::Canceled);
                        }
                    };
                    for task in tasks.into_iter() {
                        task.set_resp_result(Ok(Resp::Error(
                            format!("failed to connect to {}", address).into_bytes(),
                        )))
                    }
                }
                continue;
            }
        };
        conn_failed.store(false, Ordering::SeqCst);

        let res = handle_conn(
            writer,
            reader,
            &mut task_receiver,
            handler.clone(),
            backend_batch_buf,
            retry_state.take(),
        )
        .await;
        match res {
            Ok(()) => {
                warn!("task receiver is closed");
                return Err(BackendError::Canceled);
            }
            Err((err, state)) => {
                error!("connection is closed: {:?}", err);
                retry_state = state;
                continue;
            }
        }
    }
}

async fn handle_conn<H, S>(
    mut writer: ConnSink<<<H as CmdTaskResultHandler>::Task as CmdTask>::Pkt>,
    mut reader: ConnStream<<<H as CmdTaskResultHandler>::Task as CmdTask>::Pkt>,
    task_receiver: &mut S,
    handler: Arc<H>,
    backend_batch_buf: NonZeroUsize,
    mut retry_state_opt: Option<RetryState<H::Task>>,
) -> Result<(), (BackendError, Option<RetryState<H::Task>>)>
where
    H: CmdTaskResultHandler,
    S: Stream<Item = Vec<H::Task>> + Unpin,
{
    let mut packets = Vec::with_capacity(backend_batch_buf.get());

    loop {
        let (retry_times_opt, mut tasks) = match retry_state_opt.take() {
            Some(RetryState { retry_times, tasks }) => (Some(retry_times), tasks),
            None => {
                let tasks = match task_receiver.next().await {
                    Some(tasks) => tasks,
                    None => return Ok(()),
                };
                (None, tasks)
            }
        };

        for task in tasks.iter_mut() {
            task.log_event(TaskEvent::WritingQueueReceived);
            packets.push(task.get_packet());
        }

        let mut batch = stream::iter(packets.drain(..)).map(Ok);
        let res = writer.send_all(&mut batch).await;

        for task in tasks.iter_mut() {
            task.log_event(TaskEvent::SentToBackend);
        }

        if let Err(err) = res {
            error!("backend write error: {}", err);
            let retry_state = handle_conn_err(retry_times_opt, tasks, &err);
            return Err((err, retry_state));
        }

        let mut tasks_iter = tasks.into_iter();
        // `while let` will consume ownership.
        #[allow(clippy::while_let_loop)]
        loop {
            let mut task = match tasks_iter.next() {
                Some(task) => task,
                None => break,
            };
            let packet_res = match reader.next().await {
                Some(pkt) => pkt,
                None => {
                    error!("Failed to read packet. Connection is closed.");
                    let mut failed_tasks = vec![task];
                    failed_tasks.extend(tasks_iter);
                    let err = BackendError::Io(io::Error::from(io::ErrorKind::BrokenPipe));
                    let retry_state = handle_conn_err(retry_times_opt, failed_tasks, &err);
                    return Err((err, retry_state));
                }
            };

            task.log_event(TaskEvent::ReceivedFromBackend);
            handler.handle_task(task, packet_res);
        }
    }
}

fn handle_conn_err<T: CmdTask>(
    retry_times_opt: Option<usize>,
    tasks: Vec<T>,
    err: &BackendError,
) -> Option<RetryState<T>> {
    let retry_times = retry_times_opt.unwrap_or(0);
    if retry_times >= MAX_BACKEND_RETRY {
        for task in tasks.into_iter() {
            let cmd_err = match err {
                BackendError::Io(e) => CommandError::Io(io::Error::from(e.kind())),
                others => {
                    error!("unexpected backend error: {:?}", others);
                    CommandError::InnerError
                }
            };
            task.set_result(Err(cmd_err));
        }
        None
    } else {
        let retry_state = RetryState {
            retry_times: retry_times + 1,
            tasks,
        };
        Some(retry_state)
    }
}

#[derive(Debug)]
pub enum BackendError {
    Io(io::Error),
    NodeNotFound,
    InvalidProtocol,
    InvalidAddress,
    Canceled,
    InvalidState,
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

    fn cause(&self) -> Option<&dyn Error> {
        match self {
            BackendError::Io(err) => Some(err),
            _ => None,
        }
    }
}
