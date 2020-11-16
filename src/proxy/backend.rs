use super::command::{CommandError, CommandResult};
use super::service::ServerProxyConfig;
use super::slowlog::TaskEvent;
use crate::common::utils::{resolve_first_address, RetryError, ThreadSafe};
use crate::common::batch::BatchState;
use crate::protocol::{
    new_simple_packet_codec, DecodeError, EncodeError, EncodedPacket, FromResp, MonoPacket,
    OptionalMulti, Packet, Resp, RespCodec, RespVec, PacketSizeHint,
};
use either::Either;
use futures::channel::mpsc;
use futures::task::{Context, Poll};
use futures::{future, select, Future, FutureExt, Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use futures_timer::Delay;
use std::boxed::Box;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::pin::Pin;
use std::result::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
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
        _config: Arc<ServerProxyConfig>,
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
        let handle_backend_fut =
            handle_backend(handler, rx, conn_failed.clone(), address, conn_factory);
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

    if let Err(err) = socket.set_nodelay(true) {
        error!("failed to set TCP_NODELAY for backend: {:?}", err);
        return Err(BackendError::Io(err));
    }

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

pub async fn handle_backend<H, F>(
    handler: Arc<H>,
    mut task_receiver: mpsc::UnboundedReceiver<H::Task>,
    conn_failed: Arc<AtomicBool>,
    address: String,
    conn_factory: Arc<F>,
) -> Result<(), BackendError>
where
    H: CmdTaskResultHandler,
    F: ConnFactory<Pkt = <H::Task as CmdTask>::Pkt> + Send + Sync + 'static,
{
    // TODO: move this to upper layer.
    let sock_address = match resolve_first_address(&address).await {
        Some(addr) => addr,
        None => {
            error!("invalid address: {:?}", address);
            return Err(BackendError::InvalidAddress);
        }
    };

    let mut retry_state: Option<RetryState<H::Task>> = None;

    loop {
        let (writer, reader) = match conn_factory.create_conn(sock_address).await {
            Ok(conn) => conn,
            Err(err) => {
                conn_failed.store(true, Ordering::SeqCst);
                error!("failed to connect: {} {:?}", address, err);
                if let Some(RetryState { tasks, .. }) = retry_state.take() {
                    for task in tasks.into_iter() {
                        task.set_resp_result(Err(CommandError::Canceled));
                    }
                }

                let mut timeout_fut = Delay::new(Duration::from_secs(1)).fuse();
                loop {
                    let mut tasks_fut = task_receiver.next().fuse();
                    let task_opt = select! {
                        () = timeout_fut => break,
                        task_opt = tasks_fut => task_opt,
                    };
                    let task = match task_opt {
                        Some(task) => task,
                        None => {
                            warn!("backend sender is closed. Exit backend connection handling.");
                            return Err(BackendError::Canceled);
                        }
                    };
                    task.set_resp_result(Ok(Resp::Error(
                        format!("failed to connect to {}", address).into_bytes(),
                    )))
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

type HandleConnErr<Task> = (BackendError, Option<RetryState<Task>>);

async fn handle_conn<H, S>(
    mut writer: ConnSink<<<H as CmdTaskResultHandler>::Task as CmdTask>::Pkt>,
    mut reader: ConnStream<<<H as CmdTaskResultHandler>::Task as CmdTask>::Pkt>,
    mut task_receiver: &mut S,
    handler: Arc<H>,
    mut retry_state_opt: Option<RetryState<H::Task>>,
) -> Result<(), HandleConnErr<H::Task>>
where
    H: CmdTaskResultHandler,
    S: Stream<Item = H::Task> + Unpin,
{
    const BUF_SIZE: usize = 64;
    let mut tasks = VecDeque::with_capacity(BUF_SIZE);
    let mut packets = VecDeque::with_capacity(BUF_SIZE);

    let mut batch_state = BatchState::new(4096 * 2, Duration::from_nanos(400_000));

    future::poll_fn(
        |cx: &mut Context<'_>| -> Poll<Result<(), HandleConnErr<H::Task>>> {
            let retry_times_opt = match retry_state_opt.take() {
                Some(RetryState {
                    retry_times,
                    tasks: mut retry_tasks,
                }) => {
                    for task in retry_tasks.iter_mut() {
                        task.log_event(TaskEvent::WritingQueueReceived);
                        packets.push_back(task.get_packet());
                    }
                    tasks.extend(retry_tasks.drain(..));
                    Some(retry_times)
                }
                None => None,
            };

            while let Poll::Ready(task_opt) = Pin::new(&mut task_receiver).poll_next(cx) {
                match task_opt {
                    Some(mut task) => {
                        task.log_event(TaskEvent::WritingQueueReceived);
                        packets.push_back(task.get_packet());
                        tasks.push_back(task);
                    }
                    None => {
                        info!("backend task_receiver is closed");
                        return Poll::Ready(Ok(()));
                    }
                }
            }

            let write_res = loop {
                match writer.as_mut().poll_ready(cx) {
                    Poll::Pending => break Ok(()),
                    Poll::Ready(Ok(())) => (),
                    Poll::Ready(Err(err)) => break Err(err),
                }

                match packets.pop_front() {
                    Some(packet) => {
                        // Zero size hint only affects performance.
                        let n = packet.get_size_hint().unwrap_or_else(|| {
                            error!("FATAL: unexpected None size hint");
                            0
                        });
                        batch_state.add_content_size(n);

                        if let Err(err) = writer.as_mut().start_send(packet) {
                            break Err(err);
                        }

                        let task_index_opt = tasks.len().checked_sub(packets.len() + 1);
                        match task_index_opt.and_then(|index| tasks.get_mut(index)) {
                            Some(task) => task.log_event(TaskEvent::SentToBackend),
                            None => {
                                error!(
                                    "InvalidState: invalid task index {} - {} - 1",
                                    tasks.len(),
                                    packets.len()
                                );
                                break Err(BackendError::InvalidState);
                            }
                        }
                    }
                    None => {
                        let now = coarsetime::Instant::now();
                        if !batch_state.need_flush(cx, now) {
                            break Ok(());
                        }

                        match writer.as_mut().poll_flush(cx) {
                            Poll::Pending => break Ok(()),
                            Poll::Ready(Ok(())) => {
                                batch_state.reset(now);
                                break Ok(());
                            },
                            Poll::Ready(Err(err)) => break Err(err),
                        }
                    }
                };
            };

            if let Err(err) = write_res {
                error!("backend write error: {}", err);
                let failed_tasks = tasks.drain(..).collect();
                let retry_state = handle_conn_err(retry_times_opt, failed_tasks, &err);
                return Poll::Ready(Err((err, retry_state)));
            }

            let read_res = loop {
                let packet_res = match Pin::new(&mut reader).poll_next(cx) {
                    Poll::Ready(None) => {
                        info!("backend is closed by peer");
                        break Err(BackendError::Canceled);
                    }
                    Poll::Ready(Some(packet_res)) => packet_res,
                    Poll::Pending => break Ok(()),
                };

                let mut task = match tasks.pop_front() {
                    Some(task) => task,
                    None => {
                        error!("Invalid state, can't get task when reading");
                        break Err(BackendError::InvalidState);
                    }
                };
                task.log_event(TaskEvent::ReceivedFromBackend);
                handler.handle_task(task, packet_res);
            };

            if let Err(err) = read_res {
                error!("Failed to read packet. Connection is closed.");
                let failed_tasks = tasks.drain(..).collect();
                let retry_state = handle_conn_err(retry_times_opt, failed_tasks, &err);
                return Poll::Ready(Err((err, retry_state)));
            }

            Poll::Pending
        },
    )
    .await
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

impl BackendError {
    pub fn from_sender_backend_error<T>(err: SenderBackendError<T>) -> Either<Self, RetryError<T>> {
        match err {
            SenderBackendError::Io(io_err) => Either::Left(BackendError::Io(io_err)),
            SenderBackendError::NodeNotFound => Either::Left(BackendError::NodeNotFound),
            SenderBackendError::InvalidProtocol => Either::Left(BackendError::InvalidProtocol),
            SenderBackendError::InvalidAddress => Either::Left(BackendError::InvalidAddress),
            SenderBackendError::Canceled => Either::Left(BackendError::Canceled),
            SenderBackendError::InvalidState => Either::Left(BackendError::InvalidState),
            SenderBackendError::Retry(task) => Either::Right(RetryError::new(task)),
        }
    }
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

pub enum SenderBackendError<T> {
    Io(io::Error),
    NodeNotFound,
    InvalidProtocol,
    InvalidAddress,
    Canceled,
    InvalidState,
    Retry(T),
}

impl<T> SenderBackendError<T> {
    pub fn from_backend_error(err: BackendError) -> Self {
        match err {
            BackendError::Io(io_err) => SenderBackendError::Io(io_err),
            BackendError::NodeNotFound => SenderBackendError::NodeNotFound,
            BackendError::InvalidProtocol => SenderBackendError::InvalidProtocol,
            BackendError::InvalidAddress => SenderBackendError::InvalidAddress,
            BackendError::Canceled => SenderBackendError::Canceled,
            BackendError::InvalidState => SenderBackendError::InvalidState,
        }
    }

    pub fn map_task<P, F>(self: Self, f: F) -> SenderBackendError<P>
    where
        P: CmdTask,
        F: Fn(T) -> P,
    {
        match self {
            Self::Io(io_err) => SenderBackendError::Io(io_err),
            Self::NodeNotFound => SenderBackendError::NodeNotFound,
            Self::InvalidProtocol => SenderBackendError::InvalidProtocol,
            Self::InvalidAddress => SenderBackendError::InvalidAddress,
            Self::Canceled => SenderBackendError::Canceled,
            Self::InvalidState => SenderBackendError::InvalidState,
            Self::Retry(task) => SenderBackendError::Retry(f(task)),
        }
    }
}

impl<T> fmt::Debug for SenderBackendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Io(io_err) => write!(f, "BackendError::Io({:?})", io_err),
            Self::NodeNotFound => write!(f, "backendError::NodeNotFound"),
            Self::InvalidProtocol => write!(f, "backendError::InvalidProtocol"),
            Self::InvalidAddress => write!(f, "backendError::InvalidAddress"),
            Self::Canceled => write!(f, "backendError::Canceled"),
            Self::InvalidState => write!(f, "backendError::InvalidState"),
            Self::Retry(_) => write!(f, "BackendError::Retry"),
        }
    }
}

impl<T> fmt::Display for SenderBackendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<T> Error for SenderBackendError<T> {
    fn description(&self) -> &str {
        "sender backend error"
    }

    fn cause(&self) -> Option<&dyn Error> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    struct CounterHandler {
        counter: Arc<AtomicUsize>,
        replies: Mutex<Vec<RespVec>>,
    }

    impl CounterHandler {
        fn new() -> Self {
            let counter = Arc::new(AtomicUsize::new(0));
            Self {
                counter,
                replies: Mutex::new(vec![]),
            }
        }

        fn get_count(&self) -> usize {
            self.counter.load(Ordering::SeqCst)
        }

        fn get_replies(&self) -> Vec<RespVec> {
            self.replies.lock().unwrap().clone()
        }
    }

    impl CmdTaskResultHandler for CounterHandler {
        type Task = DummyCmdTask;

        fn handle_task(
            &self,
            _cmd_task: Self::Task,
            result: BackendResult<<Self::Task as CmdTask>::Pkt>,
        ) {
            self.counter.fetch_add(1, Ordering::SeqCst);
            self.replies.lock().unwrap().push(result.unwrap());
        }
    }

    struct DummyCmdTask {
        resp: RespVec,
    }

    impl DummyCmdTask {
        fn new(resp: RespVec) -> Self {
            Self { resp }
        }
    }

    impl CmdTask for DummyCmdTask {
        type Pkt = RespVec;
        type TaskType = u64;
        type Context = u32;

        fn get_key(&self) -> Option<&'static [u8]> {
            None
        }
        fn get_slot(&self) -> Option<usize> {
            None
        }
        fn set_result(self, _result: CommandResult<RespVec>) {}
        fn get_packet(&self) -> RespVec {
            self.resp.clone()
        }
        fn get_type(&self) -> u64 {
            0
        }
        fn get_context(&self) -> u32 {
            0
        }

        fn set_resp_result(self, _result: Result<RespVec, CommandError>)
        where
            Self: Sized,
        {
        }

        fn log_event(&mut self, _event: TaskEvent) {}
    }

    #[tokio::test]
    async fn test_handle_conn() {
        let (sender, receiver) = mpsc::unbounded::<RespVec>();
        let writer: ConnSink<RespVec> = Box::pin(sender.sink_map_err(|_| BackendError::Canceled));
        let reader: ConnStream<RespVec> = Box::pin(receiver.map(Ok));

        let (mut task_sender, mut task_receiver) = mpsc::unbounded();
        for i in 0..3 {
            let i: usize = i;
            task_sender
                .send(DummyCmdTask::new(RespVec::Simple(
                    i.to_string().into_bytes(),
                )))
                .await
                .unwrap();
        }

        let handler = Arc::new(CounterHandler::new());
        let handler2 = handler.clone();

        tokio::spawn(async move {
            while handler2.get_count() < 3 {
                Delay::new(Duration::from_millis(20)).await;
            }
            task_sender.close().await.unwrap();
        });

        let res = handle_conn(writer, reader, &mut task_receiver, handler.clone(), None).await;

        assert!(res.is_ok());
        let replies = handler.get_replies();
        assert_eq!(replies.len(), 3);
        for (i, reply) in replies.into_iter().enumerate() {
            let content = i.to_string().into_bytes();
            match reply {
                RespVec::Simple(b) => assert_eq!(b, content),
                _ => assert!(false),
            }
        }
    }

    #[tokio::test]
    async fn test_handle_conn_with_retry() {
        let (sender, receiver) = mpsc::unbounded::<RespVec>();
        let writer: ConnSink<RespVec> = Box::pin(sender.sink_map_err(|_| BackendError::Canceled));
        let reader: ConnStream<RespVec> = Box::pin(receiver.map(Ok));

        let (mut task_sender, mut task_receiver) = mpsc::unbounded();

        let tasks = (0..3)
            .map(|i| DummyCmdTask::new(RespVec::Simple(i.to_string().into_bytes())))
            .collect();
        let retry = RetryState {
            retry_times: 0,
            tasks,
        };

        for i in 3..6 {
            let i: usize = i;
            task_sender
                .send(DummyCmdTask::new(RespVec::Simple(
                    i.to_string().into_bytes(),
                )))
                .await
                .unwrap();
        }

        let handler = Arc::new(CounterHandler::new());
        let handler2 = handler.clone();

        tokio::spawn(async move {
            while handler2.get_count() < 6 {
                Delay::new(Duration::from_millis(20)).await;
            }
            task_sender.close().await.unwrap();
        });

        let res = handle_conn(
            writer,
            reader,
            &mut task_receiver,
            handler.clone(),
            Some(retry),
        )
        .await;

        assert!(res.is_ok());
        let replies = handler.get_replies();
        assert_eq!(replies.len(), 6);
        for (i, reply) in replies.into_iter().enumerate() {
            let content = i.to_string().into_bytes();
            match reply {
                RespVec::Simple(b) => assert_eq!(b, content),
                _ => assert!(false),
            }
        }
    }
}
