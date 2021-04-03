use super::backend::{
    BackendError, BackendNode, CmdTask, CmdTaskResultHandler, CmdTaskResultHandlerFactory,
    ConnFactory, ReqTask, SenderBackendError,
};
use super::service::ServerProxyConfig;
use crate::common::batch::BatchStats;
use crate::common::response;
use crate::common::track::TrackedFutureRegistry;
use crate::protocol::Resp;
use either::Either;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

pub trait CmdTaskSender {
    type Task: CmdTask;

    fn send(&self, cmd_task: Self::Task) -> Result<(), SenderBackendError<Self::Task>>;
}

pub trait CmdTaskSenderFactory {
    type Sender: CmdTaskSender;

    fn create(&self, address: String) -> Self::Sender;
}

pub struct RecoverableBackendNode<F: CmdTaskResultHandlerFactory> {
    address: String,
    node: BackendNode<<F as CmdTaskResultHandlerFactory>::Handler>,
}

impl<F: CmdTaskResultHandlerFactory> CmdTaskSender for RecoverableBackendNode<F> {
    type Task = <<F as CmdTaskResultHandlerFactory>::Handler as CmdTaskResultHandler>::Task;

    fn send(&self, cmd_task: Self::Task) -> Result<(), SenderBackendError<Self::Task>> {
        self.node.send(cmd_task).map_err(|e| {
            let cmd_task = e.into_inner();
            cmd_task.set_resp_result(Ok(Resp::Error(
                format!("{}: {}", response::ERR_BACKEND_CONNECTION, self.address).into_bytes(),
            )));
            error!("backend node is closed");
            SenderBackendError::Canceled
        })
    }
}

pub struct RecoverableBackendNodeFactory<F: CmdTaskResultHandlerFactory, CF: ConnFactory>
where
    <F::Handler as CmdTaskResultHandler>::Task: CmdTask<Pkt = CF::Pkt>,
    CF::Pkt: Send,
{
    config: Arc<ServerProxyConfig>,
    handler_factory: Arc<F>,
    conn_factory: Arc<CF>,
    future_registry: Arc<TrackedFutureRegistry>,
    batch_stats: Arc<BatchStats>,
}

impl<F: CmdTaskResultHandlerFactory, CF: ConnFactory> RecoverableBackendNodeFactory<F, CF>
where
    <F::Handler as CmdTaskResultHandler>::Task: CmdTask<Pkt = CF::Pkt>,
    CF::Pkt: Send,
{
    pub fn new(
        config: Arc<ServerProxyConfig>,
        handler_factory: Arc<F>,
        conn_factory: Arc<CF>,
        future_registry: Arc<TrackedFutureRegistry>,
        batch_stats: Arc<BatchStats>,
    ) -> Self {
        Self {
            config,
            handler_factory,
            conn_factory,
            future_registry,
            batch_stats,
        }
    }
}

impl<F: CmdTaskResultHandlerFactory, CF: ConnFactory> CmdTaskSenderFactory
    for RecoverableBackendNodeFactory<F, CF>
where
    <F::Handler as CmdTaskResultHandler>::Task: CmdTask<Pkt = CF::Pkt>,
    CF::Pkt: Send,
{
    type Sender = RecoverableBackendNode<F>;

    fn create(&self, address: String) -> Self::Sender {
        let (node, fut) = BackendNode::new(
            address.clone(),
            Arc::new(self.handler_factory.create()),
            self.config.clone(),
            self.conn_factory.clone(),
            self.batch_stats.clone(),
        );
        let desc = format!("backend::RecoverableBackendNode: address={}", address);
        let fut = TrackedFutureRegistry::wrap(self.future_registry.clone(), fut, desc);
        tokio::spawn(fut);
        Self::Sender { address, node }
    }
}

pub struct ReqAdaptorSender<S: CmdTaskSender> {
    sender: S,
}

impl<S: CmdTaskSender> ReqAdaptorSender<S> {
    pub fn new(sender: S) -> Self {
        Self { sender }
    }

    pub fn inner_sender(&self) -> &S {
        &self.sender
    }
}

impl<S: CmdTaskSender> CmdTaskSender for ReqAdaptorSender<S> {
    type Task = ReqTask<S::Task>;

    fn send(&self, cmd_task: Self::Task) -> Result<(), SenderBackendError<Self::Task>> {
        match cmd_task {
            ReqTask::Simple(t) => self
                .sender
                .send(t)
                .map_err(|err| err.map_task(ReqTask::Simple)),
            ReqTask::Multi(ts) => {
                let mut retry_tasks = vec![];
                let mut it = ts.into_iter();
                while let Some(t) = it.next() {
                    if let Err(err) = self.sender.send(t) {
                        match BackendError::from_sender_backend_error(err) {
                            Either::Right(retry_err) => {
                                retry_tasks.push(retry_err.into_inner());
                            }
                            Either::Left(backend_err) => {
                                let err_str = format!(
                                    "{}: {}",
                                    response::ERR_MULTI_KEY_PARTIAL_ERROR,
                                    backend_err
                                );
                                // Explicitly set result for the remaining commands
                                // so that they won't be dropped implicitly.
                                for t in it.chain(retry_tasks.into_iter()) {
                                    t.set_resp_result(Ok(Resp::Error(
                                        err_str.clone().into_bytes(),
                                    )));
                                }
                                return Err(SenderBackendError::from_backend_error(backend_err));
                            }
                        }
                    }
                }
                if !retry_tasks.is_empty() {
                    return Err(SenderBackendError::Retry(ReqTask::Multi(retry_tasks)));
                }
                Ok(())
            }
        }
    }
}

pub struct ReqAdaptorSenderFactory<F: CmdTaskSenderFactory> {
    inner_factory: F,
}

impl<F: CmdTaskSenderFactory> ReqAdaptorSenderFactory<F> {
    pub fn new(inner_factory: F) -> Self {
        Self { inner_factory }
    }
}

impl<F: CmdTaskSenderFactory> CmdTaskSenderFactory for ReqAdaptorSenderFactory<F> {
    type Sender = ReqAdaptorSender<F::Sender>;

    fn create(&self, address: String) -> Self::Sender {
        let sender = self.inner_factory.create(address);
        Self::Sender { sender }
    }
}

// Round robin sender.
pub struct RoundRobinSenderGroup<S: CmdTaskSender> {
    senders: Vec<S>,
    cursor: AtomicUsize,
}

impl<S: CmdTaskSender> CmdTaskSender for RoundRobinSenderGroup<S> {
    type Task = S::Task;

    fn send(&self, cmd_task: Self::Task) -> Result<(), SenderBackendError<Self::Task>> {
        let index = self.cursor.fetch_add(1, Ordering::SeqCst);
        let sender = match self.senders.get(index % self.senders.len()) {
            Some(s) => s,
            None => return Err(SenderBackendError::NodeNotFound),
        };
        sender.send(cmd_task)
    }
}

pub struct RoundRobinSenderGroupFactory<F: CmdTaskSenderFactory> {
    group_size: NonZeroUsize,
    inner_factory: F,
}

impl<F: CmdTaskSenderFactory> RoundRobinSenderGroupFactory<F> {
    pub fn new(group_size: NonZeroUsize, inner_factory: F) -> Self {
        Self {
            group_size,
            inner_factory,
        }
    }
}

impl<F: CmdTaskSenderFactory> CmdTaskSenderFactory for RoundRobinSenderGroupFactory<F> {
    type Sender = RoundRobinSenderGroup<F::Sender>;

    fn create(&self, address: String) -> Self::Sender {
        let mut senders = Vec::new();
        for _ in 0..self.group_size.get() {
            senders.push(self.inner_factory.create(address.clone()));
        }
        Self::Sender {
            senders,
            cursor: AtomicUsize::new(0),
        }
    }
}

pub struct CachedSender<S: CmdTaskSender> {
    inner_sender: Arc<S>,
}

impl<S: CmdTaskSender> CmdTaskSender for CachedSender<S> {
    type Task = S::Task;

    fn send(&self, cmd_task: Self::Task) -> Result<(), SenderBackendError<Self::Task>> {
        self.inner_sender.send(cmd_task)
    }
}

// TODO: support cleanup here to avoid memory leak.
pub struct CachedSenderFactory<F: CmdTaskSenderFactory> {
    inner_factory: F,
    cached_senders: Arc<parking_lot::RwLock<HashMap<String, Weak<F::Sender>>>>,
}

impl<F: CmdTaskSenderFactory> CachedSenderFactory<F> {
    pub fn new(inner_factory: F) -> Self {
        Self {
            inner_factory,
            cached_senders: Arc::new(parking_lot::RwLock::new(HashMap::new())),
        }
    }
}

impl<F: CmdTaskSenderFactory> CmdTaskSenderFactory for CachedSenderFactory<F> {
    type Sender = CachedSender<F::Sender>;

    fn create(&self, address: String) -> Self::Sender {
        if let Some(sender) = self
            .cached_senders
            .read()
            .get(&address)
            .and_then(|sender_weak| sender_weak.upgrade())
        {
            return CachedSender {
                inner_sender: sender,
            };
        }

        // Acceptable race condition here. Multiple threads might be creating at the same time.
        let inner_sender = Arc::new(self.inner_factory.create(address.clone()));
        let inner_sender_clone = {
            let mut guard = self.cached_senders.write();
            let inner_sender_weak = Arc::downgrade(&inner_sender);
            guard.entry(address).or_insert(inner_sender_weak);
            inner_sender
        };

        CachedSender {
            inner_sender: inner_sender_clone,
        }
    }
}

pub type BackendSenderFactory<F, CF> =
    CachedSenderFactory<RoundRobinSenderGroupFactory<RecoverableBackendNodeFactory<F, CF>>>;

pub fn gen_sender_factory<F: CmdTaskResultHandlerFactory, CF: ConnFactory>(
    config: Arc<ServerProxyConfig>,
    reply_handler_factory: Arc<F>,
    conn_factory: Arc<CF>,
    future_registry: Arc<TrackedFutureRegistry>,
    batch_stats: Arc<BatchStats>,
) -> BackendSenderFactory<F, CF>
where
    <F::Handler as CmdTaskResultHandler>::Task: CmdTask<Pkt = CF::Pkt>,
    CF::Pkt: Send,
{
    CachedSenderFactory::new(RoundRobinSenderGroupFactory::new(
        config.backend_conn_num,
        RecoverableBackendNodeFactory::new(
            config.clone(),
            reply_handler_factory,
            conn_factory,
            future_registry,
            batch_stats,
        ),
    ))
}

pub type MigrationBackendSenderFactory<F, CF> =
    RoundRobinSenderGroupFactory<ReqAdaptorSenderFactory<RecoverableBackendNodeFactory<F, CF>>>;

pub fn gen_migration_sender_factory<F: CmdTaskResultHandlerFactory, CF: ConnFactory>(
    config: Arc<ServerProxyConfig>,
    reply_handler_factory: Arc<F>,
    conn_factory: Arc<CF>,
    future_registry: Arc<TrackedFutureRegistry>,
    batch_stats: Arc<BatchStats>,
) -> MigrationBackendSenderFactory<F, CF>
where
    <F::Handler as CmdTaskResultHandler>::Task: CmdTask<Pkt = CF::Pkt>,
    CF::Pkt: Send,
{
    RoundRobinSenderGroupFactory::new(
        config.backend_conn_num,
        ReqAdaptorSenderFactory::new(RecoverableBackendNodeFactory::new(
            config.clone(),
            reply_handler_factory,
            conn_factory,
            future_registry,
            batch_stats,
        )),
    )
}
