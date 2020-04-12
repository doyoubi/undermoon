use super::backend::{
    BackendError, BackendNode, CmdTask, CmdTaskResultHandler, CmdTaskResultHandlerFactory,
    ConnFactory, ReqTask,
};
use super::service::ServerProxyConfig;
use crate::common::response::ERR_BACKEND_CONNECTION;
use crate::common::track::TrackedFutureRegistry;
use crate::protocol::Resp;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};

pub trait CmdTaskSender {
    type Task: CmdTask;

    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError>;
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

    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError> {
        self.node.send(cmd_task).map_err(|e| {
            let cmd_task = e.into_inner();
            cmd_task.set_resp_result(Ok(Resp::Error(
                format!("{}: {}", ERR_BACKEND_CONNECTION, self.address).into_bytes(),
            )));
            error!("backend node is closed");
            BackendError::Canceled
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
    ) -> Self {
        Self {
            config,
            handler_factory,
            conn_factory,
            future_registry,
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

    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError> {
        match cmd_task {
            ReqTask::Simple(t) => self.sender.send(t),
            ReqTask::Multi(ts) => {
                for t in ts.into_iter() {
                    self.sender.send(t)?;
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
pub struct RRSenderGroup<S: CmdTaskSender> {
    senders: Vec<S>,
    cursor: AtomicUsize,
}

impl<S: CmdTaskSender> CmdTaskSender for RRSenderGroup<S> {
    type Task = S::Task;

    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError> {
        let index = self.cursor.fetch_add(1, Ordering::SeqCst);
        let sender = match self.senders.get(index % self.senders.len()) {
            Some(s) => s,
            None => return Err(BackendError::NodeNotFound),
        };
        sender.send(cmd_task)
    }
}

pub struct RRSenderGroupFactory<F: CmdTaskSenderFactory> {
    group_size: NonZeroUsize,
    inner_factory: F,
}

impl<F: CmdTaskSenderFactory> RRSenderGroupFactory<F> {
    pub fn new(group_size: NonZeroUsize, inner_factory: F) -> Self {
        Self {
            group_size,
            inner_factory,
        }
    }
}

impl<F: CmdTaskSenderFactory> CmdTaskSenderFactory for RRSenderGroupFactory<F> {
    type Sender = RRSenderGroup<F::Sender>;

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

    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError> {
        self.inner_sender.send(cmd_task)
    }
}

// TODO: support cleanup here to avoid memory leak.
pub struct CachedSenderFactory<F: CmdTaskSenderFactory> {
    inner_factory: F,
    cached_senders: Arc<RwLock<HashMap<String, Weak<F::Sender>>>>,
}

impl<F: CmdTaskSenderFactory> CachedSenderFactory<F> {
    pub fn new(inner_factory: F) -> Self {
        Self {
            inner_factory,
            cached_senders: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl<F: CmdTaskSenderFactory> CmdTaskSenderFactory for CachedSenderFactory<F> {
    type Sender = CachedSender<F::Sender>;

    fn create(&self, address: String) -> Self::Sender {
        if let Some(sender) = self
            .cached_senders
            .read()
            .expect("CachedSenderFactory::create")
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
            let mut guard = self
                .cached_senders
                .write()
                .expect("CachedSenderFactory::create");
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
    CachedSenderFactory<RRSenderGroupFactory<RecoverableBackendNodeFactory<F, CF>>>;

pub fn gen_sender_factory<F: CmdTaskResultHandlerFactory, CF: ConnFactory>(
    config: Arc<ServerProxyConfig>,
    reply_handler_factory: Arc<F>,
    conn_factory: Arc<CF>,
    future_registry: Arc<TrackedFutureRegistry>,
) -> BackendSenderFactory<F, CF>
where
    <F::Handler as CmdTaskResultHandler>::Task: CmdTask<Pkt = CF::Pkt>,
    CF::Pkt: Send,
{
    CachedSenderFactory::new(RRSenderGroupFactory::new(
        config.backend_conn_num,
        RecoverableBackendNodeFactory::new(
            config.clone(),
            reply_handler_factory,
            conn_factory,
            future_registry,
        ),
    ))
}

pub type MigrationBackendSenderFactory<F, CF> =
    RRSenderGroupFactory<ReqAdaptorSenderFactory<RecoverableBackendNodeFactory<F, CF>>>;

pub fn gen_migration_sender_factory<F: CmdTaskResultHandlerFactory, CF: ConnFactory>(
    config: Arc<ServerProxyConfig>,
    reply_handler_factory: Arc<F>,
    conn_factory: Arc<CF>,
    future_registry: Arc<TrackedFutureRegistry>,
) -> MigrationBackendSenderFactory<F, CF>
where
    <F::Handler as CmdTaskResultHandler>::Task: CmdTask<Pkt = CF::Pkt>,
    CF::Pkt: Send,
{
    RRSenderGroupFactory::new(
        config.backend_conn_num,
        ReqAdaptorSenderFactory::new(RecoverableBackendNodeFactory::new(
            config.clone(),
            reply_handler_factory,
            conn_factory,
            future_registry,
        )),
    )
}
