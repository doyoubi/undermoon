use super::backend::{
    BackendError, CachedSenderFactory, CmdTask, CmdTaskResultHandler, CmdTaskResultHandlerFactory,
    CmdTaskSender, CmdTaskSenderFactory, ConnFactory, RRSenderGroupFactory,
    RecoverableBackendNodeFactory,
};
use super::command::{CommandError, CommandResult};
use super::database::DBTag;
use super::service::ServerProxyConfig;
use super::slowlog::TaskEvent;
use crate::common::utils::{ThreadSafe, Wrapper};
use crate::protocol::{Resp, RespVec};
use crossbeam_channel;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

pub trait TaskBlockingController: ThreadSafe {
    fn blocking_done(&self) -> bool;
    fn is_blocking(&self) -> bool;
    fn start_blocking(&self) -> BlockingHandle;
    fn stop_blocking(&self);
}

pub trait TaskBlockingControllerFactory {
    type Ctrl: TaskBlockingController;

    fn create(&self, address: String) -> Arc<Self::Ctrl>;
}

pub trait BlockingCmdTaskSender: CmdTaskSender + ThreadSafe {}

pub type BasicBlockingSenderFactory<F, CF> =
    RRSenderGroupFactory<RecoverableBackendNodeFactory<F, CF>>;
pub type BlockingBackendSenderFactory<F, CF, BS> =
    CachedSenderFactory<TaskBlockingQueueSenderFactory<BasicBlockingSenderFactory<F, CF>, BS>>;

pub fn gen_basic_blocking_sender_factory<F: CmdTaskResultHandlerFactory, CF: ConnFactory>(
    config: Arc<ServerProxyConfig>,
    reply_handler_factory: Arc<F>,
    conn_factory: Arc<CF>,
) -> BasicBlockingSenderFactory<F, CF>
where
    <F::Handler as CmdTaskResultHandler>::Task: CmdTask<Pkt = CF::Pkt>,
    CF::Pkt: Send,
{
    RRSenderGroupFactory::new(
        config.backend_conn_num,
        RecoverableBackendNodeFactory::new(config.clone(), reply_handler_factory, conn_factory),
    )
}

pub fn gen_blocking_sender_factory<
    F: CmdTaskResultHandlerFactory,
    CF: ConnFactory,
    BS: BlockingCmdTaskSender,
>(
    blocking_map: Arc<BlockingMap<BasicBlockingSenderFactory<F, CF>, BS>>,
) -> BlockingBackendSenderFactory<F, CF, BS>
where
    <F::Handler as CmdTaskResultHandler>::Task: CmdTask<Pkt = CF::Pkt>,
    F::Handler: CmdTaskResultHandler<Task = CounterTask<BS::Task>>,
    CF::Pkt: Send,
{
    CachedSenderFactory::new(TaskBlockingQueueSenderFactory::new(blocking_map))
}

pub struct BlockingMap<F, BS>
where
    F: CmdTaskSenderFactory,
    F::Sender: CmdTaskSender<Task = CounterTask<BS::Task>> + ThreadSafe,
    BS: BlockingCmdTaskSender,
{
    ctrl_map: DashMap<String, Arc<TaskBlockingQueue<F::Sender, BS>>>,
    sender_factory: F,
    blocking_task_sender: Arc<BS>,
}

impl<F, BS> BlockingMap<F, BS>
where
    F: CmdTaskSenderFactory,
    F::Sender: CmdTaskSender<Task = CounterTask<BS::Task>> + ThreadSafe,
    BS: BlockingCmdTaskSender,
{
    pub fn new(sender_factory: F, blocking_task_sender: Arc<BS>) -> Self {
        Self {
            ctrl_map: DashMap::new(),
            sender_factory,
            blocking_task_sender,
        }
    }

    pub fn get_blocking_queue(&self, address: String) -> Arc<TaskBlockingQueue<F::Sender, BS>> {
        self.get_or_create(address)
    }

    pub fn get_or_create(&self, address: String) -> Arc<TaskBlockingQueue<F::Sender, BS>> {
        match self.ctrl_map.entry(address.clone()) {
            Entry::Occupied(ctrl) => ctrl.get().clone(),
            Entry::Vacant(e) => {
                let sender = self.sender_factory.create(address);
                let ctrl = TaskBlockingQueue::new(sender, self.blocking_task_sender.clone());
                e.insert(Arc::new(ctrl)).clone()
            }
        }
    }
}

impl<F, BS> TaskBlockingControllerFactory for BlockingMap<F, BS>
where
    F: CmdTaskSenderFactory,
    F::Sender: CmdTaskSender<Task = CounterTask<BS::Task>> + ThreadSafe,
    BS: BlockingCmdTaskSender,
{
    type Ctrl = TaskBlockingQueue<F::Sender, BS>;

    fn create(&self, address: String) -> Arc<Self::Ctrl> {
        self.get_or_create(address)
    }
}

pub struct BlockingHandle {
    blocking: Arc<AtomicUsize>,
}

impl BlockingHandle {
    pub fn new(blocking: Arc<AtomicUsize>) -> Self {
        blocking.fetch_add(1, Ordering::SeqCst);
        info!("migration start blocking");
        Self { blocking }
    }

    pub fn stop(self) {}
}

impl Drop for BlockingHandle {
    fn drop(&mut self) {
        self.blocking.fetch_sub(1, Ordering::SeqCst);
        info!("migraiton stop blocking");
    }
}

pub struct TaskBlockingQueue<S, BS>
where
    S: CmdTaskSender<Task = CounterTask<BS::Task>> + ThreadSafe,
    BS: BlockingCmdTaskSender,
{
    queue_sender: crossbeam_channel::Sender<BS::Task>,
    queue_receiver: crossbeam_channel::Receiver<BS::Task>,
    blocking: Arc<AtomicUsize>,
    inner_sender: S,
    blocking_task_sender: Arc<BS>,
    running_cmd: Arc<AtomicI64>,
}

impl<S, BS> TaskBlockingQueue<S, BS>
where
    S: CmdTaskSender<Task = CounterTask<BS::Task>> + ThreadSafe,
    BS: BlockingCmdTaskSender,
{
    pub fn new(inner_sender: S, blocking_task_sender: Arc<BS>) -> Self {
        let (queue_sender, queue_receiver) = crossbeam_channel::unbounded();
        Self {
            queue_sender,
            queue_receiver,
            blocking: Arc::new(AtomicUsize::new(0)),
            inner_sender,
            blocking_task_sender,
            running_cmd: Arc::new(AtomicI64::new(0)),
        }
    }

    fn release_all(&self) {
        loop {
            let cmd_task = match self.queue_receiver.try_recv() {
                Ok(cmd_task) => cmd_task,
                Err(err) => {
                    if let crossbeam_channel::TryRecvError::Disconnected = err {
                        error!("invalid state, blocking queue is disconnected");
                    }
                    return;
                }
            };
            if let Err(err) = self.blocking_task_sender.send(cmd_task) {
                error!(
                    "failed to send task when releasing blocking queue: {:?}",
                    err
                );
            }
        }
    }

    fn send(&self, cmd_task: BS::Task) -> Result<(), BackendError> {
        if !self.is_blocking() {
            let counter_task = CounterTask::new(cmd_task, self.running_cmd.clone());
            return self.inner_sender.send(counter_task);
        }

        if let Err(err) = self.queue_sender.send(cmd_task) {
            let cmd_task = err.into_inner();
            cmd_task.set_resp_result(Ok(Resp::Error(
                b"failed to send to blocking queue".to_vec(),
            )));
            error!("failed to send to blocking queue");
            return Err(BackendError::Canceled);
        }

        if !self.is_blocking() {
            self.release_all();
        }
        Ok(())
    }
}

impl<S, BS> TaskBlockingController for TaskBlockingQueue<S, BS>
where
    S: CmdTaskSender<Task = CounterTask<BS::Task>> + ThreadSafe,
    BS: BlockingCmdTaskSender,
{
    fn blocking_done(&self) -> bool {
        self.running_cmd.load(Ordering::SeqCst) == 0
    }

    fn is_blocking(&self) -> bool {
        self.blocking.load(Ordering::SeqCst) == 0
    }

    fn start_blocking(&self) -> BlockingHandle {
        BlockingHandle::new(self.blocking.clone())
    }

    fn stop_blocking(&self) {
        self.release_all();
    }
}

pub struct TaskBlockingQueueSender<S, BS>
where
    S: CmdTaskSender<Task = CounterTask<BS::Task>> + ThreadSafe,
    BS: BlockingCmdTaskSender,
{
    queue: Arc<TaskBlockingQueue<S, BS>>,
}

impl<S, BS> CmdTaskSender for TaskBlockingQueueSender<S, BS>
where
    S: CmdTaskSender<Task = CounterTask<BS::Task>> + ThreadSafe,
    BS: BlockingCmdTaskSender,
{
    type Task = BS::Task;

    fn send(&self, cmd_task: Self::Task) -> Result<(), BackendError> {
        self.queue.send(cmd_task)
    }
}

pub struct TaskBlockingQueueSenderFactory<F, BS>
where
    F: CmdTaskSenderFactory,
    F::Sender: CmdTaskSender<Task = CounterTask<BS::Task>> + ThreadSafe,
    BS: BlockingCmdTaskSender,
{
    blocking_map: Arc<BlockingMap<F, BS>>,
}

impl<F, BS> TaskBlockingQueueSenderFactory<F, BS>
where
    F: CmdTaskSenderFactory,
    F::Sender: CmdTaskSender<Task = CounterTask<BS::Task>> + ThreadSafe,
    BS: BlockingCmdTaskSender,
{
    pub fn new(blocking_map: Arc<BlockingMap<F, BS>>) -> Self {
        Self { blocking_map }
    }
}

impl<F, BS> CmdTaskSenderFactory for TaskBlockingQueueSenderFactory<F, BS>
where
    F: CmdTaskSenderFactory,
    F::Sender: CmdTaskSender<Task = CounterTask<BS::Task>> + ThreadSafe,
    BS: BlockingCmdTaskSender,
{
    type Sender = TaskBlockingQueueSender<F::Sender, BS>;

    fn create(&self, address: String) -> Self::Sender {
        let queue = self.blocking_map.get_blocking_queue(address);
        Self::Sender { queue }
    }
}

pub struct CounterTask<T: CmdTask> {
    inner: T,
    _counter: AutoCounter,
}

struct AutoCounter(Arc<AtomicI64>);

impl AutoCounter {
    fn new(counter: Arc<AtomicI64>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self(counter)
    }
}

impl Drop for AutoCounter {
    fn drop(&mut self) {
        // TODO: This order could be relaxed.
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<T: CmdTask> CounterTask<T> {
    pub fn new(inner: T, counter: Arc<AtomicI64>) -> Self {
        Self {
            inner,
            _counter: AutoCounter::new(counter),
        }
    }

    pub fn into_inner(self) -> T {
        let Self { inner, .. } = self;
        inner
    }
}

impl<T: CmdTask> From<CounterTask<T>> for Wrapper<T> {
    fn from(counter_task: CounterTask<T>) -> Self {
        Wrapper(counter_task.into_inner())
    }
}

impl<T: CmdTask + DBTag> DBTag for CounterTask<T> {
    fn get_db_name(&self) -> String {
        self.inner.get_db_name()
    }

    fn set_db_name(&self, db: String) {
        self.inner.set_db_name(db)
    }
}

impl<T: CmdTask> CmdTask for CounterTask<T> {
    type Pkt = T::Pkt;

    fn get_key(&self) -> Option<&[u8]> {
        self.inner.get_key()
    }

    fn set_result(self, result: CommandResult<Self::Pkt>) {
        self.into_inner().set_result(result)
    }

    fn get_packet(&self) -> Self::Pkt {
        self.inner.get_packet()
    }

    fn set_resp_result(self, result: Result<RespVec, CommandError>)
    where
        Self: Sized,
    {
        self.inner.set_resp_result(result)
    }

    fn log_event(&self, event: TaskEvent) {
        self.inner.log_event(event)
    }
}
