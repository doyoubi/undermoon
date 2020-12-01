use super::backend::{
    CmdTask, CmdTaskResultHandler, CmdTaskResultHandlerFactory, ConnFactory, IntoTask,
    SenderBackendError,
};
use super::cluster::ClusterTag;
use super::command::{CommandError, CommandResult};
use super::sender::{
    CachedSenderFactory, CmdTaskSender, CmdTaskSenderFactory, RRSenderGroupFactory,
    RecoverableBackendNodeFactory,
};
use super::service::ServerProxyConfig;
use super::slowlog::TaskEvent;
use crate::common::batch::BatchStats;
use crate::common::biatomic::BiAtomicU32;
use crate::common::cluster::ClusterName;
use crate::common::track::TrackedFutureRegistry;
use crate::common::utils::{ThreadSafe, Wrapper};
use crate::protocol::{Resp, RespVec};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Weak};

pub type BlockingTerm = u32;

pub struct BlockingState {
    pub blocking: bool,
    pub term: BlockingTerm,
}

#[derive(Debug, Clone, Copy)]
pub enum BlockingHint {
    NotBlocking,
    NotBlockingInMigration(BlockingTerm),
    Blocking,
}

pub trait TaskBlockingController: ThreadSafe {
    type Sender: BlockingCmdTaskSender;

    fn blocking_done(&self) -> bool;
    // Returns the current blocking state and term
    fn get_blocking_state(&self) -> BlockingState;
    fn start_blocking(&self) -> BlockingHandle<Self::Sender>;
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
    future_registry: Arc<TrackedFutureRegistry>,
    batch_stats: Arc<BatchStats>,
) -> BasicBlockingSenderFactory<F, CF>
where
    <F::Handler as CmdTaskResultHandler>::Task: CmdTask<Pkt = CF::Pkt>,
    CF::Pkt: Send,
{
    RRSenderGroupFactory::new(
        config.backend_conn_num,
        RecoverableBackendNodeFactory::new(
            config.clone(),
            reply_handler_factory,
            conn_factory,
            future_registry,
            batch_stats,
        ),
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
    ctrl_map: DashMap<String, Weak<TaskBlockingQueue<F::Sender, BS>>>,
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
            Entry::Occupied(mut entry) => {
                if let Some(ctrl) = entry.get().upgrade() {
                    return ctrl;
                }
                let (ctrl, ctrl_weak) = self.create_ctrl(address);
                entry.insert(ctrl_weak);
                ctrl
            }
            Entry::Vacant(entry) => {
                let (ctrl, ctrl_weak) = self.create_ctrl(address);
                entry.insert(ctrl_weak);
                ctrl
            }
        }
    }

    #[allow(clippy::type_complexity)]
    fn create_ctrl(
        &self,
        address: String,
    ) -> (
        Arc<TaskBlockingQueue<F::Sender, BS>>,
        Weak<TaskBlockingQueue<F::Sender, BS>>,
    ) {
        let sender = self.sender_factory.create(address);
        let ctrl = TaskBlockingQueue::new(sender, self.blocking_task_sender.clone());
        let ctrl = Arc::new(ctrl);
        let ctrl_weak = Arc::downgrade(&ctrl);
        (ctrl, ctrl_weak)
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

pub struct BlockingHandle<BS: BlockingCmdTaskSender> {
    inner: Arc<BlockingHandleInner<BS>>,
}

impl<BS: BlockingCmdTaskSender> BlockingHandle<BS> {
    fn new(inner: Arc<BlockingHandleInner<BS>>) -> Self {
        inner
            .blocking_state
            .compare_and_apply(|blocking_count| blocking_count + 1, |term| term + 1);
        info!("migration start blocking");
        Self { inner }
    }

    pub fn stop(self) {}
}

impl<BS: BlockingCmdTaskSender> Drop for BlockingHandle<BS> {
    fn drop(&mut self) {
        info!("blocking handle is dropped");
        let (prev_blocking_count, _prev_term) = self
            .inner
            .blocking_state
            .compare_and_apply(|blocking_count| blocking_count - 1, |term| term + 1);
        if prev_blocking_count == 1 {
            info!("migraition stop blocking");
            self.inner.release_all();
        }
    }
}

struct BlockingHandleInner<BS: BlockingCmdTaskSender> {
    blocking_state: BiAtomicU32,
    queue_receiver: crossbeam_channel::Receiver<BS::Task>,
    blocking_task_sender: Arc<BS>,
}

impl<BS: BlockingCmdTaskSender> BlockingHandleInner<BS> {
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
}

pub struct TaskBlockingQueue<S, BS>
where
    S: CmdTaskSender<Task = CounterTask<BS::Task>> + ThreadSafe,
    BS: BlockingCmdTaskSender,
{
    queue_sender: crossbeam_channel::Sender<BS::Task>,
    blocking_handle_inner: Arc<BlockingHandleInner<BS>>,
    inner_sender: S,
    running_cmd: Arc<AtomicI64>,
}

impl<S, BS> TaskBlockingQueue<S, BS>
where
    S: CmdTaskSender<Task = CounterTask<BS::Task>> + ThreadSafe,
    BS: BlockingCmdTaskSender,
{
    pub fn new(inner_sender: S, blocking_task_sender: Arc<BS>) -> Self {
        let (queue_sender, queue_receiver) = crossbeam_channel::unbounded();
        let blocking_handle_inner = Arc::new(BlockingHandleInner {
            blocking_state: BiAtomicU32::new(0, 0),
            queue_receiver,
            blocking_task_sender,
        });
        Self {
            queue_sender,
            blocking_handle_inner,
            inner_sender,
            running_cmd: Arc::new(AtomicI64::new(0)),
        }
    }

    fn send(
        &self,
        cmd_task: BlockingHintTask<BS::Task>,
    ) -> Result<(), SenderBackendError<BlockingHintTask<BS::Task>>> {
        // `cmd_blocking_hint` is still needed even we have `self.get_blocking_state().blocking` check.
        // Without it, the following case could happen:
        // (1) A command with the key should be migrated is executed.
        // (2) The migration manager check that it should be sent to the blocking queue.
        // (3) It goes all the way into this function.
        // (4) The blocking is done, this function will forward all the commands to the backend node.
        // (5) This command is sent to the backend, which is not correct.
        // Thus we need a blocking hint for consistency.
        let cmd_blocking_hint = cmd_task.get_blocking_hint();
        let cmd_task = cmd_task.into_inner();

        // The `waiting for blocking` operation could happen between `get_blocking_state()` and `running_cmd.fetch_add()`,
        // which could result in leaking some commands even after blocking has already started.
        // We need something like RwLock to protect this critical section.
        // Since CmdTaskSender::send has to be `&self`, we have to implement something similar ourselves.
        // Add `running_cmd` anyway to hold this "lock".
        // TODO: this counter increment (reader lock) might starve the waiting side (writer lock).
        let counter = RefAutoCounter::new(&(*self.running_cmd));
        let BlockingState { blocking, term } = self.get_blocking_state();
        if !blocking {
            let blocking = match cmd_blocking_hint {
                BlockingHint::NotBlocking => false,
                BlockingHint::NotBlockingInMigration(cmd_term) if term <= cmd_term => {
                    if term < cmd_term {
                        error!(
                            "invalid state: current term {} < cmd term {}",
                            term, cmd_term
                        );
                    }
                    false
                }
                BlockingHint::NotBlockingInMigration(_) => true,
                BlockingHint::Blocking => true,
            };
            if !blocking {
                let counter_task = CounterTask::new(cmd_task, self.running_cmd.clone());
                return self.inner_sender.send(counter_task).map_err(|err| {
                    err.map_task(|task| BlockingHintTask::new(task.into_inner(), cmd_blocking_hint))
                });
            }

            info!("retry for blocking command");
            return Err(SenderBackendError::Retry(BlockingHintTask::new(
                cmd_task,
                cmd_blocking_hint,
            )));
        }
        drop(counter);

        if let Err(err) = self.queue_sender.send(cmd_task) {
            let cmd_task = err.into_inner();
            cmd_task.set_resp_result(Ok(Resp::Error(
                b"failed to send to blocking queue".to_vec(),
            )));
            error!("failed to send to blocking queue");
            return Err(SenderBackendError::Canceled);
        }

        let BlockingState { blocking, .. } = self.get_blocking_state();
        if !blocking {
            self.blocking_handle_inner.release_all();
        }
        Ok(())
    }
}

impl<S, BS> TaskBlockingController for TaskBlockingQueue<S, BS>
where
    S: CmdTaskSender<Task = CounterTask<BS::Task>> + ThreadSafe,
    BS: BlockingCmdTaskSender,
{
    type Sender = BS;

    fn blocking_done(&self) -> bool {
        self.running_cmd.load(Ordering::SeqCst) == 0
    }

    fn get_blocking_state(&self) -> BlockingState {
        let (blocking_count, term) = self.blocking_handle_inner.blocking_state.load();
        BlockingState {
            blocking: blocking_count > 0,
            term,
        }
    }

    fn start_blocking(&self) -> BlockingHandle<Self::Sender> {
        BlockingHandle::new(self.blocking_handle_inner.clone())
    }

    fn stop_blocking(&self) {
        self.blocking_handle_inner.release_all();
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
    type Task = BlockingHintTask<BS::Task>;

    fn send(&self, cmd_task: Self::Task) -> Result<(), SenderBackendError<Self::Task>> {
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

struct RefAutoCounter<'a>(&'a AtomicI64);

impl<'a> RefAutoCounter<'a> {
    fn new(counter: &'a AtomicI64) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self(counter)
    }
}

impl<'a> Drop for RefAutoCounter<'a> {
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

impl<T: CmdTask + ClusterTag> ClusterTag for CounterTask<T> {
    fn get_cluster_name(&self) -> &ClusterName {
        self.inner.get_cluster_name()
    }

    fn set_cluster_name(&mut self, cluster_name: ClusterName) {
        self.inner.set_cluster_name(cluster_name)
    }
}

impl<T: CmdTask> CmdTask for CounterTask<T> {
    type Pkt = T::Pkt;
    type TaskType = T::TaskType;
    type Context = T::Context;

    fn get_key(&self) -> Option<&[u8]> {
        self.inner.get_key()
    }

    fn get_slot(&self) -> Option<usize> {
        self.inner.get_slot()
    }

    fn set_result(self, result: CommandResult<Self::Pkt>) {
        self.into_inner().set_result(result)
    }

    fn get_packet(&self) -> Self::Pkt {
        self.inner.get_packet()
    }

    fn get_type(&self) -> Self::TaskType {
        self.inner.get_type()
    }

    fn get_context(&self) -> Self::Context {
        self.inner.get_context()
    }

    fn set_resp_result(self, result: Result<RespVec, CommandError>)
    where
        Self: Sized,
    {
        self.inner.set_resp_result(result)
    }

    fn log_event(&mut self, event: TaskEvent) {
        self.inner.log_event(event)
    }
}

pub struct BlockingHintTask<T: CmdTask> {
    inner: T,
    blocking_hint: BlockingHint,
}

impl<T: CmdTask> BlockingHintTask<T> {
    pub fn new(inner: T, blocking_hint: BlockingHint) -> Self {
        Self {
            inner,
            blocking_hint,
        }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    pub fn get_blocking_hint(&self) -> BlockingHint {
        self.blocking_hint
    }
}

impl<T: CmdTask> CmdTask for BlockingHintTask<T> {
    type Pkt = T::Pkt;
    type TaskType = T::TaskType;
    type Context = T::Context;

    fn get_key(&self) -> Option<&[u8]> {
        self.inner.get_key()
    }

    fn get_slot(&self) -> Option<usize> {
        self.inner.get_slot()
    }

    fn set_result(self, result: CommandResult<Self::Pkt>) {
        self.into_inner().set_result(result)
    }

    fn get_packet(&self) -> Self::Pkt {
        self.inner.get_packet()
    }

    fn get_type(&self) -> Self::TaskType {
        self.inner.get_type()
    }

    fn get_context(&self) -> Self::Context {
        self.inner.get_context()
    }

    fn set_resp_result(self, result: Result<RespVec, CommandError>)
    where
        Self: Sized,
    {
        self.inner.set_resp_result(result)
    }

    fn log_event(&mut self, event: TaskEvent) {
        self.inner.log_event(event)
    }
}

impl<T: CmdTask + ClusterTag> ClusterTag for BlockingHintTask<T> {
    fn get_cluster_name(&self) -> &ClusterName {
        self.inner.get_cluster_name()
    }

    fn set_cluster_name(&mut self, cluster_name: ClusterName) {
        self.inner.set_cluster_name(cluster_name)
    }
}

impl<T: CmdTask + ClusterTag> IntoTask<T> for BlockingHintTask<T> {
    fn into_task(self) -> T {
        self.into_inner()
    }
}
