use super::backend::{BackendError, CmdTask, ReqTask, ReqTaskSender, ReqTaskSenderFactory};
use super::command::{CommandError, CommandResult};
use super::database::DBTag;
use super::slowlog::TaskEvent;
use crate::common::utils::Wrapper;
use crate::protocol::{Resp, RespVec};
use crossbeam_channel;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

pub trait TaskBlockingController {
    type Task: CmdTask;

    fn blocking_done(&self) -> bool;
    fn is_blocking(&self) -> bool;
    fn start_blocking(&self);
    fn stop_blocking(&self);
    fn send(&self, cmd_task: ReqTask<Self::Task>) -> Result<(), BackendError>;
}

pub struct BlockingMap<F: ReqTaskSenderFactory> {
    ctrl_map: DashMap<String, Arc<QueueBlockingController<F::Sender>>>,
    sender_factory: F,
}

impl<F: ReqTaskSenderFactory> BlockingMap<F> {
    pub fn new(sender_factory: F) -> Self {
        Self {
            ctrl_map: DashMap::new(),
            sender_factory,
        }
    }

    pub fn get_blocking_queue(&self, address: String) -> Arc<TaskBlockingQueue<F::Sender>> {
        self.get_or_create(address).queue.clone()
    }

    pub fn get_or_create(&self, address: String) -> Arc<QueueBlockingController<F::Sender>> {
        match self.ctrl_map.entry(address.clone()) {
            Entry::Occupied(ctrl) => ctrl.get().clone(),
            Entry::Vacant(e) => {
                let sender = self.sender_factory.create(address);
                let ctrl = QueueBlockingController::new(sender);
                e.insert(Arc::new(ctrl)).clone()
            }
        }
    }
}

pub struct QueueBlockingController<S: ReqTaskSender> {
    queue: Arc<TaskBlockingQueue<S>>,
    running_cmd: Arc<AtomicI64>,
}

impl<S: ReqTaskSender> QueueBlockingController<S> {
    pub fn new(sender: S) -> Self {
        let running_cmd = Arc::new(AtomicI64::new(0));
        let queue = Arc::new(TaskBlockingQueue::new(sender));
        Self { queue, running_cmd }
    }
}

impl<S: ReqTaskSender> QueueBlockingController<S> {
    pub fn get_blocking_queue(&self) -> Arc<TaskBlockingQueue<S>> {
        self.queue.clone()
    }
}

impl<S: ReqTaskSender> TaskBlockingController for QueueBlockingController<S> {
    type Task = S::Task;

    fn blocking_done(&self) -> bool {
        self.running_cmd.load(Ordering::SeqCst) == 0
    }

    fn is_blocking(&self) -> bool {
        self.queue.is_blocking()
    }

    fn start_blocking(&self) {
        self.queue.start_blocking();
    }

    fn stop_blocking(&self) {
        self.queue.release_all();
    }

    fn send(&self, cmd_task: ReqTask<Self::Task>) -> Result<(), BackendError> {
        self.queue.send(cmd_task)
    }
}

pub struct TaskBlockingQueue<S: ReqTaskSender> {
    queue_sender: crossbeam_channel::Sender<ReqTask<S::Task>>,
    queue_receiver: crossbeam_channel::Receiver<ReqTask<S::Task>>,
    blocking: AtomicBool,
    inner_sender: S,
}

impl<S: ReqTaskSender> TaskBlockingQueue<S> {
    pub fn new(inner_sender: S) -> Self {
        let (queue_sender, queue_receiver) = crossbeam_channel::unbounded();
        Self {
            queue_sender,
            queue_receiver,
            blocking: AtomicBool::new(false),
            inner_sender,
        }
    }

    pub fn is_blocking(&self) -> bool {
        self.blocking.load(Ordering::SeqCst)
    }

    pub fn start_blocking(&self) {
        self.blocking.store(true, Ordering::SeqCst);
    }

    pub fn send(&self, cmd_task: ReqTask<S::Task>) -> Result<(), BackendError> {
        if !self.is_blocking() {
            return self.inner_sender.send(cmd_task);
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

    pub fn release_all(&self) {
        self.blocking.store(false, Ordering::SeqCst);
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
            if let Err(err) = self.inner_sender.send(cmd_task) {
                error!(
                    "failed to send task when releasing blocking queue: {:?}",
                    err
                );
            }
        }
    }
}

pub struct TaskBlockingQueueSender<S: ReqTaskSender> {
    queue: Arc<TaskBlockingQueue<S>>,
}

impl<S: ReqTaskSender> ReqTaskSender for TaskBlockingQueueSender<S> {
    type Task = S::Task;

    fn send(&self, cmd_task: ReqTask<Self::Task>) -> Result<(), BackendError> {
        self.queue.send(cmd_task)
    }
}

pub struct TaskBlockingQueueSenderFactory<F: ReqTaskSenderFactory> {
    blocking_map: Arc<BlockingMap<F>>,
}

impl<F: ReqTaskSenderFactory> TaskBlockingQueueSenderFactory<F> {
    pub fn new(blocking_map: Arc<BlockingMap<F>>) -> Self {
        Self { blocking_map }
    }
}

impl<F: ReqTaskSenderFactory> ReqTaskSenderFactory for TaskBlockingQueueSenderFactory<F> {
    type Sender = TaskBlockingQueueSender<F::Sender>;

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
