use super::backend::{BackendError, CmdTask};
use super::command::CommandResult;
use super::database::DBTag;
use super::manager::{send_cmd_ctx, SharedMetaMap};
use super::session::CmdCtx;
use super::slowlog::TaskEvent;
use crate::common::utils::{ThreadSafe, Wrapper};
use crate::protocol::Resp;
use crossbeam_channel;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

pub trait TaskBlockingController<T: CmdTask> {
    fn blocking_done(&self) -> bool;
    fn is_blocking(&self) -> bool;
    fn start_blocking(&self);
    fn stop_blocking(&self);
    fn send(&self, cmd_task: T) -> Result<(), BackendError>;
}

pub struct QueueBlockingController {
    queue: Arc<TaskBlockingQueue>,
}

pub struct TaskBlockingQueue {
    meta_map: SharedMetaMap,
    sender: crossbeam_channel::Sender<CmdCtx>,
    receiver: crossbeam_channel::Receiver<CmdCtx>,
    blocking: AtomicBool,
    running_cmd: Arc<AtomicI64>,
}

impl TaskBlockingQueue {
    pub fn new(meta_map: SharedMetaMap, running_cmd: Arc<AtomicI64>) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded();
        Self {
            meta_map,
            sender,
            receiver,
            blocking: AtomicBool::new(false),
            running_cmd,
        }
    }

    pub fn blocking_done(&self) -> bool {
        self.running_cmd.load(Ordering::SeqCst) == 0
    }

    pub fn is_blocking(&self) -> bool {
        self.blocking.load(Ordering::SeqCst)
    }

    pub fn start_blocking(&self) {
        self.blocking.store(true, Ordering::SeqCst);
    }

    pub fn send(&self, cmd_ctx: CmdCtx) {
        if !self.is_blocking() {
            send_cmd_ctx(&self.meta_map, cmd_ctx, &self.running_cmd);
            return;
        }

        if let Err(err) = self.sender.send(cmd_ctx) {
            let cmd_ctx = err.into_inner();
            cmd_ctx.set_resp_result(Ok(Resp::Error(
                b"failed to send to blocking queue".to_vec(),
            )));
            return;
        }

        if !self.is_blocking() {
            self.release_all();
        }
    }

    pub fn release_all(&self) {
        self.blocking.store(false, Ordering::SeqCst);
        loop {
            let cmd_ctx = match self.receiver.try_recv() {
                Ok(cmd_ctx) => cmd_ctx,
                Err(err) => {
                    if let crossbeam_channel::TryRecvError::Disconnected = err {
                        error!("invalid state, blocking queue is disconnected");
                    }
                    return;
                }
            };
            send_cmd_ctx(&self.meta_map, cmd_ctx, &self.running_cmd);
        }
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

impl<T: CmdTask + ThreadSafe> ThreadSafe for CounterTask<T> {}

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

    fn log_event(&self, event: TaskEvent) {
        self.inner.log_event(event)
    }
}
