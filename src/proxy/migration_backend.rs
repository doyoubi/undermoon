use super::backend::{BackendError, CmdTask, CmdTaskFactory, ReqTask, SenderBackendError};
use super::command::{requires_blocking_migration, CmdTypeTuple, CommandError, CommandResult};
use super::sender::CmdTaskSender;
use super::slowlog::TaskEvent;
use crate::common::response;
use crate::common::utils::{generate_lock_slot, pretty_print_bytes, RetryError, Wrapper};
use crate::migration::scan_migration::{pttl_to_restore_expire_time, PTTL_KEY_NOT_FOUND};
use crate::migration::stats::MigrationStats;
use crate::protocol::{Array, BinSafeStr, BulkStr, Functor, Resp, RespVec};
use arc_swap::ArcSwapOption;
use dashmap::DashSet;
use either::Either;
use futures::channel::{
    mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    oneshot,
};
use futures::{select, Future, FutureExt, StreamExt};
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const KEY_NOT_EXISTS: &str = "0";
const FAILED_TO_ACCESS_SOURCE: &str = "MIGRATION_FORWARD: failed to access source node";

struct WaitRegistry {
    pending: AtomicU64,
    closed: AtomicBool,
    signal: ArcSwapOption<oneshot::Sender<()>>,
}

impl WaitRegistry {
    fn new() -> (Self, WaitHandle) {
        let (s, r) = oneshot::channel();
        let registry = Self {
            pending: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            signal: ArcSwapOption::new(Some(Arc::new(s))),
        };
        (registry, r)
    }
}

impl WaitRegistry {
    // Should not be closed yet.
    fn register(&self) {
        self.pending.fetch_add(1, Ordering::SeqCst);
    }

    // Can run concurrently with `close`
    // Case 1: `closed.load` runs before `closed.store`
    //   `signal` runs in `close()`
    // Case 2: `closed.load` runs after `closed.store`
    //   `signal` runs in `unregister()`
    fn unregister(&self) {
        if self.pending.fetch_sub(1, Ordering::SeqCst) == 1 && self.closed.load(Ordering::SeqCst) {
            self.signal.swap(None);
        }
    }

    // Should be called after all `register` calls.
    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        if self.pending.load(Ordering::SeqCst) == 0 {
            self.signal.swap(None);
        }
    }
}

type WaitHandle = oneshot::Receiver<()>;

struct AutoDropWaitRegistry {
    inner: Arc<WaitRegistry>,
}

impl AutoDropWaitRegistry {
    fn new(inner: Arc<WaitRegistry>) -> Self {
        Self { inner }
    }
}

impl Drop for AutoDropWaitRegistry {
    fn drop(&mut self) {
        self.inner.unregister();
    }
}

pub struct WaitableTask<T: CmdTask> {
    inner: T,
    // We reply on the `drop` function of `AutoDropWaitRegistry`
    #[allow(dead_code)]
    _registry: Option<AutoDropWaitRegistry>,
}

impl<T: CmdTask> WaitableTask<T> {
    fn new_with_registry(inner: T, registry: Arc<WaitRegistry>) -> Self {
        registry.register();
        Self {
            inner,
            _registry: Some(AutoDropWaitRegistry::new(registry)),
        }
    }

    fn new_without_registry(inner: T) -> Self {
        Self {
            inner,
            _registry: None,
        }
    }

    #[cfg(test)]
    fn into_inner(self) -> T {
        let Self { inner, .. } = self;
        inner
    }
}

impl<T: CmdTask> From<WaitableTask<T>> for Wrapper<T> {
    fn from(waitable_task: WaitableTask<T>) -> Self {
        let WaitableTask { inner, .. } = waitable_task;
        Wrapper(inner)
    }
}

impl<T: CmdTask> CmdTask for WaitableTask<T> {
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
        self.inner.set_result(result)
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

type ReplyFuture = Pin<Box<dyn Future<Output = Result<RespVec, CommandError>> + Send>>;
type DataEntryFuture =
    Pin<Box<dyn Future<Output = Result<Option<DataEntry>, CommandError>> + Send>>;

struct MgrCmdStateExists<F: CmdTaskFactory> {
    inner_task: F::Task,
    key: BinSafeStr,
    lock_slot: usize,
}

impl<F: CmdTaskFactory> MgrCmdStateExists<F> {
    fn from_task(
        inner_task: F::Task,
        key: BinSafeStr,
        lock_slot: usize,
        cmd_task_factory: &F,
    ) -> (Self, ReqTask<WaitableTask<F::Task>>, ReplyFuture) {
        let resp = Self::gen_exists_resp(&key);
        let (cmd_task, reply_fut) =
            cmd_task_factory.create_with_ctx(inner_task.get_context(), resp);
        let task = ReqTask::Simple(WaitableTask::new_without_registry(cmd_task));
        let state = Self {
            inner_task,
            key,
            lock_slot,
        };
        (state, task, reply_fut)
    }

    fn gen_exists_resp(key: &[u8]) -> RespVec {
        let elements = vec![
            Resp::Bulk(BulkStr::Str("EXISTS".to_string().into_bytes())),
            Resp::Bulk(BulkStr::Str(key.into())),
        ];
        Resp::Arr(Array::Arr(elements))
    }

    fn into_inner(self) -> F::Task {
        let Self { inner_task, .. } = self;
        inner_task
    }
}

struct MgrCmdStateForward {
    // Only hold the lock guard
    _lock_guard: Option<KeyLockGuard>,
}

impl MgrCmdStateForward {
    fn from_state_exists<F: CmdTaskFactory>(
        state: MgrCmdStateExists<F>,
        registry: Arc<WaitRegistry>,
    ) -> (Self, ReqTask<WaitableTask<F::Task>>) {
        let inner_task = WaitableTask::new_with_registry(state.into_inner(), registry);
        (
            MgrCmdStateForward { _lock_guard: None },
            ReqTask::Simple(inner_task),
        )
    }

    fn from_state_dump_pttl<F: CmdTaskFactory>(
        state: MgrCmdStateDumpPttl<F>,
        registry: Arc<WaitRegistry>,
    ) -> (Self, ReqTask<WaitableTask<F::Task>>) {
        let inner_task = WaitableTask::new_with_registry(state.into_inner(), registry);
        (
            MgrCmdStateForward { _lock_guard: None },
            ReqTask::Simple(inner_task),
        )
    }

    fn from_state_umsync<F: CmdTaskFactory>(
        state: MgrCmdStateUmSync<F>,
        registry: Arc<WaitRegistry>,
    ) -> (Self, ReqTask<WaitableTask<F::Task>>) {
        let MgrCmdStateUmSync {
            inner_task,
            lock_guard,
        } = state;
        let inner_task = WaitableTask::new_with_registry(inner_task, registry);
        (
            MgrCmdStateForward {
                _lock_guard: Some(lock_guard),
            },
            ReqTask::Simple(inner_task),
        )
    }
}

struct MgrCmdStateDumpPttl<F: CmdTaskFactory> {
    inner_task: F::Task,
    key: BinSafeStr,
    lock_guard: KeyLockGuard,
}

impl<F: CmdTaskFactory> MgrCmdStateDumpPttl<F> {
    fn from_state_exists(
        state: MgrCmdStateExists<F>,
        cmd_task_factory: &F,
        lock_guard: KeyLockGuard,
    ) -> (Self, ReqTask<F::Task>, DataEntryFuture) {
        let MgrCmdStateExists {
            inner_task, key, ..
        } = state;

        let (dump_cmd_task, dump_reply_fut) =
            cmd_task_factory.create_with_ctx(inner_task.get_context(), Self::gen_dump_resp(&key));
        let (pttl_cmd_task, pttl_reply_fut) =
            cmd_task_factory.create_with_ctx(inner_task.get_context(), Self::gen_pttl_resp(&key));

        let task = ReqTask::Multi(vec![dump_cmd_task, pttl_cmd_task]);
        let state = Self {
            inner_task,
            key,
            lock_guard,
        };
        let entry_fut = Box::pin(get_data_entry(dump_reply_fut, pttl_reply_fut));

        (state, task, entry_fut)
    }

    fn gen_dump_resp(key: &[u8]) -> RespVec {
        let elements = vec![
            Resp::Bulk(BulkStr::Str("DUMP".to_string().into_bytes())),
            Resp::Bulk(BulkStr::Str(key.into())),
        ];
        Resp::Arr(Array::Arr(elements))
    }

    fn gen_pttl_resp(key: &[u8]) -> RespVec {
        let elements = vec![
            Resp::Bulk(BulkStr::Str("PTTL".to_string().into_bytes())),
            Resp::Bulk(BulkStr::Str(key.into())),
        ];
        Resp::Arr(Array::Arr(elements))
    }

    fn into_inner(self) -> F::Task {
        let Self { inner_task, .. } = self;
        inner_task
    }
}

struct DataEntry {
    raw_data: BinSafeStr,
    pttl: BinSafeStr,
}

async fn get_data_entry(
    dump: ReplyFuture,
    pttl: ReplyFuture,
) -> Result<Option<DataEntry>, CommandError> {
    let dump_result = match dump.await? {
        Resp::Bulk(BulkStr::Str(raw_data)) => Ok(Some(raw_data)),
        Resp::Bulk(BulkStr::Nil) => Ok(None),
        _others => Err(CommandError::UnexpectedResponse),
    };

    let pttl_result = match pttl.await? {
        // -2 for key not exists
        Resp::Integer(pttl) if pttl.as_slice() != PTTL_KEY_NOT_FOUND => Ok(Some(pttl)),
        Resp::Integer(_pttl) => Ok(None),
        _others => Err(CommandError::UnexpectedResponse),
    };

    match (dump_result, pttl_result) {
        (Ok(Some(raw_data)), Ok(Some(pttl))) => Ok(Some(DataEntry { raw_data, pttl })),
        (Ok(None), _) | (_, Ok(None)) => Ok(None),
        (Err(err), _) => Err(err),
        (_, Err(err)) => Err(err),
    }
}

struct MgrCmdStateUmSync<F: CmdTaskFactory> {
    inner_task: F::Task,
    lock_guard: KeyLockGuard,
}

impl<F: CmdTaskFactory> MgrCmdStateUmSync<F> {
    fn from_task(
        inner_task: F::Task,
        key: BinSafeStr,
        lock_guard: KeyLockGuard,
        cmd_task_factory: &F,
    ) -> (Self, ReqTask<F::Task>, ReplyFuture) {
        let (umsync_cmd_task, umsync_reply_fut) =
            cmd_task_factory.create_with_ctx(inner_task.get_context(), Self::gen_umsync_resp(&key));

        let task = ReqTask::Simple(umsync_cmd_task);
        let state = Self {
            inner_task,
            lock_guard,
        };
        let sync_fut = Box::pin(umsync_reply_fut);

        (state, task, sync_fut)
    }

    fn gen_umsync_resp(key: &[u8]) -> RespVec {
        let elements = vec![
            Resp::Bulk(BulkStr::Str("UMSYNC".to_string().into_bytes())),
            Resp::Bulk(BulkStr::Str(key.into())),
        ];
        Resp::Arr(Array::Arr(elements))
    }

    fn into_inner(self) -> F::Task {
        let Self { inner_task, .. } = self;
        inner_task
    }
}

struct MgrCmdStateRestoreForward<F: CmdTaskFactory> {
    lock_guard: KeyLockGuard,
    key: BinSafeStr,
    task_context: <F::Task as CmdTask>::Context,
}

impl<F: CmdTaskFactory> MgrCmdStateRestoreForward<F> {
    fn from_state_dump(
        state: MgrCmdStateDumpPttl<F>,
        entry: DataEntry,
        cmd_task_factory: &F,
        registry: Arc<WaitRegistry>,
    ) -> (Self, ReqTask<WaitableTask<F::Task>>, ReplyFuture) {
        let MgrCmdStateDumpPttl {
            inner_task,
            key,
            lock_guard,
        } = state;
        let task_context = inner_task.get_context();
        let DataEntry { raw_data, pttl } = entry;
        let resp = Self::gen_restore_resp(&key, raw_data, pttl);
        let (restore_cmd_task, restore_reply_fut) =
            cmd_task_factory.create_with_ctx(inner_task.get_context(), resp);

        let restore_cmd_task = WaitableTask::new_with_registry(restore_cmd_task, registry.clone());
        let inner_task = WaitableTask::new_with_registry(inner_task, registry);

        let task = ReqTask::Multi(vec![restore_cmd_task, inner_task]);
        (
            MgrCmdStateRestoreForward {
                lock_guard,
                key,
                task_context,
            },
            task,
            restore_reply_fut,
        )
    }

    fn gen_restore_resp(key: &[u8], raw_data: BinSafeStr, pttl: BinSafeStr) -> RespVec {
        let expire_time = pttl_to_restore_expire_time(pttl);

        let elements = vec![
            Resp::Bulk(BulkStr::Str("RESTORE".to_string().into_bytes())),
            Resp::Bulk(BulkStr::Str(key.into())),
            Resp::Bulk(BulkStr::Str(expire_time)),
            Resp::Bulk(BulkStr::Str(raw_data)),
        ];
        Resp::Arr(Array::Arr(elements))
    }
}

#[derive(Debug)]
struct MgrCmdStateDel;

impl MgrCmdStateDel {
    fn from_task_context<F: CmdTaskFactory>(
        inner_task_context: <F::Task as CmdTask>::Context,
        key: BinSafeStr,
        cmd_task_factory: &F,
    ) -> (Self, ReqTask<F::Task>, ReplyFuture) {
        let resp = Self::gen_del_resp(&key);
        let (cmd_task, reply_fut) = cmd_task_factory.create_with_ctx(inner_task_context, resp);
        let task = ReqTask::Simple(cmd_task);
        (Self, task, reply_fut)
    }

    fn gen_del_resp(key: &[u8]) -> RespVec {
        let elements = vec![
            Resp::Bulk(BulkStr::Str("DEL".to_string().into_bytes())),
            Resp::Bulk(BulkStr::Str(key.into())),
        ];
        Resp::Arr(Array::Arr(elements))
    }
}

struct PendingUmSyncTask<T> {
    cmd_task: T,
    key: BinSafeStr,
    lock_slot: usize,
}

type ExistsTaskSender<F> = UnboundedSender<(MgrCmdStateExists<F>, ReplyFuture)>;
type ExistsTaskReceiver<F> = UnboundedReceiver<(MgrCmdStateExists<F>, ReplyFuture)>;
type DumpPttlTaskSender<F> = UnboundedSender<(MgrCmdStateDumpPttl<F>, DataEntryFuture)>;
type DumpPttlTaskReceiver<F> = UnboundedReceiver<(MgrCmdStateDumpPttl<F>, DataEntryFuture)>;
type RestoreTaskSender<F> = UnboundedSender<(MgrCmdStateRestoreForward<F>, ReplyFuture)>;
type RestoreTaskReceiver<F> = UnboundedReceiver<(MgrCmdStateRestoreForward<F>, ReplyFuture)>;
type PendingUmSyncTaskSender<T> = UnboundedSender<PendingUmSyncTask<T>>;
type PendingUmSyncTaskReceiver<T> = UnboundedReceiver<PendingUmSyncTask<T>>;
type UmSyncTaskSender<F> = UnboundedSender<(MgrCmdStateUmSync<F>, ReplyFuture)>;
type UmSyncTaskReceiver<F> = UnboundedReceiver<(MgrCmdStateUmSync<F>, ReplyFuture)>;
type DeleteKeyTaskSender = UnboundedSender<ReplyFuture>;
type DeleteKeyTaskReceiver = UnboundedReceiver<ReplyFuture>;

pub struct RestoreDataCmdTaskHandler<F, S, DS, PS>
where
    F: CmdTaskFactory,
    F::Task: CmdTask<TaskType = CmdTypeTuple>,
    S: CmdTaskSender<Task = ReqTask<F::Task>>,
    DS: CmdTaskSender<Task = ReqTask<WaitableTask<F::Task>>>,
    PS: CmdTaskSender<Task = ReqTask<F::Task>>,
{
    src_sender: Arc<S>,
    dst_sender: Arc<DS>,
    src_proxy_sender: Arc<PS>,
    exists_task_sender: ExistsTaskSender<F>,
    dump_pttl_task_sender: DumpPttlTaskSender<F>,
    restore_task_sender: RestoreTaskSender<F>,
    pending_umsync_task_sender: PendingUmSyncTaskSender<F::Task>,
    umsync_task_sender: UmSyncTaskSender<F>,
    del_task_sender: DeleteKeyTaskSender,
    #[allow(clippy::type_complexity)]
    task_receivers: Arc<
        parking_lot::Mutex<
            Option<(
                ExistsTaskReceiver<F>,
                DumpPttlTaskReceiver<F>,
                RestoreTaskReceiver<F>,
                PendingUmSyncTaskReceiver<F::Task>,
                UmSyncTaskReceiver<F>,
                DeleteKeyTaskReceiver,
                WaitHandle,
            )>,
        >,
    >,
    cmd_task_factory: Arc<F>,
    key_lock: Arc<KeyLock>,
    stats: Arc<MigrationStats>,
    registry: Arc<WaitRegistry>,
}

impl<F, S, DS, PS> RestoreDataCmdTaskHandler<F, S, DS, PS>
where
    F: CmdTaskFactory,
    F::Task: CmdTask<TaskType = CmdTypeTuple>,
    S: CmdTaskSender<Task = ReqTask<F::Task>>,
    DS: CmdTaskSender<Task = ReqTask<WaitableTask<F::Task>>>,
    PS: CmdTaskSender<Task = ReqTask<F::Task>>,
{
    pub fn new(
        src_sender: S,
        dst_sender: DS,
        src_proxy_sender: PS,
        cmd_task_factory: Arc<F>,
        stats: Arc<MigrationStats>,
    ) -> Self {
        let src_sender = Arc::new(src_sender);
        let dst_sender = Arc::new(dst_sender);
        let src_proxy_sender = Arc::new(src_proxy_sender);
        let (exists_task_sender, exists_task_receiver) = unbounded();
        let (dump_pttl_task_sender, dump_pttl_task_receiver) = unbounded();
        let (restore_task_sender, restore_task_receiver) = unbounded();
        let (pending_umsync_task_sender, pending_umsync_task_receiver) = unbounded();
        let (umsync_task_sender, umsync_task_receiver) = unbounded();
        let (del_task_sender, del_task_receiver) = unbounded();
        let (registry, wait_handle) = WaitRegistry::new();
        let task_receivers = Arc::new(parking_lot::Mutex::new(Some((
            exists_task_receiver,
            dump_pttl_task_receiver,
            restore_task_receiver,
            pending_umsync_task_receiver,
            umsync_task_receiver,
            del_task_receiver,
            wait_handle,
        ))));
        let key_lock = Arc::new(KeyLock::new(LOCK_SHARD_SIZE));
        Self {
            src_sender,
            dst_sender,
            src_proxy_sender,
            exists_task_sender,
            dump_pttl_task_sender,
            restore_task_sender,
            pending_umsync_task_sender,
            umsync_task_sender,
            del_task_sender,
            task_receivers,
            cmd_task_factory,
            key_lock,
            stats,
            registry: Arc::new(registry),
        }
    }

    pub fn get_stop_handle(&self) -> StopHandle<F> {
        StopHandle {
            exists_task_sender: self.exists_task_sender.clone(),
        }
    }

    // For `select!`
    #[allow(clippy::panic)]
    pub async fn run_task_handler(&self) {
        let exists_task_sender = self.exists_task_sender.clone();
        let dump_pttl_task_sender = self.dump_pttl_task_sender.clone();
        let umsync_task_sender = self.umsync_task_sender.clone();
        let del_task_sender = self.del_task_sender.clone();
        let src_sender = self.src_sender.clone();
        let dst_sender = self.dst_sender.clone();
        let src_proxy_sender = self.src_proxy_sender.clone();
        let restore_task_sender = self.restore_task_sender.clone();
        let cmd_task_factory = self.cmd_task_factory.clone();
        let key_lock = self.key_lock.clone();

        let receiver_opt = self.task_receivers.lock().take();
        let (
            exists_task_receiver,
            dump_pttl_task_receiver,
            restore_task_receiver,
            pending_umsync_task_receiver,
            umsync_task_receiver,
            del_task_receiver,
            wait_handle,
        ) = match receiver_opt {
            Some(r) => r,
            None => {
                error!("RestoreDataCmdTaskHandler has already been started");
                return;
            }
        };

        let exists_task_handler = Self::handle_exists_task(
            exists_task_sender,
            exists_task_receiver,
            dump_pttl_task_sender,
            src_sender.clone(),
            dst_sender.clone(),
            cmd_task_factory.clone(),
            key_lock.clone(),
            self.stats.clone(),
            self.registry.clone(),
        );

        let pending_umsync_task_handler = Self::handle_pending_umsync_task(
            pending_umsync_task_receiver,
            umsync_task_sender,
            src_proxy_sender,
            dst_sender.clone(),
            key_lock.clone(),
            cmd_task_factory.clone(),
            self.stats.clone(),
            self.registry.clone(),
        );

        let dump_pttl_task_handler = Self::handle_dump_pttl_task(
            dump_pttl_task_receiver,
            restore_task_sender,
            dst_sender.clone(),
            cmd_task_factory.clone(),
            self.stats.clone(),
            self.registry.clone(),
        );

        let restore_task_handler = Self::handle_restore(
            restore_task_receiver,
            src_sender,
            del_task_sender.clone(),
            cmd_task_factory,
        );

        let umsync_task_handler = Self::handle_umsync_task(
            umsync_task_receiver,
            dst_sender,
            self.stats.clone(),
            self.registry.clone(),
        );

        let del_task_handler = Self::handle_del_task(del_task_receiver);

        let mut exists_task_handler = Box::pin(exists_task_handler.fuse());
        let mut pending_umsync_task_handler = Box::pin(pending_umsync_task_handler.fuse());
        let mut dump_pttl_task_handler = Box::pin(dump_pttl_task_handler.fuse());
        let mut restore_task_handler = Box::pin(restore_task_handler.fuse());
        let mut umsync_task_handler = Box::pin(umsync_task_handler.fuse());
        let mut del_task_handler = Box::pin(del_task_handler.fuse());
        let mut handle_wait_handler = Box::pin(wait_handle.map(|_| ()).fuse());

        select! {
            () = exists_task_handler => {},
            () = pending_umsync_task_handler => {},
            () = dump_pttl_task_handler => {},
            () = restore_task_handler => {},
            () = umsync_task_handler => {},
            () = del_task_handler => {},
            () = handle_wait_handler => {},
        }

        // Need to finish the remaining commands before exiting.
        info!("wait for pending umsync task");
        self.pending_umsync_task_sender.close_channel();

        select! {
            () = pending_umsync_task_handler => {},
            () = dump_pttl_task_handler => {},
            () = restore_task_handler => {},
            () = umsync_task_handler => {},
            () = del_task_handler => {},
            () = handle_wait_handler => {},
        }

        info!("wait for dump and pttl task");
        self.dump_pttl_task_sender.close_channel();

        select! {
            () = dump_pttl_task_handler => {},
            () = restore_task_handler => {},
            () = umsync_task_handler => {},
            () = del_task_handler => {},
            () = handle_wait_handler => {},
        }

        self.restore_task_sender.close_channel();
        info!("wait for restore task");

        select! {
            () = restore_task_handler => {},
            () = umsync_task_handler => {},
            () = del_task_handler => {},
            () = handle_wait_handler => {},
        }

        self.umsync_task_sender.close_channel();
        info!("wait for umsync task");
        select! {
            () = umsync_task_handler => {},
            () = del_task_handler => {},
            () = handle_wait_handler => {},
        }

        self.del_task_sender.close_channel();
        info!("wait for del task");
        select! {
            () = del_task_handler => {},
            () = handle_wait_handler => {},
        }

        self.registry.close();
        info!("wait for wait_handle task");
        handle_wait_handler.await;
        info!("All remaining tasks in migration backend are finished");
    }

    #[allow(clippy::too_many_arguments, clippy::cognitive_complexity)]
    async fn handle_exists_task(
        exists_task_sender: ExistsTaskSender<F>,
        mut exists_task_receiver: ExistsTaskReceiver<F>,
        dump_pttl_task_sender: DumpPttlTaskSender<F>,
        src_sender: Arc<S>,
        dst_sender: Arc<DS>,
        cmd_task_factory: Arc<F>,
        key_lock: Arc<KeyLock>,
        stats: Arc<MigrationStats>,
        registry: Arc<WaitRegistry>,
    ) {
        while let Some((state, reply_receiver)) = exists_task_receiver.next().await {
            let res = reply_receiver.await;
            let key_exists = match Self::parse_exists_result(res) {
                Ok(key_exists) => key_exists,
                Err(()) => continue,
            };

            if key_exists {
                stats
                    .importing_dst_key_existed
                    .fetch_add(1, Ordering::Relaxed);

                let (_state, req_task) =
                    MgrCmdStateForward::from_state_exists(state, registry.clone());
                if let Err(err) = dst_sender.send(req_task) {
                    debug!("failed to forward: {:?}", err);
                }
                continue;
            }
            stats
                .importing_dst_key_not_existed
                .fetch_add(1, Ordering::Relaxed);

            // Avoid restoring and deleting at the same time.
            let (lock_guard, state) = match key_lock.lock(state.key.clone(), state.lock_slot) {
                Some(lock_guard) => {
                    stats.importing_lock_success.fetch_add(1, Ordering::Relaxed);
                    (lock_guard, state)
                }
                None => {
                    stats.importing_lock_failed.fetch_add(1, Ordering::Relaxed);
                    stats
                        .importing_resend_exists
                        .fetch_add(1, Ordering::Relaxed);

                    // Retry later
                    let MgrCmdStateExists {
                        inner_task,
                        key,
                        lock_slot,
                    } = state;
                    let (state, reply_receiver) = match Self::resend_to_exist_to_dst(
                        inner_task,
                        &(*cmd_task_factory),
                        key,
                        lock_slot,
                        &dst_sender,
                    ) {
                        Ok(r) => r,
                        Err(()) => continue,
                    };

                    // Just sending back (state, future::ready(resp)) could possibly result in dead loop.
                    // The whole task will just get stuck in this function.
                    // So we have to make the future `reply_receiver` not always ready.
                    // And checking the key again by `EXISTS` is a good choice for reducing
                    // unnecessary DUMP and PTTL.
                    match exists_task_sender.unbounded_send((state, reply_receiver)) {
                        Ok(()) => continue,
                        Err(err) => {
                            stats
                                .importing_resend_exists_failed
                                .fetch_add(1, Ordering::Relaxed);
                            let (state, reply_receiver) = err.into_inner();

                            let key_exists = match Self::parse_exists_result(reply_receiver.await) {
                                Ok(key_exists) => key_exists,
                                Err(()) => continue,
                            };
                            if key_exists {
                                let (_state, req_task) =
                                    MgrCmdStateForward::from_state_exists(state, registry.clone());
                                if let Err(err) = dst_sender.send(req_task) {
                                    debug!("failed to forward: {:?}", err);
                                }
                                continue;
                            }

                            loop {
                                stats
                                    .importing_lock_loop_retry
                                    .fetch_add(1, Ordering::Relaxed);
                                warn!("EXISTS channel is closed. Waiting to get lock.");
                                match key_lock.lock(state.key.clone(), state.lock_slot) {
                                    Some(lock_guard) => break (lock_guard, state),
                                    None => tokio::time::sleep(Duration::from_millis(3)).await,
                                }
                            }
                        }
                    }
                }
            };

            let (state, req_task, reply_fut) =
                MgrCmdStateDumpPttl::from_state_exists(state, &(*cmd_task_factory), lock_guard);
            if let Err(err) = src_sender.send(req_task) {
                debug!("failed to send dump pttl: {:?}", err);
            }
            if let Err(_err) = dump_pttl_task_sender.unbounded_send((state, reply_fut)) {
                debug!("dump_pttl_task_sender is canceled");
            }
        }
    }

    fn parse_exists_result(result: Result<RespVec, CommandError>) -> Result<bool, ()> {
        let resp = match result {
            Ok(resp) => resp,
            Err(err) => {
                error!("failed to get exists cmd response: {:?}", err);
                return Err(());
            }
        };
        let key_exists = match &resp {
            Resp::Integer(num) => num.as_slice() != KEY_NOT_EXISTS.as_bytes(),
            others => {
                error!("Unexpected reply from EXISTS: {:?}. Skip it.", others);
                return Err(());
            }
        };
        Ok(key_exists)
    }

    async fn handle_dump_pttl_task(
        mut dump_pttl_task_receiver: DumpPttlTaskReceiver<F>,
        restore_task_sender: RestoreTaskSender<F>,
        dst_sender: Arc<DS>,
        cmd_task_factory: Arc<F>,
        stats: Arc<MigrationStats>,
        registry: Arc<WaitRegistry>,
    ) {
        while let Some((state, reply_fut)) = dump_pttl_task_receiver.next().await {
            let res = reply_fut.await;
            let entry = match res {
                Ok(Some(entry)) => {
                    stats
                        .importing_src_key_existed
                        .fetch_add(1, Ordering::Relaxed);
                    entry
                }
                Ok(None) => {
                    stats
                        .importing_src_key_not_existed
                        .fetch_add(1, Ordering::Relaxed);
                    // The key also does not exist in source node.
                    let (_state, req_task) =
                        MgrCmdStateForward::from_state_dump_pttl(state, registry.clone());
                    if let Err(err) = dst_sender.send(req_task) {
                        debug!("failed to send forward: {:?}", err);
                    }
                    continue;
                }
                Err(err) => {
                    stats.importing_src_failed.fetch_add(1, Ordering::Relaxed);
                    let task = state.into_inner();
                    task.set_resp_result(Ok(Resp::Error(
                        format!("{}: {:?}", FAILED_TO_ACCESS_SOURCE, err).into_bytes(),
                    )));
                    error!("failed to get exists cmd response: {:?}. Skip it", err);
                    continue;
                }
            };

            let (state, req_task, reply_receiver) = MgrCmdStateRestoreForward::from_state_dump(
                state,
                entry,
                &(*cmd_task_factory),
                registry.clone(),
            );

            if let Err(err) = dst_sender.send(req_task) {
                debug!("failed to send restore and forward: {:?}", err);
            }

            if let Err(err) = restore_task_sender.unbounded_send((state, reply_receiver)) {
                debug!("failed to send restore task to queue: {:?}", err);
            }
        }
    }

    async fn handle_restore(
        mut restore_task_receiver: RestoreTaskReceiver<F>,
        src_sender: Arc<S>,
        del_task_sender: DeleteKeyTaskSender,
        cmd_task_factory: Arc<F>,
    ) {
        while let Some((state, reply_fut)) = restore_task_receiver.next().await {
            let resp = match reply_fut.await {
                Ok(resp) => resp,
                Err(err) => {
                    error!("failed to restore: {:?}", err);
                    continue;
                }
            };
            let MgrCmdStateRestoreForward {
                key,
                task_context,
                lock_guard,
            } = state;
            drop(lock_guard);

            const BUSYKEY: &[u8] = b"BUSYKEY";
            match resp {
                Resp::Simple(_) => (),
                // Key already existed
                Resp::Error(err)
                    if err.get(..BUSYKEY.len()).map(|p| p == BUSYKEY) == Some(true) => {}
                others => {
                    let pretty_resp = others.as_ref().map(|s| pretty_print_bytes(s));
                    error!("unexpected RESTORE result: {:?}", pretty_resp);
                    continue;
                }
            }

            let (_state, req_task, reply_receiver) =
                MgrCmdStateDel::from_task_context(task_context, key, &(*cmd_task_factory));

            if let Err(err) = src_sender.send(req_task) {
                warn!("failed to send DEL to source node: {:?}", err);
                continue;
            }
            if del_task_sender.unbounded_send(reply_receiver).is_err() {
                warn!("failed to send del key task");
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_pending_umsync_task(
        mut pending_umsync_task_receiver: PendingUmSyncTaskReceiver<F::Task>,
        umsync_task_sender: UmSyncTaskSender<F>,
        src_proxy_sender: Arc<PS>,
        dst_sender: Arc<DS>,
        key_lock: Arc<KeyLock>,
        cmd_task_factory: Arc<F>,
        stats: Arc<MigrationStats>,
        registry: Arc<WaitRegistry>,
    ) {
        while let Some(pending_task) = pending_umsync_task_receiver.next().await {
            let PendingUmSyncTask {
                cmd_task,
                key,
                lock_slot,
            } = pending_task;

            let mut lock_guard_opt = None;
            const RETRY_TIMES: usize = 10;
            for _ in 0..RETRY_TIMES {
                if let Some(lock_guard) = key_lock.lock(key.clone(), lock_slot) {
                    lock_guard_opt = Some(lock_guard);
                    break;
                }
                stats
                    .importing_umsync_lock_failed_again
                    .fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
            let lock_guard = match lock_guard_opt {
                Some(lock_guard) => lock_guard,
                None => {
                    cmd_task
                        .set_resp_result(Ok(Resp::Error(b"MIGRATION_KEY_LOCK_TIMEOUT".to_vec())));
                    continue;
                }
            };

            let (state, req_task, reply_fut) =
                MgrCmdStateUmSync::from_task(cmd_task, key, lock_guard, cmd_task_factory.as_ref());
            if let Err(err) = src_proxy_sender.send(req_task) {
                error!("failed to send umsync: {:?}", err);
                state.inner_task.set_resp_result(Ok(Resp::Error(
                    b"Failed to send UMSYNC during migration after pending".to_vec(),
                )));
                continue;
            }
            if let Err(err) = umsync_task_sender.unbounded_send((state, reply_fut)) {
                // The queue has been closed so the migration should be done.
                // We can safely send it to the dst_sender.
                let MgrCmdStateUmSync { inner_task, .. } = err.into_inner().0;
                let task = WaitableTask::new_with_registry(inner_task, registry.clone());
                if let Err(err) = dst_sender.send(ReqTask::Simple(task)) {
                    error!(
                        "failed to send to dst sender after migration is done: {:?}",
                        err
                    );
                    continue;
                }
            }
        }
    }

    async fn handle_umsync_task(
        mut umsync_task_receiver: UmSyncTaskReceiver<F>,
        dst_sender: Arc<DS>,
        stats: Arc<MigrationStats>,
        registry: Arc<WaitRegistry>,
    ) {
        while let Some((state, reply_fut)) = umsync_task_receiver.next().await {
            // The DUMP and RESTORE has already been processed in the source proxy.
            // We can safely drop the lock here after reply_fut is done.
            match reply_fut.await {
                Err(err) => {
                    stats
                        .importing_umsync_failed
                        .fetch_add(1, Ordering::Relaxed);

                    // drop the lock here
                    let task = state.into_inner();
                    task.set_resp_result(Ok(Resp::Error(
                        format!("{}: {:?}", FAILED_TO_ACCESS_SOURCE, err).into_bytes(),
                    )));
                    continue;
                }
                Ok(Resp::Error(err)) if err != response::MIGRATION_TASK_NOT_FOUND.as_bytes() => {
                    stats
                        .importing_umsync_failed
                        .fetch_add(1, Ordering::Relaxed);

                    error!("Invalid reply of UMSYNC {:?}", err);
                    // drop the lock here
                    let task = state.into_inner();
                    task.set_resp_result(Ok(Resp::Error(
                        format!("{}: {:?}", FAILED_TO_ACCESS_SOURCE, err).into_bytes(),
                    )));
                    continue;
                }
                _ => (),
            };

            let (_state, req_task) = MgrCmdStateForward::from_state_umsync(state, registry.clone());
            if let Err(err) = dst_sender.send(req_task) {
                debug!("failed to forward: {:?}", err);
            }
        }
    }

    async fn handle_del_task(mut del_task_receiver: DeleteKeyTaskReceiver) {
        while let Some(reply_fut) = del_task_receiver.next().await {
            let resp = match reply_fut.await {
                Ok(reply) => reply,
                Err(err) => {
                    error!("failed to delete keys from source proxy: {:?}", err);
                    continue;
                }
            };
            if let Resp::Error(err) = resp {
                error!(
                    "failed to delete keys from source proxy. response: {:?}",
                    err
                );
            }
        }
    }

    pub fn handle_cmd_task(&self, cmd_task: F::Task) -> Result<(), RetryError<F::Task>> {
        let (key, lock_slot) = match cmd_task.get_key() {
            Some(key) => (key.to_vec(), generate_lock_slot(key)),
            _ => {
                cmd_task.set_resp_result(Ok(Resp::Error(
                    String::from("Missing key while migrating").into_bytes(),
                )));
                return Ok(());
            }
        };

        let (_, data_cmd_type) = cmd_task.get_type();
        if requires_blocking_migration(data_cmd_type) {
            self.stats
                .importing_blocking_migration_commands
                .fetch_add(1, Ordering::Relaxed);
            return self.send_to_umsync(cmd_task, key, lock_slot);
        }

        self.stats
            .importing_non_blocking_migration_commands
            .fetch_add(1, Ordering::Relaxed);

        let (state, reply_fut) = match Self::send_to_exist_to_dst(
            cmd_task,
            &(*self.cmd_task_factory),
            key,
            lock_slot,
            &self.dst_sender,
        ) {
            Ok(r) => r,
            Err(err) => {
                return match err {
                    SenderBackendError::Retry(task) => Err(RetryError::new(task)),
                    _other_err => Ok(()),
                }
            }
        };
        if let Err(err) = self.exists_task_sender.unbounded_send((state, reply_fut)) {
            let (state, _) = err.into_inner();
            let cmd_task: F::Task = state.into_inner();
            warn!("Migration backend: exists task sender is canceled");
            return Err(RetryError::new(cmd_task));
        }

        Ok(())
    }

    fn send_to_umsync(
        &self,
        cmd_task: F::Task,
        key: BinSafeStr,
        lock_slot: usize,
    ) -> Result<(), RetryError<F::Task>> {
        // Avoid restoring and deleting at the same time.
        let (lock_guard, cmd_task) = match self.key_lock.lock(key.clone(), lock_slot) {
            Some(lock_guard) => (lock_guard, cmd_task),
            None => {
                self.stats
                    .importing_umsync_lock_failed
                    .fetch_add(1, Ordering::Relaxed);
                // Retry later
                let res = self
                    .pending_umsync_task_sender
                    .unbounded_send(PendingUmSyncTask {
                        cmd_task,
                        key,
                        lock_slot,
                    });
                if let Err(err) = res {
                    // The queue has been closed, the migration should be done. Retry it.
                    let PendingUmSyncTask { cmd_task: task, .. } = err.into_inner();
                    return Err(RetryError::new(task));
                }

                return Ok(());
            }
        };
        self.stats
            .importing_umsync_lock_success
            .fetch_add(1, Ordering::Relaxed);

        let (state, req_task, reply_fut) =
            MgrCmdStateUmSync::from_task(cmd_task, key, lock_guard, self.cmd_task_factory.as_ref());
        if let Err(err) = self.src_proxy_sender.send(req_task) {
            error!("failed to send umsync: {:?}", err);
            state.inner_task.set_resp_result(Ok(Resp::Error(
                b"Failed to send UMSYNC during migration".to_vec(),
            )));
            return Ok(());
        }
        if let Err(err) = self.umsync_task_sender.unbounded_send((state, reply_fut)) {
            // The queue has been closed, retry it.
            let MgrCmdStateUmSync { inner_task, .. } = err.into_inner().0;
            return Err(RetryError::new(inner_task));
        }
        Ok(())
    }

    fn send_to_exist_to_dst(
        cmd_task: F::Task,
        cmd_task_factory: &F,
        key: BinSafeStr,
        lock_slot: usize,
        dst_sender: &DS,
    ) -> Result<(MgrCmdStateExists<F>, ReplyFuture), SenderBackendError<F::Task>> {
        let (state, task, reply_fut) =
            MgrCmdStateExists::from_task(cmd_task, key, lock_slot, cmd_task_factory);
        if let Err(err) = dst_sender.send(task) {
            let cmd_task: F::Task = state.into_inner();

            return match BackendError::from_sender_backend_error(err) {
                Either::Right(_retry_err) => {
                    error!("failed to send task to exist channel: retry");
                    Err(SenderBackendError::Retry(cmd_task))
                }
                Either::Left(BackendError::Canceled) => {
                    error!("failed to send task to exist channel: canceled");
                    Err(SenderBackendError::Retry(cmd_task))
                }
                Either::Left(backend_err) => {
                    cmd_task.set_resp_result(Ok(Resp::Error(
                        format!("Migration backend error: {:?}", backend_err).into_bytes(),
                    )));
                    Err(SenderBackendError::from_backend_error(backend_err))
                }
            };
        }
        Ok((state, reply_fut))
    }

    fn resend_to_exist_to_dst(
        cmd_task: F::Task,
        cmd_task_factory: &F,
        key: BinSafeStr,
        lock_slot: usize,
        dst_sender: &DS,
    ) -> Result<(MgrCmdStateExists<F>, ReplyFuture), ()> {
        let (state, task, reply_fut) =
            MgrCmdStateExists::from_task(cmd_task, key, lock_slot, cmd_task_factory);
        if let Err(err) = dst_sender.send(task) {
            let cmd_task: F::Task = state.into_inner();
            cmd_task.set_resp_result(Ok(Resp::Error(
                format!("Migration backend error: wait for lock {:?}", err).into_bytes(),
            )));
            return Err(());
        }
        Ok((state, reply_fut))
    }
}

pub struct StopHandle<F: CmdTaskFactory> {
    exists_task_sender: ExistsTaskSender<F>,
}

impl<F: CmdTaskFactory> StopHandle<F> {
    // Let drop function do the work.
    pub fn stop(self) {}
}

impl<F: CmdTaskFactory> Drop for StopHandle<F> {
    fn drop(&mut self) {
        info!("RestoreDataCmdTaskHandler start to stop");
        self.exists_task_sender.close_channel();
    }
}

const LOCK_SHARD_SIZE: usize = 256;

struct KeyLock {
    inner: Arc<KeyLockInner>,
}

impl KeyLock {
    fn new(shard_size: usize) -> Self {
        Self {
            inner: Arc::new(KeyLockInner::new(shard_size)),
        }
    }

    fn lock(&self, key: BinSafeStr, lock_slot: usize) -> Option<KeyLockGuard> {
        if self.inner.lock(key.clone(), lock_slot) {
            Some(KeyLockGuard {
                inner: self.inner.clone(),
                key,
                lock_slot,
            })
        } else {
            None
        }
    }
}

struct KeyLockInner {
    shards: Vec<DashSet<BinSafeStr>>,
}

impl KeyLockInner {
    fn new(shard_size: usize) -> Self {
        let mut shards = Vec::with_capacity(shard_size);
        for _ in 0..shard_size {
            shards.push(DashSet::new());
        }
        Self { shards }
    }

    fn lock(&self, key: BinSafeStr, lock_slot: usize) -> bool {
        let shard_slot = lock_slot % self.shards.len();
        let s = match self.shards.get(shard_slot) {
            None => return false,
            Some(s) => s,
        };
        s.insert(key)
    }

    fn unlock(&self, key: &[u8], lock_slot: usize) {
        let shard_slot = lock_slot % self.shards.len();
        let s = match self.shards.get(shard_slot) {
            None => return,
            Some(s) => s,
        };
        s.remove(key);
    }
}

struct KeyLockGuard {
    inner: Arc<KeyLockInner>,
    key: BinSafeStr,
    lock_slot: usize,
}

impl fmt::Debug for KeyLockGuard {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "KeyLockGuard<key: {:?}, lock_slot: {}>",
            self.key, self.lock_slot
        )
    }
}

impl Drop for KeyLockGuard {
    fn drop(&mut self) {
        self.inner.unlock(self.key.as_slice(), self.lock_slot);
    }
}

#[cfg(test)]
mod tests {
    use super::super::command::{new_command_pair, CmdReplyReceiver, Command};
    use super::super::session::{CmdCtx, CmdCtxFactory};
    use super::*;
    use crate::protocol::RespPacket;
    use crate::protocol::{BulkStr, Resp};
    use dashmap::DashMap;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio;

    #[derive(Debug, Clone, Copy)]
    enum ErrType {
        ErrorReply,
        ConnError,
    }

    struct DummyCmdTaskSender {
        exists: AtomicBool,
        closed: AtomicBool,
        cmd_count: DashMap<String, AtomicUsize>,
        err_set: HashMap<&'static str, ErrType>,
        pttl: i64,
    }

    impl DummyCmdTaskSender {
        fn new(exists: bool, err_set: HashMap<&'static str, ErrType>, pttl: i64) -> Self {
            Self {
                exists: AtomicBool::new(exists),
                closed: AtomicBool::new(false),
                cmd_count: DashMap::new(),
                err_set,
                pttl,
            }
        }

        fn handle(&self, cmd_ctx: CmdCtx) {
            let cmd_name = cmd_ctx
                .get_cmd()
                .get_command_name()
                .expect("DummyCmdTaskSender::handle")
                .to_string()
                .to_uppercase();

            let cmd = cmd_name.as_str();
            self.cmd_count
                .entry(cmd_name.clone())
                .or_insert_with(|| AtomicUsize::new(0));
            if let Some(counter) = self.cmd_count.get(cmd) {
                counter.fetch_add(1, Ordering::SeqCst);
            }

            if self.closed.load(Ordering::SeqCst) {
                cmd_ctx.set_resp_result(Err(CommandError::Canceled));
                return;
            }

            match self.get_cmd_err(cmd) {
                Some(ErrType::ErrorReply) => {
                    cmd_ctx.set_resp_result(Ok(Resp::Error(
                        format!("{} cmd error", cmd).into_bytes(),
                    )));
                    return;
                }
                Some(ErrType::ConnError) => {
                    cmd_ctx.set_resp_result(Err(CommandError::Canceled));
                    self.closed.store(true, Ordering::SeqCst);
                    return;
                }
                None => (),
            }

            match cmd {
                "EXISTS" => {
                    let reply = if self.key_exists() { "1" } else { "0" };
                    cmd_ctx.set_resp_result(Ok(Resp::Integer(reply.to_string().into_bytes())));
                }
                "GET" => {
                    if self.key_exists() {
                        cmd_ctx.set_resp_result(Ok(Resp::Bulk(BulkStr::Str(
                            "get_reply".to_string().into_bytes(),
                        ))));
                    } else {
                        cmd_ctx.set_resp_result(Ok(Resp::Bulk(BulkStr::Nil)));
                    }
                }
                "DUMP" => {
                    if self.key_exists() {
                        cmd_ctx.set_resp_result(Ok(Resp::Bulk(BulkStr::Str(
                            "dump_reply".to_string().into_bytes(),
                        ))));
                    } else {
                        cmd_ctx.set_resp_result(Ok(Resp::Bulk(BulkStr::Nil)));
                    }
                }
                "PTTL" => {
                    cmd_ctx.set_resp_result(Ok(Resp::Integer(self.pttl.to_string().into_bytes())));
                }
                "RESTORE" => {
                    let ttl = cmd_ctx.get_cmd().get_command_element(2).unwrap().to_vec();
                    assert_ne!(ttl[0], b'-');
                    self.exists.store(true, Ordering::SeqCst);
                    cmd_ctx.set_resp_result(Ok(Resp::Simple("OK".to_string().into_bytes())));
                }
                "UMSYNC" => {
                    cmd_ctx.set_resp_result(Ok(Resp::Simple(b"OK".to_vec())));
                }
                "DEL" => {
                    cmd_ctx.set_resp_result(Ok(Resp::Integer("1".to_string().into_bytes())));
                }
                _ => {
                    cmd_ctx.set_resp_result(Ok(Resp::Error(
                        "unexpected command".to_string().into_bytes(),
                    )));
                }
            }
        }

        fn key_exists(&self) -> bool {
            self.exists.load(Ordering::SeqCst)
        }

        fn get_cmd_err(&self, cmd: &str) -> Option<ErrType> {
            self.err_set.get(cmd).cloned()
        }

        fn get_cmd_count(&self, cmd: &str) -> Option<usize> {
            self.cmd_count
                .get(cmd)
                .map(|count| count.load(Ordering::SeqCst))
        }
    }

    impl CmdTaskSender for DummyCmdTaskSender {
        type Task = ReqTask<CmdCtx>;

        fn send(&self, req_task: Self::Task) -> Result<(), SenderBackendError<Self::Task>> {
            match req_task {
                ReqTask::Simple(cmd_task) => self.handle(cmd_task),
                ReqTask::Multi(cmd_task_arr) => {
                    for cmd_task in cmd_task_arr.into_iter() {
                        self.handle(cmd_task);
                    }
                }
            }
            Ok(())
        }
    }

    struct DummyWaitTaskSender {
        sender: DummyCmdTaskSender,
    }

    impl DummyWaitTaskSender {
        fn new(exists: bool, err_set: HashMap<&'static str, ErrType>, pttl: i64) -> Self {
            Self {
                sender: DummyCmdTaskSender::new(exists, err_set, pttl),
            }
        }

        fn get_cmd_count(&self, cmd: &str) -> Option<usize> {
            self.sender.get_cmd_count(cmd)
        }
    }

    impl CmdTaskSender for DummyWaitTaskSender {
        type Task = ReqTask<WaitableTask<CmdCtx>>;

        fn send(&self, req_task: Self::Task) -> Result<(), SenderBackendError<Self::Task>> {
            match req_task {
                ReqTask::Simple(waitable_task) => self.sender.handle(waitable_task.into_inner()),
                ReqTask::Multi(cmd_task_arr) => {
                    for waitable_task in cmd_task_arr.into_iter() {
                        self.sender.handle(waitable_task.into_inner());
                    }
                }
            }
            Ok(())
        }
    }

    fn gen_test_cmd_ctx(command: Vec<&'static str>) -> (CmdCtx, CmdReplyReceiver) {
        let resp = Resp::Arr(Array::Arr(
            command
                .into_iter()
                .map(|s| Resp::Bulk(BulkStr::Str(s.to_string().into_bytes())))
                .collect(),
        ));
        let packet = Box::new(RespPacket::from_resp_vec(resp));
        let cmd = Command::new(packet);
        let (reply_sender, reply_receiver) = new_command_pair(&cmd);
        let cmd_ctx = CmdCtx::new(cmd, reply_sender, 0, true);
        (cmd_ctx, reply_receiver)
    }

    async fn gen_reply_future(reply_receiver: CmdReplyReceiver) -> Result<BinSafeStr, ()> {
        reply_receiver
            .await
            .map_err(|err| error!("cmd err: {:?}", err))
            .map(|task_reply| {
                let (_, packet, _) = task_reply.into_inner();
                match packet.to_resp_slice() {
                    Resp::Bulk(BulkStr::Str(s)) => s.to_vec(),
                    Resp::Bulk(BulkStr::Nil) => "key_not_exists".to_string().into_bytes(),
                    Resp::Integer(n) => n.to_vec(),
                    Resp::Error(err_str) => err_str.to_vec(),
                    others => format!("invalid_reply {:?}", others).into_bytes(),
                }
            })
    }

    async fn run_future(
        handler: &RestoreDataCmdTaskHandler<
            CmdCtxFactory,
            DummyCmdTaskSender,
            DummyWaitTaskSender,
            DummyCmdTaskSender,
        >,
        reply_receiver: CmdReplyReceiver,
    ) -> BinSafeStr {
        let reply = gen_reply_future(reply_receiver);
        let run = handler
            .run_task_handler()
            .map(|()| "handler_end_first".to_string().into_bytes());
        let res = select! {
            res = reply.fuse() => res,
            s = run.fuse() => Ok(s),
        };
        res.unwrap_or_else(|err| format!("future_returns_error: {:?}", err).into_bytes())
    }

    #[tokio::test]
    async fn test_key_exists() {
        let handler = RestoreDataCmdTaskHandler::new(
            DummyCmdTaskSender::new(false, HashMap::new(), 666),
            DummyWaitTaskSender::new(true, HashMap::new(), 666),
            DummyCmdTaskSender::new(false, HashMap::new(), 0),
            Arc::new(CmdCtxFactory::default()),
            Arc::new(MigrationStats::default()),
        );

        let (cmd_ctx, reply_receiver) = gen_test_cmd_ctx(vec!["GET", "somekey"]);

        handler.handle_cmd_task(cmd_ctx).unwrap();
        let s = run_future(&handler, reply_receiver).await;

        assert_eq!(handler.dst_sender.get_cmd_count("EXISTS"), Some(1));
        assert_eq!(handler.dst_sender.get_cmd_count("GET"), Some(1));
        assert_eq!(handler.src_sender.get_cmd_count("DUMP"), None);
        assert_eq!(handler.src_sender.get_cmd_count("PTTL"), None);
        assert_eq!(handler.dst_sender.get_cmd_count("RESTORE"), None);

        assert_eq!(s, "get_reply".to_string().into_bytes());
    }

    #[tokio::test]
    async fn test_key_dst_not_exists() {
        let handler = RestoreDataCmdTaskHandler::new(
            DummyCmdTaskSender::new(true, HashMap::new(), 666),
            DummyWaitTaskSender::new(false, HashMap::new(), 1),
            DummyCmdTaskSender::new(false, HashMap::new(), 0),
            Arc::new(CmdCtxFactory::default()),
            Arc::new(MigrationStats::default()),
        );

        let (cmd_ctx, reply_receiver) = gen_test_cmd_ctx(vec!["GET", "somekey"]);

        handler.handle_cmd_task(cmd_ctx).unwrap();
        let s = run_future(&handler, reply_receiver).await;

        assert_eq!(handler.dst_sender.get_cmd_count("EXISTS"), Some(1));
        assert_eq!(handler.dst_sender.get_cmd_count("GET"), Some(1));
        assert_eq!(handler.src_sender.get_cmd_count("DUMP"), Some(1));
        assert_eq!(handler.src_sender.get_cmd_count("PTTL"), Some(1));
        assert_eq!(handler.dst_sender.get_cmd_count("RESTORE"), Some(1));
        // This is async
        // assert_eq!(handler.src_sender.get_cmd_count("DEL"), Some(1));
        assert_eq!(handler.dst_sender.get_cmd_count("DEL"), None);

        assert_eq!(s, "get_reply".to_string().into_bytes());
    }

    #[tokio::test]
    async fn test_key_both_not_exists() {
        let handler = RestoreDataCmdTaskHandler::new(
            DummyCmdTaskSender::new(false, HashMap::new(), 666),
            DummyWaitTaskSender::new(false, HashMap::new(), 233),
            DummyCmdTaskSender::new(false, HashMap::new(), 0),
            Arc::new(CmdCtxFactory::default()),
            Arc::new(MigrationStats::default()),
        );

        let (cmd_ctx, reply_receiver) = gen_test_cmd_ctx(vec!["GET", "somekey"]);

        handler.handle_cmd_task(cmd_ctx).unwrap();
        let s = run_future(&handler, reply_receiver).await;

        assert_eq!(handler.dst_sender.get_cmd_count("EXISTS"), Some(1));
        assert_eq!(handler.dst_sender.get_cmd_count("GET"), Some(1));
        assert_eq!(handler.src_sender.get_cmd_count("DUMP"), Some(1));
        assert_eq!(handler.src_sender.get_cmd_count("PTTL"), Some(1));
        assert_eq!(handler.dst_sender.get_cmd_count("RESTORE"), None);
        assert_eq!(handler.src_sender.get_cmd_count("DEL"), None);
        assert_eq!(handler.dst_sender.get_cmd_count("DEL"), None);

        assert_eq!(s, "key_not_exists".to_string().into_bytes());
    }

    #[tokio::test]
    async fn test_key_exists_with_exists_err() {
        let mut err_set1 = HashMap::new();
        err_set1.insert("EXISTS", ErrType::ErrorReply);
        let mut err_set2 = HashMap::new();
        err_set2.insert("EXISTS", ErrType::ConnError);

        for err_set in &[err_set1, err_set2] {
            let handler = RestoreDataCmdTaskHandler::new(
                DummyCmdTaskSender::new(false, HashMap::new(), 666),
                DummyWaitTaskSender::new(true, err_set.clone(), 666),
                DummyCmdTaskSender::new(false, HashMap::new(), 0),
                Arc::new(CmdCtxFactory::default()),
                Arc::new(MigrationStats::default()),
            );

            let (cmd_ctx, reply_receiver) = gen_test_cmd_ctx(vec!["GET", "somekey"]);

            handler.handle_cmd_task(cmd_ctx).unwrap();
            let s = run_future(&handler, reply_receiver).await;

            assert_eq!(handler.dst_sender.get_cmd_count("EXISTS"), Some(1));
            assert_eq!(handler.dst_sender.get_cmd_count("GET"), None);
            assert_eq!(handler.src_sender.get_cmd_count("DUMP"), None);
            assert_eq!(handler.src_sender.get_cmd_count("PTTL"), None);
            assert_eq!(handler.dst_sender.get_cmd_count("RESTORE"), None);
            assert_eq!(handler.src_sender.get_cmd_count("DEL"), None);
            assert_eq!(handler.dst_sender.get_cmd_count("DEL"), None);

            assert!(s.starts_with("future_returns_error".as_bytes()));
        }
    }

    #[tokio::test]
    async fn test_key_exists_with_get_err() {
        let mut err_set1 = HashMap::new();
        err_set1.insert("GET", ErrType::ErrorReply);
        let mut err_set2 = HashMap::new();
        err_set2.insert("GET", ErrType::ConnError);

        for (i, err_set) in [err_set1, err_set2].iter().enumerate() {
            let handler = RestoreDataCmdTaskHandler::new(
                DummyCmdTaskSender::new(false, HashMap::new(), 666),
                DummyWaitTaskSender::new(true, err_set.clone(), 666),
                DummyCmdTaskSender::new(false, HashMap::new(), 0),
                Arc::new(CmdCtxFactory::default()),
                Arc::new(MigrationStats::default()),
            );

            let (cmd_ctx, reply_receiver) = gen_test_cmd_ctx(vec!["GET", "somekey"]);

            handler.handle_cmd_task(cmd_ctx).unwrap();
            let s = run_future(&handler, reply_receiver).await;

            assert_eq!(handler.dst_sender.get_cmd_count("EXISTS"), Some(1));
            assert_eq!(handler.dst_sender.get_cmd_count("GET"), Some(1));
            assert_eq!(handler.src_sender.get_cmd_count("DUMP"), None);
            assert_eq!(handler.src_sender.get_cmd_count("PTTL"), None);
            assert_eq!(handler.dst_sender.get_cmd_count("RESTORE"), None);
            assert_eq!(handler.src_sender.get_cmd_count("DEL"), None);
            assert_eq!(handler.dst_sender.get_cmd_count("DEL"), None);

            if i == 0 {
                assert_eq!(s, "GET cmd error".as_bytes());
            } else {
                assert!(s.starts_with("future_returns_error".as_bytes()));
            }
        }
    }

    #[tokio::test]
    async fn test_key_exists_with_dump_err() {
        let mut err_set1 = HashMap::new();
        err_set1.insert("DUMP", ErrType::ErrorReply);
        let mut err_set2 = HashMap::new();
        err_set2.insert("DUMP", ErrType::ConnError);

        for err_set in &[err_set1, err_set2] {
            let handler = RestoreDataCmdTaskHandler::new(
                DummyCmdTaskSender::new(true, err_set.clone(), 666),
                DummyWaitTaskSender::new(false, HashMap::new(), 666),
                DummyCmdTaskSender::new(false, HashMap::new(), 0),
                Arc::new(CmdCtxFactory::default()),
                Arc::new(MigrationStats::default()),
            );

            let (cmd_ctx, reply_receiver) = gen_test_cmd_ctx(vec!["GET", "somekey"]);

            handler.handle_cmd_task(cmd_ctx).unwrap();
            let s = run_future(&handler, reply_receiver).await;

            assert_eq!(handler.dst_sender.get_cmd_count("EXISTS"), Some(1));
            assert_eq!(handler.dst_sender.get_cmd_count("GET"), None);
            assert_eq!(handler.src_sender.get_cmd_count("DUMP"), Some(1));
            assert_eq!(handler.src_sender.get_cmd_count("PTTL"), Some(1));
            assert_eq!(handler.dst_sender.get_cmd_count("RESTORE"), None);
            assert_eq!(handler.src_sender.get_cmd_count("DEL"), None);
            assert_eq!(handler.dst_sender.get_cmd_count("DEL"), None);

            assert!(s.starts_with(FAILED_TO_ACCESS_SOURCE.as_bytes()));
        }
    }

    #[tokio::test]
    async fn test_key_exists_with_pttl_err() {
        let mut err_set1 = HashMap::new();
        err_set1.insert("PTTL", ErrType::ErrorReply);
        let mut err_set2 = HashMap::new();
        err_set2.insert("PTTL", ErrType::ConnError);

        for err_set in &[err_set1, err_set2] {
            let handler = RestoreDataCmdTaskHandler::new(
                DummyCmdTaskSender::new(true, err_set.clone(), 666),
                DummyWaitTaskSender::new(false, HashMap::new(), 666),
                DummyCmdTaskSender::new(false, HashMap::new(), 0),
                Arc::new(CmdCtxFactory::default()),
                Arc::new(MigrationStats::default()),
            );

            let (cmd_ctx, reply_receiver) = gen_test_cmd_ctx(vec!["GET", "somekey"]);

            handler.handle_cmd_task(cmd_ctx).unwrap();
            let s = run_future(&handler, reply_receiver).await;

            assert_eq!(handler.dst_sender.get_cmd_count("EXISTS"), Some(1));
            assert_eq!(handler.dst_sender.get_cmd_count("GET"), None);
            assert_eq!(handler.src_sender.get_cmd_count("DUMP"), Some(1));
            assert_eq!(handler.src_sender.get_cmd_count("PTTL"), Some(1));
            assert_eq!(handler.dst_sender.get_cmd_count("RESTORE"), None);
            assert_eq!(handler.src_sender.get_cmd_count("DEL"), None);
            assert_eq!(handler.dst_sender.get_cmd_count("DEL"), None);

            assert!(s.starts_with(FAILED_TO_ACCESS_SOURCE.as_bytes()));
        }
    }

    #[tokio::test]
    async fn test_key_exists_with_restore_err() {
        let mut err_set1 = HashMap::new();
        err_set1.insert("RESTORE", ErrType::ErrorReply);
        let mut err_set2 = HashMap::new();
        err_set2.insert("RESTORE", ErrType::ConnError);

        for (i, err_set) in [err_set1, err_set2].iter().enumerate() {
            let handler = RestoreDataCmdTaskHandler::new(
                DummyCmdTaskSender::new(true, HashMap::new(), 666),
                DummyWaitTaskSender::new(false, err_set.clone(), 666),
                DummyCmdTaskSender::new(false, HashMap::new(), 0),
                Arc::new(CmdCtxFactory::default()),
                Arc::new(MigrationStats::default()),
            );

            let (cmd_ctx, reply_receiver) = gen_test_cmd_ctx(vec!["GET", "somekey"]);

            handler.handle_cmd_task(cmd_ctx).unwrap();
            let s = run_future(&handler, reply_receiver).await;

            assert_eq!(handler.dst_sender.get_cmd_count("EXISTS"), Some(1));
            assert_eq!(handler.dst_sender.get_cmd_count("GET"), Some(1));
            assert_eq!(handler.src_sender.get_cmd_count("DUMP"), Some(1));
            assert_eq!(handler.src_sender.get_cmd_count("PTTL"), Some(1));
            assert_eq!(handler.dst_sender.get_cmd_count("RESTORE"), Some(1));
            assert_eq!(handler.src_sender.get_cmd_count("DEL"), None);
            assert_eq!(handler.dst_sender.get_cmd_count("DEL"), None);

            if i == 0 {
                // Since the last RESTORE and GET command are sent at the same time,
                // this has to be correct. In reality, this might be error.
                assert_eq!(s, "key_not_exists".as_bytes());
            } else {
                assert!(s.starts_with(b"future_returns_error"));
            }
        }
    }

    #[tokio::test]
    async fn test_key_exists_with_negative_pttl() {
        let handler = RestoreDataCmdTaskHandler::new(
            DummyCmdTaskSender::new(true, HashMap::new(), -1),
            DummyWaitTaskSender::new(false, HashMap::new(), -2),
            DummyCmdTaskSender::new(false, HashMap::new(), 0),
            Arc::new(CmdCtxFactory::default()),
            Arc::new(MigrationStats::default()),
        );

        let (cmd_ctx, reply_receiver) = gen_test_cmd_ctx(vec!["GET", "somekey"]);

        handler.handle_cmd_task(cmd_ctx).unwrap();
        let s = run_future(&handler, reply_receiver).await;

        assert_eq!(handler.dst_sender.get_cmd_count("EXISTS"), Some(1));
        assert_eq!(handler.dst_sender.get_cmd_count("GET"), Some(1));
        assert_eq!(handler.src_sender.get_cmd_count("DUMP"), Some(1));
        assert_eq!(handler.src_sender.get_cmd_count("PTTL"), Some(1));
        assert_eq!(handler.dst_sender.get_cmd_count("RESTORE"), Some(1));
        // This is async
        // assert_eq!(handler.src_sender.get_cmd_count("DEL"), Some(1));
        assert_eq!(handler.dst_sender.get_cmd_count("DEL"), None);

        assert_eq!(s, "get_reply".to_string().into_bytes());
    }

    #[tokio::test]
    async fn test_key_dst_not_exists_for_del() {
        let handler = RestoreDataCmdTaskHandler::new(
            DummyCmdTaskSender::new(true, HashMap::new(), 666),
            DummyWaitTaskSender::new(false, HashMap::new(), 1),
            DummyCmdTaskSender::new(false, HashMap::new(), 0),
            Arc::new(CmdCtxFactory::default()),
            Arc::new(MigrationStats::default()),
        );

        let (cmd_ctx, reply_receiver) = gen_test_cmd_ctx(vec!["DEL", "somekey"]);

        handler.handle_cmd_task(cmd_ctx).unwrap();
        let s = run_future(&handler, reply_receiver).await;

        assert_eq!(handler.dst_sender.get_cmd_count("EXISTS"), None);
        assert_eq!(handler.dst_sender.get_cmd_count("DEL"), Some(1));
        assert_eq!(handler.src_proxy_sender.get_cmd_count("UMSYNC"), Some(1));
        // These should be triggered by migrating task but not in this tests.
        assert_eq!(handler.src_sender.get_cmd_count("DUMP"), None);
        assert_eq!(handler.src_sender.get_cmd_count("PTTL"), None);
        assert_eq!(handler.dst_sender.get_cmd_count("RESTORE"), None);
        assert_eq!(handler.src_sender.get_cmd_count("DEL"), None);

        assert_eq!(s, b"1".to_vec());
    }

    #[test]
    fn test_key_lock() {
        let lock = KeyLock::new(1);
        let some_key = b"some_key".to_vec();
        let another_key = b"another_key".to_vec();
        {
            let _guard = lock.lock(some_key.clone(), 0).unwrap();
            assert!(lock.lock(some_key.clone(), 0).is_none());
            assert!(lock.lock(another_key.clone(), 0).is_some());
        }
        assert!(lock.lock(some_key.clone(), 0).is_some());

        {
            let _guard = lock.lock(some_key.clone(), 233).unwrap();
            assert!(lock.lock(some_key.clone(), 0).is_none());
            assert!(lock.lock(another_key.clone(), 0).is_some());
        }
    }

    async fn assert_ready<F: Future<Output = ()> + Unpin>(f: F) {
        use futures::future::{ready, select, Either};

        match select(f, ready(())).await {
            Either::Left(((), _)) => (),
            Either::Right(((), _)) => assert!(false),
        };
    }

    async fn assert_not_ready<F: Future<Output = ()> + Unpin>(f: F) -> F {
        use futures::future::{ready, select, Either};

        let f_opt = match select(f, ready(())).await {
            Either::Left(((), _)) => None,
            Either::Right(((), f)) => Some(f),
        };
        f_opt.unwrap()
    }

    #[tokio::test]
    async fn test_waitable_task() {
        let (registry, handle) = WaitRegistry::new();
        let handle = handle.map(|_| ());

        let handle = assert_not_ready(handle).await;
        registry.register();
        let handle = assert_not_ready(handle).await;
        registry.unregister();
        let handle = assert_not_ready(handle).await;

        registry.register();
        registry.register();
        let handle = assert_not_ready(handle).await;
        registry.unregister();
        registry.unregister();
        let handle = assert_not_ready(handle).await;

        registry.close();
        assert_ready(handle).await;

        {
            let (registry, handle) = WaitRegistry::new();
            let handle = handle.map(|_| ());
            registry.register();
            registry.close();
            let handle = assert_not_ready(handle).await;
            registry.unregister();
            assert_ready(handle).await;
        }
    }
}
