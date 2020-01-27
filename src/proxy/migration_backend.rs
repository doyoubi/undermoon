use super::backend::{BackendSenderFactory, CmdTask, CmdTaskFactory, ReqTask, ReqTaskSender};
use super::command::CommandError;
use super::reply::ReplyCommitHandlerFactory;
use ::protocol::RespVec;
use atomic_option::AtomicOption;
use common::utils::ThreadSafe;
use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::{future, Future, Stream};
use protocol::{Array, BinSafeStr, BulkStr, Resp};
use std::sync::atomic::Ordering;
use std::sync::Arc;

const KEY_NOT_EXISTS: &str = "0";

type ReplyFuture = Box<dyn Future<Item = RespVec, Error = CommandError> + Send>;
type DataEntryFuture = Box<dyn Future<Item = Option<DataEntry>, Error = CommandError> + Send>;

#[derive(Debug)]
struct MgrCmdStateExists<F: CmdTaskFactory> {
    inner_task: F::Task,
    key: BinSafeStr,
}

impl<F: CmdTaskFactory> MgrCmdStateExists<F> {
    fn from_task(
        inner_task: F::Task,
        key: BinSafeStr,
        cmd_task_factory: &F,
    ) -> (Self, ReqTask<F::Task>, ReplyFuture) {
        let resp = Self::gen_exists_resp(&key);
        let (cmd_task, reply_fut) = cmd_task_factory.create_with(&inner_task, resp);
        let task = ReqTask::Simple(cmd_task);
        let state = Self { inner_task, key };
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

#[derive(Debug)]
struct MgrCmdStateForward;

impl MgrCmdStateForward {
    fn from_state_exists<F: CmdTaskFactory>(
        state: MgrCmdStateExists<F>,
    ) -> (Self, ReqTask<F::Task>) {
        let inner_task = state.into_inner();
        (MgrCmdStateForward, ReqTask::Simple(inner_task))
    }

    fn from_state_dump_pttl<F: CmdTaskFactory>(
        state: MgrCmdStateDumpPttl<F>,
    ) -> (Self, ReqTask<F::Task>) {
        let inner_task = state.into_inner();
        (MgrCmdStateForward, ReqTask::Simple(inner_task))
    }
}

#[derive(Debug)]
struct MgrCmdStateDumpPttl<F: CmdTaskFactory> {
    inner_task: F::Task,
    key: BinSafeStr,
}

impl<F: CmdTaskFactory> MgrCmdStateDumpPttl<F> {
    fn from_state_exists(
        state: MgrCmdStateExists<F>,
        cmd_task_factory: &F,
    ) -> (Self, ReqTask<F::Task>, DataEntryFuture) {
        let MgrCmdStateExists { inner_task, key } = state;

        let (dump_cmd_task, dump_reply_fut) =
            cmd_task_factory.create_with(&inner_task, Self::gen_dump_resp(&key));
        let (pttl_cmd_task, pttl_reply_fut) =
            cmd_task_factory.create_with(&inner_task, Self::gen_pttl_resp(&key));

        let task = ReqTask::Multi(vec![dump_cmd_task, pttl_cmd_task]);
        let state = Self { inner_task, key };
        let entry_fut = data_entry_future(dump_reply_fut, pttl_reply_fut);

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

fn data_entry_future(dump: ReplyFuture, pttl: ReplyFuture) -> DataEntryFuture {
    let fut = dump.join(pttl).and_then(|(dump, pttl)| {
        let dump_result = match dump {
            Resp::Bulk(BulkStr::Str(raw_data)) => Ok(Some(raw_data)),
            Resp::Bulk(BulkStr::Nil) => Ok(None),
            _others => Err(CommandError::UnexpectedResponse),
        };
        let pttl_result = match pttl {
            // -2 for key not exists
            Resp::Integer(pttl) if pttl.as_slice() != "-2".as_bytes() => Ok(Some(pttl)),
            Resp::Integer(_pttl) => Ok(None),
            _others => Err(CommandError::UnexpectedResponse),
        };
        match (dump_result, pttl_result) {
            (Ok(Some(raw_data)), Ok(Some(pttl))) => future::ok(Some(DataEntry { raw_data, pttl })),
            (Ok(None), _) | (_, Ok(None)) => future::ok(None),
            (Err(err), _) => future::err(err),
            (_, Err(err)) => future::err(err),
        }
    });
    Box::new(fut)
}

#[derive(Debug)]
struct MgrCmdStateRestoreForward;

impl MgrCmdStateRestoreForward {
    fn from_state_exists<F: CmdTaskFactory>(
        state: MgrCmdStateDumpPttl<F>,
        entry: DataEntry,
        cmd_task_factory: &F,
    ) -> (Self, ReqTask<F::Task>, ReplyFuture) {
        let MgrCmdStateDumpPttl { inner_task, key } = state;
        let DataEntry { raw_data, pttl } = entry;
        let resp = Self::gen_restore_resp(&key, raw_data, pttl);
        let (restore_cmd_task, restore_reply_fut) = cmd_task_factory.create_with(&inner_task, resp);

        let task = ReqTask::Multi(vec![restore_cmd_task, inner_task]);
        (MgrCmdStateRestoreForward, task, restore_reply_fut)
    }

    fn gen_restore_resp(key: &[u8], raw_data: BinSafeStr, pttl: BinSafeStr) -> RespVec {
        let elements = vec![
            Resp::Bulk(BulkStr::Str("RESTORE".to_string().into_bytes())),
            Resp::Bulk(BulkStr::Str(key.into())),
            Resp::Bulk(BulkStr::Str(pttl)),
            Resp::Bulk(BulkStr::Str(raw_data)),
        ];
        Resp::Arr(Array::Arr(elements))
    }
}

pub type SenderFactory = BackendSenderFactory<ReplyCommitHandlerFactory>;

type ExistsTaskSender<F> = UnboundedSender<(MgrCmdStateExists<F>, ReplyFuture)>;
type ExistsTaskReceiver<F> = UnboundedReceiver<(MgrCmdStateExists<F>, ReplyFuture)>;
type DumpPttlTaskSender<F> = UnboundedSender<(MgrCmdStateDumpPttl<F>, DataEntryFuture)>;
type DumpPttlTaskReceiver<F> = UnboundedReceiver<(MgrCmdStateDumpPttl<F>, DataEntryFuture)>;

pub struct RestoreDataCmdTaskHandler<F: CmdTaskFactory, S: ReqTaskSender<Task = F::Task>> {
    src_sender: Arc<S>,
    dst_sender: Arc<S>,
    exists_task_sender: ExistsTaskSender<F>,
    dump_pttl_task_sender: DumpPttlTaskSender<F>,
    task_receivers: AtomicOption<(ExistsTaskReceiver<F>, DumpPttlTaskReceiver<F>)>,
    cmd_task_factory: Arc<F>,
}

impl<F: CmdTaskFactory + ThreadSafe, S: ReqTaskSender<Task = F::Task> + ThreadSafe> ThreadSafe
    for RestoreDataCmdTaskHandler<F, S>
{
}

impl<F: CmdTaskFactory, S: ReqTaskSender<Task = F::Task>> RestoreDataCmdTaskHandler<F, S> {
    pub fn new(src_sender: S, dst_sender: S, cmd_task_factory: Arc<F>) -> Self {
        let src_sender = Arc::new(src_sender);
        let dst_sender = Arc::new(dst_sender);
        let (exists_task_sender, exists_task_receiver) = unbounded();
        let (dump_pttl_task_sender, dump_pttl_task_receiver) = unbounded();
        let task_receivers =
            AtomicOption::new(Box::new((exists_task_receiver, dump_pttl_task_receiver)));
        Self {
            src_sender,
            dst_sender,
            exists_task_sender,
            dump_pttl_task_sender,
            task_receivers,
            cmd_task_factory,
        }
    }

    pub fn run_task_handler(&self) -> impl Future<Item = (), Error = ()> {
        let dump_pttl_task_sender = self.dump_pttl_task_sender.clone();
        let src_sender = self.src_sender.clone();
        let dst_sender = self.dst_sender.clone();
        let cmd_task_factory = self.cmd_task_factory.clone();
        let r = self
            .task_receivers
            .take(Ordering::SeqCst)
            .map(|p| *p)
            .ok_or_else(|| ());

        if r.is_err() {
            error!("Failed to run importing task handler. It's already run.");
        }

        future::result(r).and_then(move |(exists_task_receiver, dump_pttl_task_receiver)| {
            let exists_task_handler = Self::handle_exists_task(
                exists_task_receiver,
                dump_pttl_task_sender,
                src_sender,
                dst_sender.clone(),
                cmd_task_factory.clone(),
            );
            let dump_pttl_task_handler =
                Self::handle_dump_pttl_task(dump_pttl_task_receiver, dst_sender, cmd_task_factory);
            exists_task_handler
                .select(dump_pttl_task_handler)
                .map(|_| ())
                .map_err(|_| ())
        })
    }

    fn handle_exists_task(
        exists_task_receiver: ExistsTaskReceiver<F>,
        dump_pttl_task_sender: DumpPttlTaskSender<F>,
        src_sender: Arc<S>,
        dst_sender: Arc<S>,
        cmd_task_factory: Arc<F>,
    ) -> impl Future<Item = (), Error = ()> {
        exists_task_receiver
            .and_then(|(state, reply_receiver)| {
                reply_receiver
                    .and_then(move |resp| {
                        let r = match resp {
                            Resp::Integer(num) => {
                                Ok((num.as_slice() != KEY_NOT_EXISTS.as_bytes(), state))
                            }
                            _others => Err(CommandError::UnexpectedResponse),
                        };
                        future::result(r)
                    })
                    .then(|res| match res {
                        Ok(item) => future::ok(Some(item)),
                        Err(err) => {
                            debug!("failed to get exists cmd response: {:?}", err);
                            future::ok(None)
                        }
                    })
            })
            .for_each(move |item| {
                match item {
                    Some((true, state)) => {
                        let (_state, req_task) = MgrCmdStateForward::from_state_exists(state);
                        if let Err(err) = dst_sender.send(req_task) {
                            debug!("failed to forward: {:?}", err);
                        }
                    }
                    Some((false, state)) => {
                        let (state, req_task, reply_fut) =
                            MgrCmdStateDumpPttl::from_state_exists(state, &(*cmd_task_factory));
                        if let Err(err) = src_sender.send(req_task) {
                            debug!("failed to send dump pttl: {:?}", err);
                        }
                        if let Err(_err) = dump_pttl_task_sender.unbounded_send((state, reply_fut))
                        {
                            debug!("dump_pttl_task_sender is canceled");
                        }
                    }
                    None => (),
                }
                future::ok(())
            })
    }

    fn handle_dump_pttl_task(
        dump_pttl_task_receiver: DumpPttlTaskReceiver<F>,
        dst_sender: Arc<S>,
        cmd_task_factory: Arc<F>,
    ) -> impl Future<Item = (), Error = ()> {
        let dst_sender_clone = dst_sender.clone();
        dump_pttl_task_receiver
            .and_then(move |(state, reply_fut)| {
                let dst_sender = dst_sender_clone.clone();
                reply_fut.then(move |res| match res {
                    Ok(Some(entry)) => future::ok(Some((entry, state))),
                    Ok(None) => {
                        let (_state, req_task) = MgrCmdStateForward::from_state_dump_pttl(state);
                        if let Err(err) = dst_sender.send(req_task) {
                            debug!("failed to send forward: {:?}", err);
                        }
                        future::ok(None)
                    }
                    Err(err) => {
                        debug!("failed to get exists cmd response: {:?}", err);
                        future::ok(None)
                    }
                })
            })
            .for_each(move |item| {
                let fut: Box<dyn Future<Item = (), Error = ()> + Send> =
                    if let Some((entry, state)) = item {
                        let (_state, req_task, reply_receiver) =
                            MgrCmdStateRestoreForward::from_state_exists(
                                state,
                                entry,
                                &(*cmd_task_factory),
                            );
                        if let Err(err) = dst_sender.send(req_task) {
                            debug!("failed to send restore and forward: {:?}", err);
                        }
                        Box::new(reply_receiver.map(|_| ()).map_err(|_| ()))
                    } else {
                        Box::new(future::ok(()))
                    };
                fut
            })
    }

    pub fn handle_cmd_task(&self, cmd_task: F::Task) {
        let key = match cmd_task.get_key() {
            Some(key) => key.to_vec(),
            None => {
                cmd_task.set_resp_result(Ok(Resp::Error(
                    String::from("Missing key while migrating").into_bytes(),
                )));
                return;
            }
        };

        let (state, task, reply_fut) =
            MgrCmdStateExists::from_task(cmd_task, key, &(*self.cmd_task_factory));
        if let Err(err) = self.dst_sender.send(task) {
            let cmd_task: F::Task = state.into_inner();
            cmd_task.set_resp_result(Ok(Resp::Error(
                format!("Migration backend error: {:?}", err).into_bytes(),
            )));
            return;
        }

        if let Err(err) = self.exists_task_sender.unbounded_send((state, reply_fut)) {
            let (state, _) = err.into_inner();
            let cmd_task: F::Task = state.into_inner();
            cmd_task.set_resp_result(Ok(Resp::Error(
                String::from("Migration backend canceled").into_bytes(),
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::backend::BackendError;
    use super::super::command::{new_command_pair, CmdReplyReceiver, Command};
    use super::super::session::{CmdCtx, CmdCtxFactory};
    use super::*;
    use crate::protocol::RespPacket;
    use chashmap::CHashMap;
    use futures::{future, Future};
    use protocol::{BulkStr, Resp};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::RwLock;

    #[derive(Debug, Clone, Copy)]
    enum ErrType {
        ErrorReply,
        ConnError,
    }

    struct DummyReqTaskSender {
        exists: AtomicBool,
        closed: AtomicBool,
        cmd_count: CHashMap<String, AtomicUsize>,
        err_set: HashMap<&'static str, ErrType>,
    }

    impl DummyReqTaskSender {
        fn new(exists: bool, err_set: HashMap<&'static str, ErrType>) -> Self {
            Self {
                exists: AtomicBool::new(exists),
                closed: AtomicBool::new(false),
                cmd_count: CHashMap::new(),
                err_set,
            }
        }

        fn handle(&self, cmd_ctx: CmdCtx) {
            let cmd_name = cmd_ctx
                .get_cmd()
                .get_command_name()
                .expect("DummyReqTaskSender::handle")
                .to_string()
                .to_uppercase();

            let cmd = cmd_name.as_str();
            self.cmd_count
                .upsert(cmd_name.clone(), || AtomicUsize::new(0), |_| ());
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
                    cmd_ctx.set_resp_result(Ok(Resp::Integer("6666".to_string().into_bytes())));
                }
                "RESTORE" => {
                    self.exists.store(true, Ordering::SeqCst);
                    cmd_ctx.set_resp_result(Ok(Resp::Simple("OK".to_string().into_bytes())));
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

    impl ReqTaskSender for DummyReqTaskSender {
        type Task = CmdCtx;

        fn send(&self, cmd_task: ReqTask<Self::Task>) -> Result<(), BackendError> {
            match cmd_task {
                ReqTask::Simple(cmd_ctx) => self.handle(cmd_ctx),
                ReqTask::Multi(cmds) => {
                    for cmd in cmds.into_iter() {
                        self.handle(cmd);
                    }
                }
            }
            Ok(())
        }
    }

    fn gen_test_cmd_ctx() -> (CmdCtx, CmdReplyReceiver) {
        let resp = Resp::Arr(Array::Arr(vec![
            Resp::Bulk(BulkStr::Str("GET".to_string().into())),
            Resp::Bulk(BulkStr::Str("somekey".to_string().into())),
        ]));
        let db = Arc::new(RwLock::new("mydb".to_string()));
        let packet = Box::new(RespPacket::from_resp_vec(resp));
        let (reply_sender, reply_receiver) = new_command_pair(Command::new(packet));
        let cmd_ctx = CmdCtx::new(db, reply_sender, 0);
        (cmd_ctx, reply_receiver)
    }

    fn gen_reply_future(
        reply_receiver: CmdReplyReceiver,
    ) -> impl Future<Item = BinSafeStr, Error = ()> {
        reply_receiver
            .wait_response()
            .map_err(|err| error!("cmd err: {:?}", err))
            .and_then(|task_reply| {
                let (packet, _) = task_reply.into_inner();
                let s = match packet.to_resp_slice() {
                    Resp::Bulk(BulkStr::Str(s)) => s.to_vec(),
                    Resp::Bulk(BulkStr::Nil) => "key_not_exists".to_string().into_bytes(),
                    Resp::Error(err_str) => err_str.to_vec(),
                    others => format!("invalid_reply {:?}", others).into_bytes(),
                };
                future::ok(s)
            })
    }

    fn run_future(
        handler: &RestoreDataCmdTaskHandler<CmdCtxFactory, DummyReqTaskSender>,
        reply_receiver: CmdReplyReceiver,
    ) -> BinSafeStr {
        let reply = gen_reply_future(reply_receiver);
        let run = handler
            .run_task_handler()
            .map(|()| "handler_end_first".to_string().into_bytes());
        reply
            .select(run)
            .map(|(s, _next)| s)
            .wait()
            .unwrap_or_else(|(err, _next)| format!("future_returns_error: {:?}", err).into_bytes())
    }

    #[test]
    fn test_key_exists() {
        let handler = RestoreDataCmdTaskHandler::new(
            DummyReqTaskSender::new(false, HashMap::new()),
            DummyReqTaskSender::new(true, HashMap::new()),
            Arc::new(CmdCtxFactory::default()),
        );

        let (cmd_ctx, reply_receiver) = gen_test_cmd_ctx();

        handler.handle_cmd_task(cmd_ctx);
        let s = run_future(&handler, reply_receiver);

        assert_eq!(handler.dst_sender.get_cmd_count("EXISTS"), Some(1));
        assert_eq!(handler.dst_sender.get_cmd_count("GET"), Some(1));
        assert_eq!(handler.src_sender.get_cmd_count("DUMP"), None);
        assert_eq!(handler.src_sender.get_cmd_count("PTTL"), None);
        assert_eq!(handler.dst_sender.get_cmd_count("RESTORE"), None);

        assert_eq!(s, "get_reply".to_string().into_bytes());
    }

    #[test]
    fn test_key_dst_not_exists() {
        let handler = RestoreDataCmdTaskHandler::new(
            DummyReqTaskSender::new(true, HashMap::new()),
            DummyReqTaskSender::new(false, HashMap::new()),
            Arc::new(CmdCtxFactory::default()),
        );

        let (cmd_ctx, reply_receiver) = gen_test_cmd_ctx();

        handler.handle_cmd_task(cmd_ctx);
        let s = run_future(&handler, reply_receiver);

        assert_eq!(handler.dst_sender.get_cmd_count("EXISTS"), Some(1));
        assert_eq!(handler.dst_sender.get_cmd_count("GET"), Some(1));
        assert_eq!(handler.src_sender.get_cmd_count("DUMP"), Some(1));
        assert_eq!(handler.src_sender.get_cmd_count("PTTL"), Some(1));
        assert_eq!(handler.dst_sender.get_cmd_count("RESTORE"), Some(1));

        assert_eq!(s, "get_reply".to_string().into_bytes());
    }

    #[test]
    fn test_key_both_not_exists() {
        let handler = RestoreDataCmdTaskHandler::new(
            DummyReqTaskSender::new(false, HashMap::new()),
            DummyReqTaskSender::new(false, HashMap::new()),
            Arc::new(CmdCtxFactory::default()),
        );

        let (cmd_ctx, reply_receiver) = gen_test_cmd_ctx();

        handler.handle_cmd_task(cmd_ctx);
        let s = run_future(&handler, reply_receiver);

        assert_eq!(handler.dst_sender.get_cmd_count("EXISTS"), Some(1));
        assert_eq!(handler.dst_sender.get_cmd_count("GET"), Some(1));
        assert_eq!(handler.src_sender.get_cmd_count("DUMP"), Some(1));
        assert_eq!(handler.src_sender.get_cmd_count("PTTL"), Some(1));
        assert_eq!(handler.dst_sender.get_cmd_count("RESTORE"), None);

        assert_eq!(s, "key_not_exists".to_string().into_bytes());
    }

    #[test]
    fn test_key_exists_with_exists_err() {
        let mut err_set1 = HashMap::new();
        err_set1.insert("EXISTS", ErrType::ErrorReply);
        let mut err_set2 = HashMap::new();
        err_set2.insert("EXISTS", ErrType::ConnError);

        for err_set in &[err_set1, err_set2] {
            let handler = RestoreDataCmdTaskHandler::new(
                DummyReqTaskSender::new(false, HashMap::new()),
                DummyReqTaskSender::new(true, err_set.clone()),
                Arc::new(CmdCtxFactory::default()),
            );

            let (cmd_ctx, reply_receiver) = gen_test_cmd_ctx();

            handler.handle_cmd_task(cmd_ctx);
            let s = run_future(&handler, reply_receiver);

            assert_eq!(handler.dst_sender.get_cmd_count("EXISTS"), Some(1));
            assert_eq!(handler.dst_sender.get_cmd_count("GET"), None);
            assert_eq!(handler.src_sender.get_cmd_count("DUMP"), None);
            assert_eq!(handler.src_sender.get_cmd_count("PTTL"), None);
            assert_eq!(handler.dst_sender.get_cmd_count("RESTORE"), None);

            assert!(s.starts_with("future_returns_error".as_bytes()));
        }
    }

    #[test]
    fn test_key_exists_with_get_err() {
        let mut err_set1 = HashMap::new();
        err_set1.insert("GET", ErrType::ErrorReply);
        let mut err_set2 = HashMap::new();
        err_set2.insert("GET", ErrType::ConnError);

        for (i, err_set) in [err_set1, err_set2].iter().enumerate() {
            let handler = RestoreDataCmdTaskHandler::new(
                DummyReqTaskSender::new(false, HashMap::new()),
                DummyReqTaskSender::new(true, err_set.clone()),
                Arc::new(CmdCtxFactory::default()),
            );

            let (cmd_ctx, reply_receiver) = gen_test_cmd_ctx();

            handler.handle_cmd_task(cmd_ctx);
            let s = run_future(&handler, reply_receiver);

            assert_eq!(handler.dst_sender.get_cmd_count("EXISTS"), Some(1));
            assert_eq!(handler.dst_sender.get_cmd_count("GET"), Some(1));
            assert_eq!(handler.src_sender.get_cmd_count("DUMP"), None);
            assert_eq!(handler.src_sender.get_cmd_count("PTTL"), None);
            assert_eq!(handler.dst_sender.get_cmd_count("RESTORE"), None);

            if i == 0 {
                assert_eq!(s, "GET cmd error".as_bytes());
            } else {
                assert!(s.starts_with("future_returns_error".as_bytes()));
            }
        }
    }

    #[test]
    fn test_key_exists_with_dump_err() {
        let mut err_set1 = HashMap::new();
        err_set1.insert("DUMP", ErrType::ErrorReply);
        let mut err_set2 = HashMap::new();
        err_set2.insert("DUMP", ErrType::ConnError);

        for err_set in &[err_set1, err_set2] {
            let handler = RestoreDataCmdTaskHandler::new(
                DummyReqTaskSender::new(true, err_set.clone()),
                DummyReqTaskSender::new(false, HashMap::new()),
                Arc::new(CmdCtxFactory::default()),
            );

            let (cmd_ctx, reply_receiver) = gen_test_cmd_ctx();

            handler.handle_cmd_task(cmd_ctx);
            let s = run_future(&handler, reply_receiver);

            assert_eq!(handler.dst_sender.get_cmd_count("EXISTS"), Some(1));
            assert_eq!(handler.dst_sender.get_cmd_count("GET"), None);
            assert_eq!(handler.src_sender.get_cmd_count("DUMP"), Some(1));
            assert_eq!(handler.src_sender.get_cmd_count("PTTL"), Some(1));
            assert_eq!(handler.dst_sender.get_cmd_count("RESTORE"), None);

            assert!(s.starts_with("future_returns_error".as_bytes()));
        }
    }

    #[test]
    fn test_key_exists_with_pttl_err() {
        let mut err_set1 = HashMap::new();
        err_set1.insert("PTTL", ErrType::ErrorReply);
        let mut err_set2 = HashMap::new();
        err_set2.insert("PTTL", ErrType::ConnError);

        for err_set in &[err_set1, err_set2] {
            let handler = RestoreDataCmdTaskHandler::new(
                DummyReqTaskSender::new(true, err_set.clone()),
                DummyReqTaskSender::new(false, HashMap::new()),
                Arc::new(CmdCtxFactory::default()),
            );

            let (cmd_ctx, reply_receiver) = gen_test_cmd_ctx();

            handler.handle_cmd_task(cmd_ctx);
            let s = run_future(&handler, reply_receiver);

            assert_eq!(handler.dst_sender.get_cmd_count("EXISTS"), Some(1));
            assert_eq!(handler.dst_sender.get_cmd_count("GET"), None);
            assert_eq!(handler.src_sender.get_cmd_count("DUMP"), Some(1));
            assert_eq!(handler.src_sender.get_cmd_count("PTTL"), Some(1));
            assert_eq!(handler.dst_sender.get_cmd_count("RESTORE"), None);

            assert!(s.starts_with("future_returns_error".as_bytes()));
        }
    }

    #[test]
    fn test_key_exists_with_restore_err() {
        let mut err_set1 = HashMap::new();
        err_set1.insert("RESTORE", ErrType::ErrorReply);
        let mut err_set2 = HashMap::new();
        err_set2.insert("RESTORE", ErrType::ConnError);

        for (i, err_set) in [err_set1, err_set2].iter().enumerate() {
            let handler = RestoreDataCmdTaskHandler::new(
                DummyReqTaskSender::new(true, HashMap::new()),
                DummyReqTaskSender::new(false, err_set.clone()),
                Arc::new(CmdCtxFactory::default()),
            );

            let (cmd_ctx, reply_receiver) = gen_test_cmd_ctx();

            handler.handle_cmd_task(cmd_ctx);
            let s = run_future(&handler, reply_receiver);

            assert_eq!(handler.dst_sender.get_cmd_count("EXISTS"), Some(1));
            assert_eq!(handler.dst_sender.get_cmd_count("GET"), Some(1));
            assert_eq!(handler.src_sender.get_cmd_count("DUMP"), Some(1));
            assert_eq!(handler.src_sender.get_cmd_count("PTTL"), Some(1));
            assert_eq!(handler.dst_sender.get_cmd_count("RESTORE"), Some(1));

            if i == 0 {
                // Since the last RESTORE and GET command are sent at the same time,
                // this has to be correct. In reality, this might be error.
                assert_eq!(s, "key_not_exists".as_bytes());
            } else {
                assert!(s.starts_with("future_returns_error".as_bytes()));
            }
        }
    }
}
