use std::io;
use std::sync;
use std::clone::Clone;
use futures::{future, Future, stream, Stream, BoxFuture};
use tokio;
use tokio::net::TcpStream;
use super::command::{CmdReplySender, CommandError};
use super::session::{CmdCtxHandler, CmdCtx};
use super::backend::{BackendNode, ReplyHandler, CmdTask, BackendResult, BackendError};
use protocol::{Resp, Array, BulkStr, decode_resp, DecodeError, resp_to_buf};

#[derive(Clone)]
pub struct ForwardHandler {
    addr: sync::Arc<String>,
    node: sync::Arc<sync::RwLock<Option<BackendNode<CmdCtx>>>>,
}

impl ForwardHandler {
    pub fn new() -> ForwardHandler {
        ForwardHandler{
            addr: sync::Arc::new("127.0.0.1:6379".to_string()),
            node: sync::Arc::new(sync::RwLock::new(None)),
        }
    }
}

impl CmdCtxHandler for ForwardHandler {
    fn handle_cmd_ctx(&self, cmd_ctx: CmdCtx) {
//        let backup = cmd_ctx.clone();  TODO: cmd_ctx might get lost
        let needInit = self.node.read().unwrap().is_none();
        // Race condition here. Multiple threads might be creating new connection at the same time.
        // Maybe it's just fine. If not, lock the creating connection phrase.
        if needInit {
            let nodeArc = self.node.clone();
            let addr = self.addr.parse().unwrap();
            let sock = TcpStream::connect(&addr);
            let fut = sock.then(move |res| {
                let fut : BoxFuture<_, BackendError> = match res {
                    Ok(sock) => {
                        let (node, nodeFut) = BackendNode::<CmdCtx>::new_pair(sock, ReplyCommitHandler{});
                        node.send(cmd_ctx).unwrap();  // must not fail
                        nodeArc.write().unwrap().get_or_insert(node);
                        Box::new(nodeFut)
                    },
                    Err(e) => {
                        cmd_ctx.send(Err(CommandError::Io(io::Error::from(e.kind()))));
                        Box::new(future::err(BackendError::Io(e)))
                    },
                };
                fut.map_err(|_| ())
            });
            // If this future fails, cmd_ctx will be lost. Let itself send back an error response.
            tokio::spawn(fut);
            return;
        }
        if let Err(err) = self.node.read().unwrap().as_ref().unwrap().send(cmd_ctx) {
            // if it fails, remove this connection.
            self.node.write().unwrap().take();
        }
    }
}

pub struct ReplyCommitHandler {}

impl<T: CmdTask> ReplyHandler<T> for ReplyCommitHandler {
    fn handle_reply(&self, cmd_task: T, result: BackendResult) {
    }
}
