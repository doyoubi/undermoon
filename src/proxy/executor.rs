use std::io;
use std::sync;
use std::clone::Clone;
use std::boxed::Box;
use futures::{future, Future};
use tokio;
use tokio::net::TcpStream;
use super::command::CommandError;
use super::session::{CmdCtxHandler, CmdCtx};
use super::backend::{BackendNode, ReplyHandler, CmdTask, BackendResult, BackendError};

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
        let need_init = self.node.read().unwrap().is_none();
        // Race condition here. Multiple threads might be creating new connection at the same time.
        // Maybe it's just fine. If not, lock the creating connection phrase.
        if need_init {
            let node_arc = self.node.clone();
            let node_arc2 = self.node.clone();
            let addr = self.addr.parse().unwrap();
            let sock = TcpStream::connect(&addr);
            let fut = sock.then(move |res| {
                let fut : Box<Future<Item=_, Error=BackendError> + Send> = match res {
                    Ok(sock) => {
                        let (node, node_fut) = BackendNode::<CmdCtx>::new_pair(sock, ReplyCommitHandler{});
                        node.send(cmd_ctx).unwrap();  // must not fail
                        node_arc.write().unwrap().get_or_insert(node);
                        Box::new(node_fut)
                    },
                    Err(e) => {
                        let res = cmd_ctx.send(Err(CommandError::Io(io::Error::from(e.kind()))));
                        if let Err(e) = res {
                            println!("Failed to send back error: {:?}", e)
                        }
                        Box::new(future::err(BackendError::Io(e)))
                    },
                };
                fut.then(move |r| {
                    println!("backend exited with result {:?}", r);
                    node_arc2.write().unwrap().take();
                    future::ok(())
                })
            });
            // If this future fails, cmd_ctx will be lost. Let itself send back an error response.
            tokio::spawn(fut);
            return;
        }
        let res = self.node.read().unwrap().as_ref().unwrap().send(cmd_ctx);
        if let Err(e) = res {
            // if it fails, remove this connection.
            self.node.write().unwrap().take();
            println!("reset backend connecton {:?}", e)
        }
    }
}

// TODO: Remove this. Retry can be built with a CmdTask wrapper
pub struct ReplyCommitHandler {}

impl<T: CmdTask> ReplyHandler<T> for ReplyCommitHandler {
    fn handle_reply(&self, _cmd_task: T, _result: BackendResult) {
    }
}
