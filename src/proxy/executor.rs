use super::session::{CmdCtxHandler, CmdCtx};
use super::backend::{RecoverableBackendNode, CmdTaskSender};

pub struct ForwardHandler {
    node: RecoverableBackendNode<CmdCtx>,
}

impl ForwardHandler {
    pub fn new() -> ForwardHandler {
        ForwardHandler{
            node: RecoverableBackendNode::new("127.0.0.1:6379".to_string()),
        }
    }
}

impl CmdCtxHandler for ForwardHandler {
    fn handle_cmd_ctx(&self, cmd_ctx: CmdCtx) {
        let res = self.node.send(cmd_ctx);
        if let Err(e) = res {
            println!("Failed to foward cmd_ctx: {:?}", e)
        }
    }
}
