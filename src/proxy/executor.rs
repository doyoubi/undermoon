use super::session::{CmdCtxHandler, CmdCtx};
use super::backend::RecoverableBackendNode;
use super::database::DatabaseMap;

pub struct ForwardHandler {
    task_sender: DatabaseMap<RecoverableBackendNode<CmdCtx>>,
}

impl ForwardHandler {
    pub fn new() -> ForwardHandler {
        let db = DatabaseMap::new();
        ForwardHandler{
            task_sender: db,
        }
    }
}

impl CmdCtxHandler for ForwardHandler {
    fn handle_cmd_ctx(&self, cmd_ctx: CmdCtx) {
        let res = self.task_sender.send(cmd_ctx);
        if let Err(e) = res {
            println!("Failed to foward cmd_ctx: {:?}", e)
        }
    }
}
