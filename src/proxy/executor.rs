use super::command::CmdReplySender;
use super::session::{CmdCtxHandler, CmdCtx};
use protocol::{Resp, Array, BulkStr, decode_resp, DecodeError, resp_to_buf};

pub struct ForwardHandler {
    addr: String,
}

impl ForwardHandler {
    pub fn new() -> ForwardHandler {
        ForwardHandler{
            addr: "127.0.0.1:6379".to_string(),
        }
    }
}

impl CmdCtxHandler for ForwardHandler {
    fn handle_cmd_ctx(&mut self, cmd_ctx: CmdCtx) {
        let reply = Resp::Bulk(BulkStr::Str(String::from("done").into_bytes()));
        cmd_ctx.send(Ok(reply)).unwrap();
    }
}