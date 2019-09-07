use super::backend::{BackendResult, CmdTask, CmdTaskHandler, CmdTaskHandlerFactory};
use super::compress::{CmdReplyDecompressor, CompressionError};
use super::manager::SharedMetaMap;
use super::session::CmdCtx;
use ::common::utils::ThreadSafe;
use ::protocol::{BulkStr, Resp};

pub struct ReplyCommitHandlerFactory {
    meta_map: SharedMetaMap,
}

impl ThreadSafe for ReplyCommitHandlerFactory {}

impl ReplyCommitHandlerFactory {
    pub fn new(meta_map: SharedMetaMap) -> Self {
        Self { meta_map }
    }
}

impl CmdTaskHandlerFactory for ReplyCommitHandlerFactory {
    type Handler = ReplyCommitHandler;

    fn create(&self) -> Self::Handler {
        ReplyCommitHandler {
            decompressor: CmdReplyDecompressor::new(self.meta_map.clone()),
        }
    }
}

pub struct ReplyCommitHandler {
    decompressor: CmdReplyDecompressor,
}

impl CmdTaskHandler for ReplyCommitHandler {
    type Task = CmdCtx;

    fn handle_task(&self, cmd_ctx: Self::Task, result: BackendResult) {
        let mut packet = match result {
            Ok(pkt) => pkt,
            Err(err) => {
                return cmd_ctx.set_resp_result(Ok(Resp::Error(
                    format!("backend failed to handle task: {:?}", err).into_bytes(),
                )));
            }
        };

        match self.decompressor.decompress(&cmd_ctx, &mut packet) {
            Ok(()) | Err(CompressionError::UnsupportedCmdType) => (),
            Err(err) => {
                warn!(
                    "failed to decompress: {:?}. Force to return nil bulk string",
                    err
                );
                return cmd_ctx.set_resp_result(Ok(Resp::Bulk(BulkStr::Nil)));
            }
        }

        cmd_ctx.set_result(Ok(packet))
    }
}
