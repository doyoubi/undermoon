use super::backend::{BackendResult, CmdTask, CmdTaskHandler, CmdTaskHandlerFactory};
use super::compress::{CmdReplyDecompressor, CompressionError};
use super::session::CmdCtx;
use ::common::utils::ThreadSafe;
use ::protocol::{BulkStr, Resp};
use std::sync::Arc;

pub struct ReplyCommitHandlerFactory {
    clusters_config: Arc<()>,
}

impl ThreadSafe for ReplyCommitHandlerFactory {}

impl ReplyCommitHandlerFactory {
    pub fn new(clusters_config: Arc<()>) -> Self {
        Self { clusters_config }
    }
}

impl CmdTaskHandlerFactory for ReplyCommitHandlerFactory {
    type Handler = ReplyCommitHandler;

    fn create(&self) -> Self::Handler {
        ReplyCommitHandler {
            decompressor: CmdReplyDecompressor::new(self.clusters_config.clone()),
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
