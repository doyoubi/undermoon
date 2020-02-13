use super::backend::{BackendResult, CmdTask, CmdTaskResultHandler, CmdTaskResultHandlerFactory};
use super::compress::{CmdReplyDecompressor, CompressionError};
use super::manager::SharedMetaMap;
use super::session::CmdCtx;
use crate::common::utils::ThreadSafe;
use crate::protocol::{BulkStr, Resp};

pub struct DecompressCommitHandlerFactory {
    meta_map: SharedMetaMap,
}

impl ThreadSafe for DecompressCommitHandlerFactory {}

impl DecompressCommitHandlerFactory {
    pub fn new(meta_map: SharedMetaMap) -> Self {
        Self { meta_map }
    }
}

impl CmdTaskResultHandlerFactory for DecompressCommitHandlerFactory {
    type Handler = DecompressCommitHandler;

    fn create(&self) -> Self::Handler {
        DecompressCommitHandler {
            decompressor: CmdReplyDecompressor::new(self.meta_map.clone()),
        }
    }
}

pub struct DecompressCommitHandler {
    decompressor: CmdReplyDecompressor,
}

impl CmdTaskResultHandler for DecompressCommitHandler {
    type Task = CmdCtx;

    fn handle_task(
        &self,
        cmd_ctx: Self::Task,
        result: BackendResult<<Self::Task as CmdTask>::Pkt>,
    ) {
        let mut packet = match result {
            Ok(pkt) => pkt,
            Err(err) => {
                return cmd_ctx.set_resp_result(Ok(Resp::Error(
                    format!("backend failed to handle task: {:?}", err).into_bytes(),
                )));
            }
        };

        match self.decompressor.decompress(&cmd_ctx, &mut packet) {
            Ok(())
            | Err(CompressionError::UnsupportedCmdType)
            | Err(CompressionError::Disabled) => (),
            Err(err) => {
                warn!(
                    "failed to decompress: {:?}. Force to return nil bulk string",
                    err
                );
                return cmd_ctx.set_resp_result(Ok(Resp::Bulk(BulkStr::Nil)));
            }
        }

        cmd_ctx.set_result(Ok(Box::new(packet)))
    }
}

pub struct ReplyCommitHandlerFactory;

impl ThreadSafe for ReplyCommitHandlerFactory {}

impl Default for ReplyCommitHandlerFactory {
    fn default() -> Self {
        Self
    }
}

impl CmdTaskResultHandlerFactory for ReplyCommitHandlerFactory {
    type Handler = ReplyCommitHandler;

    fn create(&self) -> Self::Handler {
        ReplyCommitHandler
    }
}

pub struct ReplyCommitHandler;

impl CmdTaskResultHandler for ReplyCommitHandler {
    type Task = CmdCtx;

    fn handle_task(
        &self,
        cmd_ctx: Self::Task,
        result: BackendResult<<Self::Task as CmdTask>::Pkt>,
    ) {
        let packet = match result {
            Ok(pkt) => pkt,
            Err(err) => {
                return cmd_ctx.set_resp_result(Ok(Resp::Error(
                    format!("backend failed to handle task: {:?}", err).into_bytes(),
                )));
            }
        };
        cmd_ctx.set_result(Ok(Box::new(packet)))
    }
}
