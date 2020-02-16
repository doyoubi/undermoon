use super::backend::{BackendResult, CmdTask, CmdTaskResultHandler, CmdTaskResultHandlerFactory};
use super::compress::{CmdReplyDecompressor, CompressionError};
use super::manager::SharedMetaMap;
use super::session::CmdCtx;
use crate::common::utils::{ThreadSafe, Wrapper};
use crate::protocol::{BulkStr, Resp, RespPacket};
use std::marker::PhantomData;

pub struct DecompressCommitHandlerFactory<T: CmdTask<Pkt = RespPacket> + Into<Wrapper<CmdCtx>>> {
    meta_map: SharedMetaMap,
    phanthom: PhantomData<T>,
}

impl<T: CmdTask<Pkt = RespPacket> + Into<Wrapper<CmdCtx>>> ThreadSafe
    for DecompressCommitHandlerFactory<T>
{
}

impl<T: CmdTask<Pkt = RespPacket> + Into<Wrapper<CmdCtx>>> DecompressCommitHandlerFactory<T> {
    pub fn new(meta_map: SharedMetaMap) -> Self {
        Self {
            meta_map,
            phanthom: PhantomData,
        }
    }
}

impl<T: CmdTask<Pkt = RespPacket> + Into<Wrapper<CmdCtx>>> CmdTaskResultHandlerFactory
    for DecompressCommitHandlerFactory<T>
{
    type Handler = DecompressCommitHandler<T>;

    fn create(&self) -> Self::Handler {
        DecompressCommitHandler {
            decompressor: CmdReplyDecompressor::new(self.meta_map.clone()),
            phanthom: PhantomData,
        }
    }
}

pub struct DecompressCommitHandler<T: CmdTask<Pkt = RespPacket> + Into<Wrapper<CmdCtx>>> {
    decompressor: CmdReplyDecompressor,
    phanthom: PhantomData<T>,
}

impl<T: CmdTask<Pkt = RespPacket> + Into<Wrapper<CmdCtx>>> CmdTaskResultHandler
    for DecompressCommitHandler<T>
{
    type Task = T;

    fn handle_task(
        &self,
        cmd_task: Self::Task,
        result: BackendResult<<Self::Task as CmdTask>::Pkt>,
    ) {
        let cmd_ctx = cmd_task.into().into_inner();

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
