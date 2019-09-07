use super::backend::CmdTask;
use super::command::DataCmdType;
use super::session::CmdCtx;
use ::protocol::{BulkStr, Resp, RespPacket};
use std::error::Error;
use std::fmt;
use std::io;
use std::sync::Arc;
use zstd;

pub struct CmdCompressor {
    clusters_config: Arc<()>,
}

impl CmdCompressor {
    pub fn new(clusters_config: Arc<()>) -> Self {
        Self { clusters_config }
    }

    pub fn try_compressing_cmd_ctx(&self, cmd_ctx: &mut CmdCtx) -> Result<(), CompressionError> {
        let index = match cmd_ctx.get_data_cmd_type() {
            DataCmdType::GETSET | DataCmdType::SET | DataCmdType::SETNX => 2,
            DataCmdType::PSETEX | DataCmdType::SETEX => 3,
            DataCmdType::APPEND
            | DataCmdType::BITCOUNT
            | DataCmdType::BITFIELD
            | DataCmdType::BITOP
            | DataCmdType::BITPOS
            | DataCmdType::DECR
            | DataCmdType::DECRBY
            | DataCmdType::GETBIT
            | DataCmdType::GETRANGE
            | DataCmdType::INCR
            | DataCmdType::INCRBY
            | DataCmdType::INCRBYFLOAT
            | DataCmdType::MGET
            | DataCmdType::MSET
            | DataCmdType::MSETNX
            | DataCmdType::SETBIT
            | DataCmdType::SETRANGE
            | DataCmdType::STRLEN => return Err(CompressionError::RestrictedCmd),
            _ => return Ok(()),
        };

        let value = match cmd_ctx.get_cmd().get_command_element(index) {
            Some(e) => e,
            None => return Err(CompressionError::InvalidRequest),
        };

        let compressed = match zstd::encode_all(value, 1) {
            Ok(c) => c,
            Err(err) => {
                return Err(CompressionError::Io(err));
            }
        };

        if cmd_ctx.change_cmd_element(index, compressed) {
            Ok(())
        } else {
            Err(CompressionError::InvalidRequest)
        }
    }
}

pub struct CmdReplyDecompressor {
    clusters_config: Arc<()>,
}

impl CmdReplyDecompressor {
    pub fn new(clusters_config: Arc<()>) -> Self {
        Self { clusters_config }
    }

    pub fn decompress(
        &self,
        cmd_ctx: &CmdCtx,
        packet: &mut RespPacket,
    ) -> Result<(), CompressionError> {
        let data_cmd_type = cmd_ctx.get_data_cmd_type();
        match data_cmd_type {
            DataCmdType::GET | DataCmdType::GETSET => {
                let compressed = if let Resp::Bulk(BulkStr::Str(s)) = packet.get_resp() {
                    let compressed = match zstd::decode_all(s.as_slice()) {
                        Ok(c) => c,
                        Err(err) => {
                            return Err(CompressionError::Io(err));
                        }
                    };
                    Some(compressed)
                } else {
                    None
                };
                if let Some(c) = compressed {
                    if !packet.change_bulk_str(c) {
                        return Err(CompressionError::InvalidResp);
                    }
                }
                Ok(())
            }
            _ => Err(CompressionError::UnsupportedCmdType),
        }
    }
}

#[derive(Debug)]
pub enum CompressionError {
    Io(io::Error),
    InvalidRequest,
    InvalidResp,
    UnsupportedCmdType,
    RestrictedCmd,
}

impl fmt::Display for CompressionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for CompressionError {
    fn description(&self) -> &str {
        "compression error"
    }

    fn cause(&self) -> Option<&dyn Error> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}
