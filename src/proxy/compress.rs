use super::command::DataCmdType;
use super::manager::SharedMetaMap;
use super::session::CmdCtx;
use crate::common::cluster::ClusterName;
use crate::common::config::{ClusterConfig, CompressionStrategy};
use crate::protocol::RespPacket;
use crate::protocol::{BulkStr, Resp};
use crate::proxy::cluster::ClusterTag;
use std::error::Error;
use std::fmt;
use std::io;
use zstd;

pub struct CmdCompressor {
    meta_map: SharedMetaMap,
}

impl CmdCompressor {
    pub fn new(meta_map: SharedMetaMap) -> Self {
        Self { meta_map }
    }

    pub fn try_compressing_cmd_ctx(&self, cmd_ctx: &mut CmdCtx) -> Result<(), CompressionError> {
        let strategy = get_strategy(&cmd_ctx.get_cluster_name(), &self.meta_map);

        if strategy == CompressionStrategy::Disabled {
            return Err(CompressionError::Disabled);
        }

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
            | DataCmdType::STRLEN => match strategy {
                CompressionStrategy::SetGetOnly => return Err(CompressionError::RestrictedCmd),
                _ => return Err(CompressionError::UnsupportedCmdType),
            },
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
    meta_map: SharedMetaMap,
}

impl CmdReplyDecompressor {
    pub fn new(meta_map: SharedMetaMap) -> Self {
        Self { meta_map }
    }

    pub fn decompress(
        &self,
        cmd_ctx: &CmdCtx,
        packet: &mut RespPacket,
    ) -> Result<(), CompressionError> {
        let strategy = get_strategy(&cmd_ctx.get_cluster_name(), &self.meta_map);

        if strategy == CompressionStrategy::Disabled {
            return Err(CompressionError::Disabled);
        }

        let data_cmd_type = cmd_ctx.get_data_cmd_type();
        match data_cmd_type {
            DataCmdType::GET | DataCmdType::GETSET => {
                let compressed = if let Resp::Bulk(BulkStr::Str(s)) = packet.to_resp_slice() {
                    let compressed = match zstd::decode_all(s) {
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

fn get_strategy(cluster_name: &ClusterName, meta_map: &SharedMetaMap) -> CompressionStrategy {
    let meta_map = meta_map.lease();
    match meta_map.get_db_map().get_config(&cluster_name) {
        Some(config) => config.compression_strategy,
        None => ClusterConfig::default().compression_strategy,
    }
}

#[derive(Debug)]
pub enum CompressionError {
    Io(io::Error),
    InvalidRequest,
    InvalidResp,
    Disabled,
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
