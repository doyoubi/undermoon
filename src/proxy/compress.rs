use super::backend::ConnFactory;
use super::command::DataCmdType;
use super::manager::SharedMetaMap;
use super::session::CmdCtx;
use crate::common::config::CompressionStrategy;
use crate::protocol::{Array, BulkStr, OptionalMulti, Resp, RespPacket};
use std::error::Error;
use std::fmt;
use std::io;

// CompressionStrategyConfig is used for easier mocking in unit tests.
pub trait CompressionStrategyConfig {
    fn get_config(&self) -> CompressionStrategy;
}

pub struct CompressionStrategyMetaMapConfig<C: ConnFactory<Pkt = RespPacket>> {
    meta_map: SharedMetaMap<C>,
}

impl<C: ConnFactory<Pkt = RespPacket>> CompressionStrategyMetaMapConfig<C> {
    pub fn new(meta_map: SharedMetaMap<C>) -> Self {
        Self { meta_map }
    }
}

impl<C: ConnFactory<Pkt = RespPacket>> CompressionStrategyConfig
    for CompressionStrategyMetaMapConfig<C>
{
    fn get_config(&self) -> CompressionStrategy {
        let meta_map = self.meta_map.load();
        meta_map.get_cluster_map().get_config().compression_strategy
    }
}

pub struct CmdCompressor<C: CompressionStrategyConfig> {
    config: C,
}

impl<C: CompressionStrategyConfig> CmdCompressor<C> {
    pub fn new(config: C) -> Self {
        Self { config }
    }

    pub fn try_compressing_cmd_ctx(&self, cmd_ctx: &mut CmdCtx) -> Result<(), CompressionError> {
        let strategy = self.config.get_config();

        if strategy == CompressionStrategy::Disabled {
            return Err(CompressionError::Disabled);
        }

        let index = match cmd_ctx.get_data_cmd_type() {
            DataCmdType::Getset | DataCmdType::Set | DataCmdType::Setnx => OptionalMulti::Single(2),
            DataCmdType::Psetex | DataCmdType::Setex => OptionalMulti::Single(3),
            DataCmdType::Mset | DataCmdType::Msetnx => {
                let l = match cmd_ctx.get_cmd().get_command_len() {
                    None => return Err(CompressionError::InvalidRequest),
                    Some(l) => l,
                };
                let key_indices = (2..l).step_by(2).collect();
                OptionalMulti::Multi(key_indices)
            }
            DataCmdType::Append
            | DataCmdType::Bitcount
            | DataCmdType::Bitfield
            | DataCmdType::Bitop
            | DataCmdType::Bitpos
            | DataCmdType::Decr
            | DataCmdType::Decrby
            | DataCmdType::Getbit
            | DataCmdType::Getrange
            | DataCmdType::Incr
            | DataCmdType::Incrby
            | DataCmdType::Incrbyfloat
            | DataCmdType::Mget
            | DataCmdType::Setbit
            | DataCmdType::Setrange
            | DataCmdType::Strlen => match strategy {
                CompressionStrategy::SetGetOnly => return Err(CompressionError::RestrictedCmd),
                _ => return Err(CompressionError::UnsupportedCmdType),
            },
            _ => return Ok(()),
        };

        match index {
            OptionalMulti::Single(index) => Self::compress_one_element(cmd_ctx, index),
            OptionalMulti::Multi(indices) => {
                for index in indices.into_iter() {
                    Self::compress_one_element(cmd_ctx, index)?;
                }
                Ok(())
            }
        }
    }

    fn compress_one_element(cmd_ctx: &mut CmdCtx, index: usize) -> Result<(), CompressionError> {
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

pub struct CmdReplyDecompressor<C: CompressionStrategyConfig> {
    config: C,
}

impl<C: CompressionStrategyConfig> CmdReplyDecompressor<C> {
    pub fn new(config: C) -> Self {
        Self { config }
    }

    pub fn decompress(
        &self,
        cmd_ctx: &CmdCtx,
        packet: &mut RespPacket,
    ) -> Result<(), CompressionError> {
        let strategy = self.config.get_config();

        if strategy == CompressionStrategy::Disabled {
            return Err(CompressionError::Disabled);
        }

        let data_cmd_type = cmd_ctx.get_data_cmd_type();
        match data_cmd_type {
            DataCmdType::Get | DataCmdType::Getset => {
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
            DataCmdType::Mget => {
                let compressed_arr = if let Resp::Arr(Array::Arr(arr)) = packet.to_resp_slice() {
                    let mut compressed_arr = vec![];
                    for bulk_str in arr.iter() {
                        let element = match bulk_str {
                            Resp::Bulk(BulkStr::Str(s)) => {
                                let compressed = match zstd::decode_all(*s) {
                                    Ok(c) => c,
                                    Err(err) => {
                                        return Err(CompressionError::Io(err));
                                    }
                                };
                                Some(compressed)
                            }
                            _ => None,
                        };
                        compressed_arr.push(element);
                    }
                    compressed_arr
                } else {
                    vec![]
                };

                for (i, compressed) in compressed_arr.into_iter().enumerate() {
                    if let Some(c) = compressed {
                        if !packet.change_bulk_array_element(i, c) {
                            return Err(CompressionError::InvalidResp);
                        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::BinSafeStr;
    use crate::proxy::command::{new_command_pair, Command};

    fn gen_array_packet(array: Vec<String>) -> RespPacket {
        let arr = array
            .into_iter()
            .map(|s| Resp::Bulk(BulkStr::Str(s.into_bytes())))
            .collect();
        let resp = Resp::Arr(Array::Arr(arr));
        RespPacket::Data(resp)
    }

    fn gen_bulk_str_packet(s: BinSafeStr) -> RespPacket {
        RespPacket::Data(Resp::Bulk(BulkStr::Str(s)))
    }

    fn gen_cmd_ctx(cmd: Vec<String>) -> CmdCtx {
        let request = gen_array_packet(cmd);
        let cmd = Command::new(Box::new(request));
        let (sender, _) = new_command_pair(&cmd);
        CmdCtx::new(cmd, sender, 233, false)
    }

    struct DummyConfig {
        strategy: CompressionStrategy,
    }

    impl CompressionStrategyConfig for DummyConfig {
        fn get_config(&self) -> CompressionStrategy {
            self.strategy
        }
    }

    #[test]
    fn test_disabled_for_set() {
        let mut cmd_ctx = gen_cmd_ctx(vec![
            "SET".to_string(),
            "key".to_string(),
            "value".to_string(),
        ]);
        let config = DummyConfig {
            strategy: CompressionStrategy::Disabled,
        };
        let compressor = CmdCompressor::new(config);
        let err = compressor
            .try_compressing_cmd_ctx(&mut cmd_ctx)
            .unwrap_err();
        assert!(matches!(err, CompressionError::Disabled));
    }

    #[test]
    fn test_disabled_for_get() {
        let mut cmd_ctx = gen_cmd_ctx(vec!["GET".to_string(), "key".to_string()]);
        let config = DummyConfig {
            strategy: CompressionStrategy::Disabled,
        };
        let decompressor = CmdReplyDecompressor::new(config);
        let mut reply_packet = gen_bulk_str_packet("value".to_string().into_bytes());
        let err = decompressor
            .decompress(&mut cmd_ctx, &mut reply_packet)
            .unwrap_err();
        assert!(matches!(err, CompressionError::Disabled));
    }

    #[test]
    fn test_enabled_for_set_get() {
        let comppressed_value = {
            let mut cmd_ctx = gen_cmd_ctx(vec![
                "SET".to_string(),
                "key".to_string(),
                "value".to_string(),
            ]);
            let config = DummyConfig {
                strategy: CompressionStrategy::SetGetOnly,
            };
            let compressor = CmdCompressor::new(config);
            compressor.try_compressing_cmd_ctx(&mut cmd_ctx).unwrap();
            let comppressed_value = cmd_ctx.get_cmd().get_command_element(2).unwrap().to_vec();
            assert_ne!(comppressed_value, b"value");
            comppressed_value
        };

        let mut cmd_ctx = gen_cmd_ctx(vec!["GET".to_string(), "key".to_string()]);
        let config = DummyConfig {
            strategy: CompressionStrategy::SetGetOnly,
        };
        let decompressor = CmdReplyDecompressor::new(config);
        let mut reply_packet = gen_bulk_str_packet(comppressed_value);
        decompressor
            .decompress(&mut cmd_ctx, &mut reply_packet)
            .unwrap();
        let v = match reply_packet {
            RespPacket::Data(Resp::Bulk(BulkStr::Str(v))) => v,
            _ => panic!(),
        };
        assert_eq!(v, b"value");
    }

    #[test]
    fn test_enabled_for_mset_mget() {
        let (comppressed_value_one, comppressed_value_two) = {
            let mut cmd_ctx = gen_cmd_ctx(vec![
                "MSET".to_string(),
                "key_one".to_string(),
                "value_one".to_string(),
                "key_two".to_string(),
                "value_two".to_string(),
            ]);
            let config = DummyConfig {
                strategy: CompressionStrategy::SetGetOnly,
            };
            let compressor = CmdCompressor::new(config);
            compressor.try_compressing_cmd_ctx(&mut cmd_ctx).unwrap();
            let comppressed_value_one = cmd_ctx.get_cmd().get_command_element(2).unwrap().to_vec();
            assert_ne!(comppressed_value_one, b"value_one");
            let comppressed_value_two = cmd_ctx.get_cmd().get_command_element(4).unwrap().to_vec();
            assert_ne!(comppressed_value_two, b"value_two");
            (comppressed_value_one, comppressed_value_two)
        };

        let mut cmd_ctx = gen_cmd_ctx(vec![
            "MGET".to_string(),
            "key_one".to_string(),
            "key_two".to_string(),
        ]);
        let config = DummyConfig {
            strategy: CompressionStrategy::SetGetOnly,
        };
        let decompressor = CmdReplyDecompressor::new(config);
        let mut reply_packet = RespPacket::Data(Resp::Arr(Array::Arr(vec![
            Resp::Bulk(BulkStr::Str(comppressed_value_one)),
            Resp::Bulk(BulkStr::Str(comppressed_value_two)),
        ])));
        decompressor
            .decompress(&mut cmd_ctx, &mut reply_packet)
            .unwrap();
        let arr = match reply_packet {
            RespPacket::Data(Resp::Arr(Array::Arr(arr))) => arr,
            _ => panic!(),
        };
        let reply_one = match &arr[0] {
            Resp::Bulk(BulkStr::Str(s)) => s,
            _ => panic!(),
        };
        let reply_two = match &arr[1] {
            Resp::Bulk(BulkStr::Str(s)) => s,
            _ => panic!(),
        };
        assert_eq!(reply_one, b"value_one");
        assert_eq!(reply_two, b"value_two");
    }
}
