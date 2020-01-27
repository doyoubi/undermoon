use super::decoder::DecodeError;
use super::encoder::encode_resp;
use super::packet::RespPacket;
use super::stateless::{parse_indexed_resp, ParseError};
use bytes::BytesMut;
use std::io;
use tokio::codec::{Decoder, Encoder};

pub struct RespCodec {}

impl Default for RespCodec {
    fn default() -> Self {
        Self {}
    }
}

impl Decoder for RespCodec {
    type Item = Box<RespPacket>;
    type Error = DecodeError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let indexed_resp = match parse_indexed_resp(buf) {
            Ok(r) => r,
            Err(e) => {
                return match e {
                    ParseError::NotEnoughData => Ok(None),
                    ParseError::InvalidProtocol => Err(DecodeError::InvalidProtocol),
                    ParseError::UnexpectedErr => {
                        error!("Unexpected error");
                        Err(DecodeError::InvalidProtocol)
                    }
                };
            }
        };
        let packet = RespPacket::Indexed(indexed_resp);
        Ok(Some(Box::new(packet)))
    }
}

impl Encoder for RespCodec {
    type Item = Box<RespPacket>;
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, buf: &mut BytesMut) -> Result<(), Self::Error> {
        match *item {
            RespPacket::Indexed(indexed_resp) => buf.extend_from_slice(indexed_resp.get_data()),
            RespPacket::Data(resp) => {
                let mut b = Vec::with_capacity(1024);
                let size = encode_resp(&mut b, &resp)?;
                assert_eq!(b.len(), size);
                buf.extend_from_slice(&b);
            }
        }
        Ok(())
    }
}
