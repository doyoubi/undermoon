use std::str;
use std::io;
use bytes::{BufMut, BytesMut};
use tokio::codec::{Encoder, Decoder};
use super::resp::Resp;
use super::stateless::{parse_resp, ParseError};
use super::decoder::DecodeError;
use super::encoder::encode_resp;

pub struct RespCodec {}

impl Decoder for RespCodec {
    type Item = Resp;
    type Error = DecodeError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let (resp, consumed) = match parse_resp(&buf) {
            Ok(r) => r,
            Err(e) => {
                return match e {
                    ParseError::NotEnoughData => Ok(None),
                    ParseError::InvalidProtocol => Err(DecodeError::InvalidProtocol),
                    ParseError::Io(e) => Err(DecodeError::Io(e)),
                }
            },
        };
        buf.advance(consumed);
        Ok(Some(resp))
    }
}

impl Encoder for RespCodec {
    type Item = Resp;
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, buf: &mut BytesMut) -> Result<(), Self::Error> {
        let mut b = vec![];
        let size = encode_resp(&mut b, &item)?;
        assert_eq!(b.len(), size);
        buf.extend_from_slice(&b);
        Ok(())
    }
}
