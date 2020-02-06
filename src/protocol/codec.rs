use super::decoder::DecodeError;
use super::packet::{PacketDecoder, PacketEncoder};
use bytes::BytesMut;
use std::io;
use std::marker::PhantomData;
use tokio::codec::{Decoder, Encoder};

pub struct RespCodec<T>(PhantomData<T>);

impl<T> Default for RespCodec<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T: PacketDecoder> Decoder for RespCodec<T> {
    type Item = T;
    type Error = DecodeError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        T::decode(buf)
    }
}

impl<T: PacketEncoder> Encoder for RespCodec<T> {
    type Item = T;
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, buf: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(|data| buf.extend_from_slice(data))?;
        Ok(())
    }
}
