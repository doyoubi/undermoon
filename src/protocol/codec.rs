use super::decoder::DecodeError;
use super::packet::{PacketDecoder, PacketEncoder};
use bytes::BytesMut;
use std::io;
use std::marker::PhantomData;
use tokio_util::codec::{Decoder, Encoder};

pub struct RespCodec<E, D>(PhantomData<E>, PhantomData<D>);

impl<E, D> Default for RespCodec<E, D> {
    fn default() -> Self {
        Self(PhantomData, PhantomData)
    }
}

impl<E, D: PacketDecoder> Decoder for RespCodec<E, D> {
    type Item = D;
    type Error = DecodeError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        D::decode(buf)
    }
}

impl<E: PacketEncoder, D> Encoder for RespCodec<E, D> {
    type Item = E;
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, buf: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(|data| buf.extend_from_slice(data))?;
        Ok(())
    }
}
