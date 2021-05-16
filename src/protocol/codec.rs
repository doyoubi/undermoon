use super::decoder::DecodeError;
use super::encoder::EncodeError;
use crate::protocol::packet::{PacketDecoder, PacketEncoder};
use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

pub struct RespCodec<E: PacketEncoder, D: PacketDecoder> {
    encoder: E,
    decoder: D,
}

impl<E: PacketEncoder, D: PacketDecoder> RespCodec<E, D> {
    pub fn new(encoder: E, decoder: D) -> Self {
        Self { encoder, decoder }
    }
}

impl<E: PacketEncoder, D: PacketDecoder> Decoder for RespCodec<E, D> {
    type Item = D::Pkt;
    type Error = DecodeError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.decoder.decode(buf)
    }
}

impl<E: PacketEncoder, D: PacketDecoder> Encoder<E::Pkt> for RespCodec<E, D> {
    type Error = EncodeError<E::Pkt>;

    fn encode(&mut self, item: E::Pkt, buf: &mut BytesMut) -> Result<(), Self::Error> {
        self.encoder
            .encode(item, |data| buf.extend_from_slice(data))?;
        Ok(())
    }
}
