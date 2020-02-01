use super::decoder::DecodeError;
use super::encoder::{command_to_buf, encode_resp};
use super::fp::{RFunctor, VFunctor};
use super::resp::{BinSafeStr, IndexedResp, Resp, RespSlice, RespVec};
use super::stateless::{parse_indexed_resp, ParseError};
use crate::common::utils::{change_bulk_array_element, change_bulk_str, get_command_element};
use crate::protocol::EncodeError;
use bytes::BytesMut;
use std::io;
use std::marker::PhantomData;
use std::str;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// EncodedPacket and DecodedPacket abstracts the entries sent between clients and redis instances,
// including the single Resp and multiple Resp.
// TODO: use EncodedPacket + DecodedPacket in CmdTask.

pub trait EncodedPacket {
    fn encode<F>(self, f: F) -> io::Result<(usize, F)>
    where
        F: FnMut(&[u8]);
}

pub trait DecodedPacket {
    fn decode(buf: &mut BytesMut) -> Result<Option<Self>, DecodeError>
    where
        Self: Sized;
}

pub trait Packet: EncodedPacket + DecodedPacket + From<RespVec> {}

impl<T: EncodedPacket + DecodedPacket + From<RespVec>> Packet for T {}

pub trait PacketEncoder {
    type Pkt: EncodedPacket;

    // Return EncodeError::NotReady if not ready.
    fn encode<F>(&mut self, packet: Self::Pkt, f: F) -> Result<usize, EncodeError<Self::Pkt>>
    where
        F: FnMut(&[u8]);
}

pub trait PacketDecoder {
    // Pkt can't decode itself for some types.
    type Pkt;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Pkt>, DecodeError>
    where
        Self: Sized;
}

#[derive(Debug, Clone)]
pub enum RespPacket {
    Indexed(IndexedResp),
    Data(RespVec),
}

impl RespPacket {
    pub fn from_resp_vec(resp: RespVec) -> Self {
        Self::Data(resp)
    }

    pub fn to_resp_vec(&self) -> RespVec {
        match self {
            Self::Indexed(indexed_resp) => indexed_resp.to_resp_vec(),
            Self::Data(resp) => resp.clone(),
        }
    }

    pub fn get_array_element(&self, index: usize) -> Option<&[u8]> {
        match self {
            Self::Indexed(indexed_resp) => indexed_resp.get_array_element(index),
            Self::Data(resp) => get_command_element(&resp, index),
        }
    }

    pub fn get_command_name(&self) -> Option<&str> {
        let element = self.get_array_element(0)?;
        str::from_utf8(element).ok()
    }

    pub fn to_resp_slice(&self) -> RespSlice {
        match self {
            Self::Indexed(indexed_resp) => indexed_resp.to_resp_slice(),
            Self::Data(resp) => resp.as_ref().map(|a| a.as_slice()),
        }
    }

    pub fn into_resp_vec(self) -> RespVec {
        match self {
            Self::Indexed(indexed_resp) => indexed_resp.to_resp_vec(),
            Self::Data(resp) => resp,
        }
    }

    pub fn change_bulk_array_element(&mut self, index: usize, data: Vec<u8>) -> bool {
        let mut resp = match self {
            Self::Indexed(indexed_resp) => indexed_resp.to_resp_vec(),
            Self::Data(resp) => return change_bulk_array_element(resp, index, data),
        };
        let success = change_bulk_array_element(&mut resp, index, data);
        if success {
            *self = Self::Data(resp);
        }
        success
    }

    pub fn change_bulk_str(&mut self, data: Vec<u8>) -> bool {
        let mut resp = match self {
            Self::Indexed(indexed_resp) => indexed_resp.to_resp_vec(),
            Self::Data(resp) => return change_bulk_str(resp, data),
        };
        let success = change_bulk_str(&mut resp, data);
        if success {
            *self = Self::Data(resp);
        }
        success
    }
}

impl From<RespVec> for RespPacket {
    fn from(resp: RespVec) -> Self {
        RespPacket::from_resp_vec(resp)
    }
}

impl EncodedPacket for Vec<BinSafeStr> {
    fn encode<F>(self, mut f: F) -> io::Result<(usize, F)>
    where
        F: FnMut(&[u8]),
    {
        let mut buf = Vec::new();
        let s = command_to_buf(&mut buf, self)?;
        f(&buf);
        Ok((s, f))
    }
}

// Multiple commands
impl EncodedPacket for Vec<Vec<BinSafeStr>> {
    fn encode<F>(self, f: F) -> io::Result<(usize, F)>
    where
        F: FnMut(&[u8]),
    {
        let mut sum = 0;
        let mut f = f;
        for cmd in self.into_iter() {
            let (s, nf) = cmd.encode(f)?;
            sum += s;
            f = nf;
        }
        Ok((sum, f))
    }
}

impl<T: AsRef<[u8]>> EncodedPacket for Resp<T> {
    fn encode<F>(self, mut f: F) -> io::Result<(usize, F)>
    where
        F: FnMut(&[u8]),
    {
        let mut b = Vec::new();
        let s = encode_resp(&mut b, &self)?;
        f(&b);
        Ok((s, f))
    }
}

impl DecodedPacket for RespVec {
    fn decode(buf: &mut BytesMut) -> Result<Option<Self>, DecodeError>
    where
        Self: Sized,
    {
        let item = IndexedResp::decode(buf)?;
        match item {
            Some(resp) => Ok(Some(resp.to_resp_vec())),
            None => Ok(None),
        }
    }
}

impl DecodedPacket for IndexedResp {
    fn decode(buf: &mut BytesMut) -> Result<Option<Self>, DecodeError>
    where
        Self: Sized,
    {
        match parse_indexed_resp(buf) {
            Ok(r) => Ok(Some(r)),
            Err(e) => match e {
                ParseError::NotEnoughData => Ok(None),
                ParseError::InvalidProtocol => Err(DecodeError::InvalidProtocol),
                ParseError::UnexpectedErr => {
                    error!("Unexpected error");
                    Err(DecodeError::InvalidProtocol)
                }
            },
        }
    }
}

impl EncodedPacket for RespPacket {
    fn encode<F>(self, mut f: F) -> io::Result<(usize, F)>
    where
        F: FnMut(&[u8]),
    {
        match self {
            RespPacket::Indexed(indexed_resp) => {
                let data = indexed_resp.get_data();
                f(data);
                Ok((data.len(), f))
            }
            RespPacket::Data(resp) => {
                let mut b = Vec::with_capacity(1024);
                let size = encode_resp(&mut b, &resp)?;
                f(&b);
                Ok((size, f))
            }
        }
    }
}

impl DecodedPacket for RespPacket {
    fn decode(buf: &mut BytesMut) -> Result<Option<Self>, DecodeError>
    where
        Self: Sized,
    {
        Ok(IndexedResp::decode(buf)?.map(RespPacket::Indexed))
    }
}

impl<T: EncodedPacket> EncodedPacket for Box<T> {
    fn encode<F>(self, f: F) -> io::Result<(usize, F)>
    where
        F: FnMut(&[u8]),
    {
        (*self).encode(f)
    }
}

impl<T: DecodedPacket> DecodedPacket for Box<T> {
    fn decode(buf: &mut BytesMut) -> Result<Option<Self>, DecodeError>
    where
        Self: Sized,
    {
        Ok(T::decode(buf)?.map(Box::new))
    }
}

pub fn new_simple_packet_codec<E: EncodedPacket, D: DecodedPacket>(
) -> (SimplePacketEncoder<E>, SimplePacketDecoder<D>) {
    (
        SimplePacketEncoder::default(),
        SimplePacketDecoder::default(),
    )
}

pub struct SimplePacketEncoder<T: EncodedPacket>(PhantomData<T>);
pub struct SimplePacketDecoder<T: DecodedPacket>(PhantomData<T>);

impl<T: EncodedPacket> Default for SimplePacketEncoder<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T: EncodedPacket> PacketEncoder for SimplePacketEncoder<T> {
    type Pkt = T;

    fn encode<F>(&mut self, packet: Self::Pkt, f: F) -> Result<usize, EncodeError<Self::Pkt>>
    where
        F: FnMut(&[u8]),
    {
        packet.encode(f).map(|(s, _)| s).map_err(EncodeError::Io)
    }
}

impl<T: DecodedPacket> Default for SimplePacketDecoder<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T: DecodedPacket> PacketDecoder for SimplePacketDecoder<T> {
    type Pkt = T;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Pkt>, DecodeError>
    where
        Self: Sized,
    {
        Self::Pkt::decode(buf)
    }
}

#[derive(Debug, Clone)]
pub enum OptionalMulti<T> {
    Single(T),
    Multi(Vec<T>),
}

#[derive(Debug, Clone, Copy)]
enum OptionalMultiHint<T> {
    Single,
    Multi(T),
}

impl<T> From<T> for OptionalMulti<T> {
    fn from(t: T) -> Self {
        Self::Single(t)
    }
}

impl<T> OptionalMulti<T> {
    fn to_hint(&self) -> OptionalMultiHint<usize> {
        match self {
            Self::Single(_) => OptionalMultiHint::Single,
            Self::Multi(v) => OptionalMultiHint::Multi(v.len()),
        }
    }

    pub fn map<F, A>(self, f: F) -> OptionalMulti<A>
    where
        F: Fn(T) -> A,
    {
        match self {
            Self::Single(t) => OptionalMulti::Single(f(t)),
            Self::Multi(v) => OptionalMulti::Multi(v.into_iter().map(f).collect()),
        }
    }
}

impl<T: EncodedPacket> EncodedPacket for OptionalMulti<T> {
    fn encode<F>(self, f: F) -> io::Result<(usize, F)>
    where
        F: FnMut(&[u8]),
    {
        match self {
            OptionalMulti::Single(t) => t.encode(f),
            OptionalMulti::Multi(v) => {
                let mut sum = 0;
                let mut f = f;
                for t in v.into_iter() {
                    let (s, nf) = t.encode(f)?;
                    sum += s;
                    f = nf;
                }
                Ok((sum, f))
            }
        }
    }
}

struct OptionalMultiHintState {
    state: Arc<AtomicUsize>,
}

impl OptionalMultiHintState {
    fn new_pair() -> (Self, Self) {
        let s1 = Arc::new(AtomicUsize::new(0));
        let s2 = s1.clone();
        (Self { state: s1 }, Self { state: s2 })
    }

    fn consume(&self) -> Option<OptionalMultiHint<usize>> {
        match self.state.swap(0, Ordering::SeqCst) {
            0 => None,
            1 => Some(OptionalMultiHint::Single),
            n => Some(OptionalMultiHint::Multi(n - 1)),
        }
    }

    fn produce(&self, hint: OptionalMultiHint<usize>) -> bool {
        let n = match hint {
            OptionalMultiHint::Single => 1,
            OptionalMultiHint::Multi(n) => n + 1,
        };
        self.state.compare_and_swap(0, n, Ordering::SeqCst) == 0
    }
}

pub fn new_optional_multi_packet_codec<E: EncodedPacket, D: DecodedPacket>(
) -> (OptionalMultiPacketEncoder<E>, OptionalMultiPacketDecoder<D>) {
    let (s1, s2) = OptionalMultiHintState::new_pair();
    let encoder = OptionalMultiPacketEncoder::new(s1);
    let decoder = OptionalMultiPacketDecoder::new(s2);
    (encoder, decoder)
}

// Does not support pipeline for packet.
// But the packet it self can support pipeline.
pub struct OptionalMultiPacketEncoder<E: EncodedPacket> {
    state: OptionalMultiHintState,
    phantom: PhantomData<E>,
}

impl<E: EncodedPacket> OptionalMultiPacketEncoder<E> {
    fn new(state: OptionalMultiHintState) -> Self {
        Self {
            state,
            phantom: PhantomData,
        }
    }
}

impl<E: EncodedPacket> PacketEncoder for OptionalMultiPacketEncoder<E> {
    type Pkt = OptionalMulti<E>;

    fn encode<F>(&mut self, packet: Self::Pkt, f: F) -> Result<usize, EncodeError<Self::Pkt>>
    where
        F: FnMut(&[u8]),
    {
        let hint = packet.to_hint();
        if !self.state.produce(hint) {
            return Err(EncodeError::NotReady(packet));
        }

        packet.encode(f).map(|(s, _)| s).map_err(EncodeError::Io)
    }
}

pub struct OptionalMultiPacketDecoder<D: DecodedPacket> {
    state: OptionalMultiHintState,
    buf: Vec<D>,
    curr_hint: Option<OptionalMultiHint<usize>>,
}

impl<D: DecodedPacket> OptionalMultiPacketDecoder<D> {
    fn new(state: OptionalMultiHintState) -> Self {
        Self {
            state,
            buf: vec![],
            curr_hint: None,
        }
    }
}

impl<D: DecodedPacket> PacketDecoder for OptionalMultiPacketDecoder<D> {
    type Pkt = OptionalMulti<D>;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Pkt>, DecodeError>
    where
        Self: Sized,
    {
        let hint = match &self.curr_hint {
            Some(h) => *h,
            None => {
                let h = match self.state.consume() {
                    Some(h) => h,
                    None => return Ok(None),
                };
                self.curr_hint = Some(h);
                h
            }
        };

        // Empty requested
        if let OptionalMultiHint::Multi(0) = hint {
            self.curr_hint = None;
            return Ok(Some(OptionalMulti::Multi(vec![])));
        }

        let packet = match D::decode(buf)? {
            Some(p) => p,
            None => return Ok(None),
        };

        match hint {
            OptionalMultiHint::Single => {
                self.curr_hint = None;
                Ok(Some(OptionalMulti::Single(packet)))
            }
            OptionalMultiHint::Multi(size) => {
                self.buf.push(packet);

                if size == self.buf.len() {
                    self.curr_hint = None;
                    let v = self.buf.drain(..).collect();

                    Ok(Some(OptionalMulti::Multi(v)))
                } else {
                    Ok(None)
                }
            }
        }
    }
}
