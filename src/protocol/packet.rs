use super::decoder::DecodeError;
use super::encoder::{command_to_buf, encode_resp};
use super::fp::{RFunctor, VFunctor};
use super::resp::{BinSafeStr, IndexedResp, Resp, RespSlice, RespVec};
use super::stateless::{parse_indexed_resp, ParseError};
use crate::common::utils::{
    change_bulk_array_element, change_bulk_str, get_command_element, get_command_len, ThreadSafe,
};
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
    type Hint;

    fn encode<F>(self, f: F) -> io::Result<(usize, F)>
    where
        F: FnMut(&[u8]);
    fn get_hint(&self) -> Self::Hint;
}

pub trait DecodedPacket {
    type Hint;

    fn decode(buf: &mut BytesMut, hint: Self::Hint) -> Result<Option<Self>, DecodeError>
    where
        Self: Sized;
}

pub trait FromResp {
    type Hint;

    fn from_resp(resp: RespVec, hint: Self::Hint) -> Self;
}

impl<T: From<RespVec>> FromResp for T {
    type Hint = ();

    fn from_resp(resp: RespVec, _hint: Self::Hint) -> Self {
        Self::from(resp)
    }
}

pub trait Packet: EncodedPacket + DecodedPacket + ThreadSafe + FromResp
where
    Self: DecodedPacket<Hint = <Self as EncodedPacket>::Hint>,
    Self: FromResp<Hint = <Self as EncodedPacket>::Hint>,
{
}
pub trait MonoPacket:
    EncodedPacket<Hint = ()> + DecodedPacket<Hint = ()> + ThreadSafe + FromResp<Hint = ()>
{
}

impl<T: EncodedPacket + DecodedPacket + ThreadSafe + FromResp> Packet for T
where
    Self: DecodedPacket<Hint = <Self as EncodedPacket>::Hint>,
    Self: FromResp<Hint = <Self as EncodedPacket>::Hint>,
{
}
impl<T: EncodedPacket<Hint = ()> + DecodedPacket<Hint = ()> + ThreadSafe + FromResp<Hint = ()>>
    MonoPacket for T
{
}

pub trait PacketEncoder {
    type Pkt: EncodedPacket;

    // Return EncodeError::NotReady if not ready.
    fn encode<F>(&mut self, packet: Self::Pkt, f: F) -> Result<usize, EncodeError<Self::Pkt>>
    where
        F: FnMut(&[u8]);
}

pub trait PacketDecoder {
    type Pkt: DecodedPacket;

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

    pub fn get_array_last_element(&self) -> Option<&[u8]> {
        let len = self.get_array_len()?;
        let index = len.checked_sub(1)?;
        self.get_array_element(index)
    }

    pub fn get_array_len(&self) -> Option<usize> {
        match self {
            Self::Indexed(indexed_resp) => indexed_resp.get_array_len(),
            Self::Data(resp) => get_command_len(&resp),
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
    type Hint = ();

    fn encode<F>(self, mut f: F) -> io::Result<(usize, F)>
    where
        F: FnMut(&[u8]),
    {
        let mut buf = Vec::new();
        let s = command_to_buf(&mut buf, self)?;
        f(&buf);
        Ok((s, f))
    }

    fn get_hint(&self) -> Self::Hint {}
}

// Multiple commands
impl EncodedPacket for Vec<Vec<BinSafeStr>> {
    type Hint = ();

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

    fn get_hint(&self) -> Self::Hint {}
}

impl<T: AsRef<[u8]>> EncodedPacket for Resp<T> {
    type Hint = ();

    fn encode<F>(self, mut f: F) -> io::Result<(usize, F)>
    where
        F: FnMut(&[u8]),
    {
        let mut b = Vec::new();
        let s = encode_resp(&mut b, &self)?;
        f(&b);
        Ok((s, f))
    }

    fn get_hint(&self) -> Self::Hint {}
}

impl DecodedPacket for RespVec {
    type Hint = ();

    fn decode(buf: &mut BytesMut, _hint: Self::Hint) -> Result<Option<Self>, DecodeError>
    where
        Self: Sized,
    {
        let item = IndexedResp::decode(buf, ())?;
        match item {
            Some(resp) => Ok(Some(resp.to_resp_vec())),
            None => Ok(None),
        }
    }
}

impl DecodedPacket for IndexedResp {
    type Hint = ();

    fn decode(buf: &mut BytesMut, _hint: Self::Hint) -> Result<Option<Self>, DecodeError>
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
    type Hint = ();

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

    fn get_hint(&self) -> Self::Hint {}
}

impl DecodedPacket for RespPacket {
    type Hint = ();

    fn decode(buf: &mut BytesMut, _hint: Self::Hint) -> Result<Option<Self>, DecodeError>
    where
        Self: Sized,
    {
        Ok(IndexedResp::decode(buf, ())?.map(RespPacket::Indexed))
    }
}

impl<T: EncodedPacket> EncodedPacket for Box<T> {
    type Hint = ();

    fn encode<F>(self, f: F) -> io::Result<(usize, F)>
    where
        F: FnMut(&[u8]),
    {
        (*self).encode(f)
    }

    fn get_hint(&self) -> Self::Hint {}
}

impl<T: DecodedPacket> DecodedPacket for Box<T> {
    type Hint = T::Hint;

    fn decode(buf: &mut BytesMut, hint: Self::Hint) -> Result<Option<Self>, DecodeError>
    where
        Self: Sized,
    {
        Ok(T::decode(buf, hint)?.map(Box::new))
    }
}

pub fn new_simple_packet_codec<E: EncodedPacket<Hint = ()>, D: DecodedPacket<Hint = ()>>(
) -> (SimplePacketEncoder<E>, SimplePacketDecoder<D>) {
    (
        SimplePacketEncoder::default(),
        SimplePacketDecoder::default(),
    )
}

pub struct SimplePacketEncoder<T: EncodedPacket<Hint = ()>>(PhantomData<T>);
pub struct SimplePacketDecoder<T: DecodedPacket<Hint = ()>>(PhantomData<T>);

impl<T: EncodedPacket<Hint = ()>> Default for SimplePacketEncoder<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T: EncodedPacket<Hint = ()>> PacketEncoder for SimplePacketEncoder<T> {
    type Pkt = T;

    fn encode<F>(&mut self, packet: Self::Pkt, f: F) -> Result<usize, EncodeError<Self::Pkt>>
    where
        F: FnMut(&[u8]),
    {
        packet.encode(f).map(|(s, _)| s).map_err(EncodeError::Io)
    }
}

impl<T: DecodedPacket<Hint = ()>> Default for SimplePacketDecoder<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T: DecodedPacket<Hint = ()>> PacketDecoder for SimplePacketDecoder<T> {
    type Pkt = T;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Pkt>, DecodeError>
    where
        Self: Sized,
    {
        Self::Pkt::decode(buf, ())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum OptionalMulti<T> {
    Single(T),
    Multi(Vec<T>),
}

pub type OptionalMultiHint<T> = OptionalMulti<T>;

impl<T> OptionalMulti<T> {
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

impl<T: FromResp> FromResp for OptionalMulti<T> {
    type Hint = OptionalMultiHint<T::Hint>;

    fn from_resp(resp: RespVec, hint: Self::Hint) -> Self {
        match hint {
            OptionalMultiHint::Single(hint) => Self::Single(T::from_resp(resp, hint)),
            OptionalMultiHint::Multi(hints) => {
                let v = hints
                    .into_iter()
                    .map(|hint| T::from_resp(resp.clone(), hint))
                    .collect();
                Self::Multi(v)
            }
        }
    }
}

impl<T: EncodedPacket> EncodedPacket for OptionalMulti<T> {
    // Vec<()> will be optimized by compiler and will not allocate any memory.
    type Hint = OptionalMultiHint<T::Hint>;

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

    fn get_hint(&self) -> Self::Hint {
        match self {
            Self::Single(t) => OptionalMultiHint::Single(t.get_hint()),
            Self::Multi(v) => OptionalMultiHint::Multi(v.iter().map(|p| p.get_hint()).collect()),
        }
    }
}

impl<T: DecodedPacket> DecodedPacket for OptionalMulti<T> {
    type Hint = OptionalMultiHint<T::Hint>;

    fn decode(buf: &mut BytesMut, hint: Self::Hint) -> Result<Option<Self>, DecodeError>
    where
        Self: Sized,
    {
        let hints = match hint {
            OptionalMultiHint::Single(hint) => {
                return match T::decode(buf, hint)? {
                    Some(p) => Ok(Some(OptionalMulti::Single(p))),
                    None => Ok(None),
                };
            }
            OptionalMultiHint::Multi(hints) => hints,
        };

        if hints.is_empty() {
            return Ok(Some(OptionalMulti::Multi(vec![])));
        }

        let mut packets = vec![];

        for hint in hints {
            let packet = match T::decode(buf, hint)? {
                Some(p) => p,
                None => return Ok(None),
            };
            packets.push(packet);
        }
        Ok(Some(OptionalMulti::Multi(packets)))
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

    fn consume(&self) -> Option<OptionalMultiHint<()>> {
        match self.state.swap(0, Ordering::SeqCst) {
            0 => None,
            1 => Some(OptionalMultiHint::Single(())),
            n => Some(OptionalMultiHint::Multi((0..n - 2).map(|_| ()).collect())),
        }
    }

    fn produce(&self, hint: OptionalMultiHint<()>) -> bool {
        let n = match hint {
            OptionalMultiHint::Single(()) => 1,
            OptionalMultiHint::Multi(v) => v.len() + 2,
        };
        self.state.compare_and_swap(0, n, Ordering::SeqCst) == 0
    }
}

pub fn new_optional_multi_packet_codec<E: EncodedPacket<Hint = ()>, D: DecodedPacket<Hint = ()>>(
) -> (OptionalMultiPacketEncoder<E>, OptionalMultiPacketDecoder<D>) {
    let (s1, s2) = OptionalMultiHintState::new_pair();
    let encoder = OptionalMultiPacketEncoder::new(s1);
    let decoder = OptionalMultiPacketDecoder::new(s2);
    (encoder, decoder)
}

// Does not support pipeline for packet.
// But the packet it self can support pipeline.
pub struct OptionalMultiPacketEncoder<E: EncodedPacket<Hint = ()>> {
    state: OptionalMultiHintState,
    phantom: PhantomData<E>,
}

impl<E: EncodedPacket<Hint = ()>> OptionalMultiPacketEncoder<E> {
    fn new(state: OptionalMultiHintState) -> Self {
        Self {
            state,
            phantom: PhantomData,
        }
    }
}

impl<E: EncodedPacket<Hint = ()>> PacketEncoder for OptionalMultiPacketEncoder<E> {
    type Pkt = OptionalMulti<E>;

    fn encode<F>(&mut self, packet: Self::Pkt, f: F) -> Result<usize, EncodeError<Self::Pkt>>
    where
        F: FnMut(&[u8]),
    {
        let hint = packet.get_hint();
        if !self.state.produce(hint) {
            return Err(EncodeError::NotReady(packet));
        }

        packet.encode(f).map(|(s, _)| s).map_err(EncodeError::Io)
    }
}

pub struct OptionalMultiPacketDecoder<D: DecodedPacket<Hint = ()>> {
    state: OptionalMultiHintState,
    buf: Vec<D>,
    curr_hint: Option<OptionalMultiHint<()>>,
}

impl<D: DecodedPacket<Hint = ()>> OptionalMultiPacketDecoder<D> {
    fn new(state: OptionalMultiHintState) -> Self {
        Self {
            state,
            buf: vec![],
            curr_hint: None,
        }
    }
}

impl<D: DecodedPacket<Hint = ()>> PacketDecoder for OptionalMultiPacketDecoder<D> {
    type Pkt = OptionalMulti<D>;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Pkt>, DecodeError>
    where
        Self: Sized,
    {
        let hint = match &self.curr_hint {
            Some(h) => h.clone(),
            None => {
                let h = match self.state.consume() {
                    Some(h) => h,
                    None => return Ok(None),
                };
                self.curr_hint = Some(h.clone());
                h
            }
        };

        // Empty requested
        if let OptionalMultiHint::Multi(v) = &hint {
            if v.is_empty() {
                self.curr_hint = None;
                return Ok(Some(OptionalMulti::Multi(vec![])));
            }
        }

        loop {
            let packet = match D::decode(buf, ())? {
                Some(p) => p,
                None => return Ok(None),
            };

            match &hint {
                OptionalMultiHint::Single(_) => {
                    self.curr_hint = None;
                    return Ok(Some(OptionalMulti::Single(packet)));
                }
                OptionalMultiHint::Multi(hints) => {
                    self.buf.push(packet);

                    if hints.len() == self.buf.len() {
                        self.curr_hint = None;
                        let v = self.buf.drain(..).collect();

                        return Ok(Some(OptionalMulti::Multi(v)));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Array;
    use matches::assert_matches;

    #[test]
    fn test_single_packet() {
        let (mut encoder, mut decoder) =
            new_optional_multi_packet_codec::<Vec<BinSafeStr>, RespVec>();
        let mut buf = BytesMut::new();
        let cmd = OptionalMulti::Single(vec![b"PING1".to_vec()]);
        encoder
            .encode(cmd, |data| buf.extend_from_slice(data))
            .unwrap();
        assert_eq!(buf.as_ref(), b"*1\r\n$5\r\nPING1\r\n");
        let res = decoder.decode(&mut buf);
        assert!(decoder.curr_hint.is_none());
        assert!(decoder.buf.is_empty());
        assert!(decoder.state.consume().is_none());
        let pkt = res.unwrap().unwrap();
        let response = match pkt {
            OptionalMulti::Multi(_) => panic!("test_single_packet"),
            OptionalMulti::Single(response) => response,
        };
        assert_matches!(response, Resp::Arr(Array::Arr(_)));
    }

    #[test]
    fn test_multi_packet() {
        let (mut encoder, mut decoder) =
            new_optional_multi_packet_codec::<Vec<BinSafeStr>, RespVec>();
        let mut buf = BytesMut::new();
        let cmd = OptionalMulti::Multi(vec![vec![b"PING1".to_vec()], vec![b"PING2".to_vec()]]);
        encoder
            .encode(cmd, |data| buf.extend_from_slice(data))
            .unwrap();
        assert_eq!(buf.as_ref(), b"*1\r\n$5\r\nPING1\r\n*1\r\n$5\r\nPING2\r\n");
        let res = decoder.decode(&mut buf);
        assert!(decoder.curr_hint.is_none());
        assert!(decoder.buf.is_empty());
        assert!(decoder.state.consume().is_none());
        let pkt = res.unwrap().unwrap();
        let response = match pkt {
            OptionalMulti::Single(_) => panic!("test_multi_packet"),
            OptionalMulti::Multi(response) => response,
        };
        assert_eq!(response.len(), 2);
    }

    #[test]
    fn test_empty_multi_packet() {
        let (mut encoder, mut decoder) =
            new_optional_multi_packet_codec::<Vec<BinSafeStr>, RespVec>();
        let mut buf = BytesMut::new();
        let cmd = OptionalMulti::Multi(vec![]);
        encoder
            .encode(cmd, |data| buf.extend_from_slice(data))
            .unwrap();
        assert_eq!(buf.as_ref(), b"");
        let res = decoder.decode(&mut buf);
        assert!(decoder.curr_hint.is_none());
        assert!(decoder.buf.is_empty());
        assert!(decoder.state.consume().is_none());
        let pkt = res.unwrap().unwrap();
        let response = match pkt {
            OptionalMulti::Single(_) => panic!("test_empty_multi_packet"),
            OptionalMulti::Multi(response) => response,
        };
        assert_eq!(response.len(), 0);
    }
}
