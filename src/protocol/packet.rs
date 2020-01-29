use super::decoder::DecodeError;
use super::encoder::{command_to_buf, encode_resp};
use super::fp::{RFunctor, VFunctor};
use super::resp::{BinSafeStr, IndexedResp, Resp, RespSlice, RespVec};
use super::stateless::{parse_indexed_resp, ParseError};
use crate::common::utils::{change_bulk_array_element, change_bulk_str, get_command_element};
use bytes::BytesMut;
use std::io;
use std::str;

// PacketEncoder and PacketDecoder abstracts the entries sent between clients and redis instances,
// including the single Resp and multiple Resp.
// TODO: use TaskEncoder + TaskDecoder in CmdTask.

pub trait PacketEncoder {
    fn encode<F>(self, f: F) -> io::Result<usize>
    where
        F: FnMut(&[u8]);
}

pub trait PacketDecoder {
    fn decode(buf: &mut BytesMut) -> Result<Option<Self>, DecodeError>
    where
        Self: Sized;
}

pub trait Packet: PacketEncoder + PacketDecoder + From<RespVec> {}

impl<T: PacketEncoder + PacketDecoder + From<RespVec>> Packet for T {}

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

impl PacketEncoder for Vec<BinSafeStr> {
    fn encode<F>(self, mut f: F) -> io::Result<usize>
    where
        F: FnMut(&[u8]),
    {
        let mut buf = Vec::new();
        let s = command_to_buf(&mut buf, self)?;
        f(&buf);
        Ok(s)
    }
}

// Multiple commands
impl PacketEncoder for Vec<Vec<BinSafeStr>> {
    fn encode<F>(self, mut f: F) -> io::Result<usize>
    where
        F: FnMut(&[u8]),
    {
        let mut buf = Vec::new();
        let mut s = 0;
        for cmd in self.into_iter() {
            s += command_to_buf(&mut buf, cmd)?;
        }
        f(&buf);
        Ok(s)
    }
}

impl<T: AsRef<[u8]>> PacketEncoder for Resp<T> {
    fn encode<F>(self, mut f: F) -> io::Result<usize>
    where
        F: FnMut(&[u8]),
    {
        let mut b = Vec::new();
        let s = encode_resp(&mut b, &self)?;
        f(&b);
        Ok(s)
    }
}

impl PacketDecoder for RespVec {
    fn decode(buf: &mut BytesMut) -> Result<Option<Self>, DecodeError>
    where
        Self: Sized,
    {
        let item = IndexedResp::decode(buf)?;
        match item {
            Some(resp) => Ok(Some(resp.to_resp_vec())),
            None => return Ok(None),
        }
    }
}

impl PacketDecoder for IndexedResp {
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

impl PacketEncoder for RespPacket {
    fn encode<F>(self, mut f: F) -> io::Result<usize>
    where
        F: FnMut(&[u8]),
    {
        match self {
            RespPacket::Indexed(indexed_resp) => {
                let data = indexed_resp.get_data();
                f(data);
                Ok(data.len())
            }
            RespPacket::Data(resp) => {
                let mut b = Vec::with_capacity(1024);
                let size = encode_resp(&mut b, &resp)?;
                f(&b);
                Ok(size)
            }
        }
    }
}

impl PacketDecoder for RespPacket {
    fn decode(buf: &mut BytesMut) -> Result<Option<Self>, DecodeError>
    where
        Self: Sized,
    {
        Ok(IndexedResp::decode(buf)?.map(RespPacket::Indexed))
    }
}

impl<T: PacketEncoder> PacketEncoder for Box<T> {
    fn encode<F>(self, f: F) -> io::Result<usize>
    where
        F: FnMut(&[u8]),
    {
        (*self).encode(f)
    }
}

impl<T: PacketDecoder> PacketDecoder for Box<T> {
    fn decode(buf: &mut BytesMut) -> Result<Option<Self>, DecodeError>
    where
        Self: Sized,
    {
        Ok(T::decode(buf)?.map(Box::new))
    }
}

pub enum OptionalMulti<T> {
    Simple(T),
    Multi(Vec<T>),
}

impl<T: From<RespVec>> From<RespVec> for OptionalMulti<T> {
    fn from(resp: RespVec) -> Self {
        Self::Simple(T::from(resp))
    }
}
