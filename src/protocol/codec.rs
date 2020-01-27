use super::decoder::DecodeError;
use super::encoder::encode_resp;
use super::fp::{RFunctor, VFunctor};
use super::resp::{IndexedResp, RespSlice, RespVec};
use super::stateless::{parse_indexed_resp, ParseError};
use ::common::utils::{change_bulk_array_element, change_bulk_str, get_command_element};
use bytes::BytesMut;
use std::io;
use std::str;
use tokio::codec::{Decoder, Encoder};

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
