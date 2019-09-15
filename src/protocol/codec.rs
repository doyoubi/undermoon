use super::decoder::DecodeError;
use super::encoder::encode_resp;
use super::resp::Resp;
use super::stateless::{parse_resp, ParseError};
use ::common::utils::{change_bulk_array_element, change_bulk_str};
use atomic_option::AtomicOption;
use bytes::BytesMut;
use std::fmt;
use std::io;
use std::sync::atomic::Ordering;
use tokio::codec::{Decoder, Encoder};

pub struct RespPacket {
    resp: Resp,
    data: AtomicOption<BytesMut>,
}

impl RespPacket {
    pub fn new(resp: Resp) -> Self {
        Self {
            resp,
            data: AtomicOption::empty(),
        }
    }
    pub fn new_with_buf(resp: Resp, data: BytesMut) -> Self {
        Self {
            resp,
            data: AtomicOption::new(Box::new(data)),
        }
    }

    pub fn get_resp(&self) -> &Resp {
        &self.resp
    }

    pub fn get_mut_resp(&mut self) -> &mut Resp {
        &mut self.resp
    }

    pub fn drain_data(&self) -> Option<BytesMut> {
        self.data.take(Ordering::SeqCst).map(|data| *data)
    }

    pub fn change_bulk_array_element(&mut self, index: usize, data: Vec<u8>) -> bool {
        let success = change_bulk_array_element(&mut self.resp, index, data);
        if success {
            // clear the cache
            self.data.take(Ordering::SeqCst);
        }
        success
    }

    pub fn change_bulk_str(&mut self, data: Vec<u8>) -> bool {
        let r = change_bulk_str(&mut self.resp, data);
        if r {
            // clear the cache
            self.data.take(Ordering::SeqCst);
        }
        r
    }
}

impl fmt::Debug for RespPacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RespPacket(resp={:?})", self.resp)
    }
}

pub struct RespCodec {}

impl Decoder for RespCodec {
    type Item = Box<RespPacket>;
    type Error = DecodeError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let (resp, consumed) = match parse_resp(&buf) {
            Ok(r) => r,
            Err(e) => {
                return match e {
                    ParseError::NotEnoughData => Ok(None),
                    ParseError::InvalidProtocol => Err(DecodeError::InvalidProtocol),
                    ParseError::Io(e) => Err(DecodeError::Io(e)),
                };
            }
        };
        let resp_raw_data = buf.split_to(consumed);
        Ok(Some(Box::new(RespPacket::new_with_buf(
            resp,
            resp_raw_data,
        ))))
    }
}

impl Encoder for RespCodec {
    type Item = Box<RespPacket>;
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, buf: &mut BytesMut) -> Result<(), Self::Error> {
        match item.drain_data() {
            Some(raw_data) => {
                buf.extend_from_slice(&raw_data);
            }
            None => {
                let mut b = Vec::with_capacity(1024);
                let size = encode_resp(&mut b, item.get_resp())?;
                assert_eq!(b.len(), size);
                buf.extend_from_slice(&b);
            }
        };
        Ok(())
    }
}
