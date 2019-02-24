use std::fmt;
use std::io;
use std::time::Duration;
use std::error::Error;
use futures::{Future, Stream, future};
use tokio::prelude::FutureExt;
use tokio_core::reactor;
use tokio::net::TcpStream;
use tokio::io::{write_all, AsyncRead, AsyncWrite};
use ::common::utils::ThreadSafe;
use super::resp::{Resp, BinSafeStr};
use super::decoder::{decode_resp, DecodeError};
use super::encoder::command_to_buf;

pub trait RedisClient : ThreadSafe {
    fn execute(&self, address: String, command: Vec<BinSafeStr>) -> Box<dyn Future<Item = Resp, Error = ClientError> + Send>;
}

#[derive(Clone)]
pub struct SimpleRedisClient;

impl SimpleRedisClient {
    pub fn new() -> Self { Self }
}

impl ThreadSafe for SimpleRedisClient {}

impl RedisClient for SimpleRedisClient {
    fn execute(&self, address: String, command: Vec<BinSafeStr>) -> Box<dyn Future<Item = Resp, Error = ClientError> + Send> {
        let sock_address = match address.parse() {
            Ok(address) => address,
            Err(e) => return Box::new(future::err(ClientError::InvalidAddress))
        };
        let connect_fut = TcpStream::connect(&sock_address)
            .map_err(|e| ClientError::Io(e))
            .and_then(move |sock| {
                let mut buf = Vec::new();
                command_to_buf(&mut buf, command);
                let (rx, tx) = sock.split();
                let reader = io::BufReader::new(rx);
                let writer = tx;
                write_all(writer, buf)
                    .map_err(|e| ClientError::Io(e))
                    .and_then(move |(writer, _buf)| {
                        decode_resp(reader)
                            .map_err(|e| {
                                match e {
                                    DecodeError::Io(e) => ClientError::Io(e),
                                    DecodeError::InvalidProtocol => ClientError::InvalidReply,
                                }
                            })
                            .map(|(_sock, resp)| resp)
                    })
            });
        let f = connect_fut.timeout(Duration::from_secs(1)).map_err(|e| {
            println!("timeout {:?}", e);
            ClientError::Timeout
        });
        Box::new(f)
    }
}

#[derive(Debug)]
pub enum ClientError {
    Io(io::Error),
    Timeout,
    InvalidReply,
    InvalidAddress,
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for ClientError {
    fn description(&self) -> &str {
        "client error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            ClientError::Io(err) => Some(err),
            _ => None,
        }
    }
}
