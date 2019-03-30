use std::fmt;
use std::io;
use std::time::Duration;
use std::error::Error;
use futures::{Future, future};
use tokio::prelude::FutureExt;
use tokio::net::TcpStream;
use tokio::io::{write_all, AsyncRead};
use ::common::utils::ThreadSafe;
use super::resp::{Resp, BinSafeStr};
use super::decoder::{decode_resp, DecodeError};
use super::encoder::command_to_buf;

pub trait RedisClient : ThreadSafe {
    fn execute(&self, address: String, command: Vec<BinSafeStr>) -> Box<dyn Future<Item = Resp, Error =RedisClientError> + Send>;
}

#[derive(Clone)]
pub struct SimpleRedisClient;

impl SimpleRedisClient {
    pub fn new() -> Self { Self }
}

impl ThreadSafe for SimpleRedisClient {}

impl RedisClient for SimpleRedisClient {
    fn execute(&self, address: String, command: Vec<BinSafeStr>) -> Box<dyn Future<Item = Resp, Error =RedisClientError> + Send> {
        let sock_address = match address.parse() {
            Ok(address) => address,
            Err(_e) => return Box::new(future::err(RedisClientError::InvalidAddress))
        };
        let address_clone = address.clone();
        let connect_fut = TcpStream::connect(&sock_address)
            .map_err(|e| RedisClientError::Io(e))
            .and_then(move |sock| {
                let mut buf = Vec::new();
                command_to_buf(&mut buf, command);
                let (rx, tx) = sock.split();
                let reader = io::BufReader::new(rx);
                let writer = tx;
                write_all(writer, buf)
                    .map_err(|e| RedisClientError::Io(e))
                    .and_then(move |(_writer, _buf)| {
                        decode_resp(reader)
                            .map_err(|e| {
                                match e {
                                    DecodeError::Io(e) => RedisClientError::Io(e),
                                    DecodeError::InvalidProtocol => RedisClientError::InvalidReply,
                                }
                            })
                            .map(|(_sock, resp)| resp)
                    })
            });
        let f = connect_fut.timeout(Duration::from_secs(1)).map_err(move |e| {
            error!("redis client error {} {:?}", address_clone, e);
            RedisClientError::Timeout
        });
        Box::new(f)
    }
}

#[derive(Debug)]
pub enum RedisClientError {
    Io(io::Error),
    Timeout,
    InvalidReply,
    InvalidAddress,
}

impl fmt::Display for RedisClientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for RedisClientError {
    fn description(&self) -> &str {
        "client error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            RedisClientError::Io(err) => Some(err),
            _ => None,
        }
    }
}
