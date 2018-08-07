use std::io;
use std::error::Error;
use std::fmt;
use std::result::Result;
use tokio::prelude::{AsyncRead};
use tokio::io::read_until;
use futures::{future, Future};
use super::resp::{Resp};

#[derive(Debug)]
pub enum DecodeError {
    InvalidProtocol,
    Io(io::Error),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for DecodeError {
    fn description(&self) -> &str {
        "I'm the superhero of errors"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            DecodeError::Io(err) => Some(err),
            _ => None,
        }
    }
}

type ParseResult = Result<(), DecodeError>;

const CR: u8 = '\r' as u8;
const LF: u8 = '\n' as u8;

fn bytes_to_int(bytes: &[u8]) -> Result<i64, DecodeError> {
    const ZERO : u8 = '0' as u8;
    bytes.iter().fold(Ok(0), |sum, i| {
        match *i as char {
            '0' ... '9' => sum.map(|s| s * 10 + (i - ZERO) as i64),
            _ => Err(DecodeError::InvalidProtocol),
        }
    })
}

fn decode_len<R>(reader: R) -> impl Future<Item = (R, i64), Error = DecodeError>
    where R: AsyncRead + io::BufRead
{
    let b = vec![];
    read_until(reader, LF, b)
        .map_err(|e| DecodeError::Io(e))
        .and_then(|(reader, buf)| {
        if buf.len() <= 2 {
            return future::err(DecodeError::InvalidProtocol)
        }
        let num_len = buf.len() - 2;
        let len_res = bytes_to_int(&buf[..num_len]).map(|l| (reader, l));
        future::result(len_res)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_to_int() {
        assert_eq!(233, bytes_to_int("233".as_bytes()).unwrap());
        assert!(bytes_to_int("233a".as_bytes()).is_err())
    }
}