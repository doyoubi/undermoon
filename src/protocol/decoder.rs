use super::resp::{Array, BinSafeStr, BulkStr, Resp};
use futures::{future, Future};
use futures::{stream, Stream};
use std::boxed::Box;
use std::cmp::max;
use std::error::Error;
use std::fmt;
use std::io;
use std::iter;
use std::result::Result;
use std::vec::Vec;
use tokio::io::{read_exact, read_until};
use tokio::prelude::AsyncRead;

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
        "decode error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            DecodeError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for DecodeError {
    fn from(e: io::Error) -> Self {
        DecodeError::Io(e)
    }
}

pub const CR: u8 = b'\r';
pub const LF: u8 = b'\n';

fn bytes_to_int(bytes: &[u8]) -> Result<i64, DecodeError> {
    const ZERO: u8 = b'0';
    let mut it = bytes.iter().peekable();
    let f = match it.peek().map(|first| **first as char) {
        Some('-') => {
            it.next();
            -1
        }
        _ => 1,
    };
    it.fold(Ok(0), |sum, i| match *i as char {
        '0'...'9' => sum.map(|s| s * 10 + i64::from(i - ZERO)),
        _ => Err(DecodeError::InvalidProtocol),
    })
    .map(|i| i * f)
}

fn decode_len<R>(reader: R) -> impl Future<Item = (R, i64), Error = DecodeError> + Send
where
    R: AsyncRead + io::BufRead + Send,
{
    decode_line(reader).and_then(|(reader, s)| {
        let len_res = bytes_to_int(&s[..]).map(|l| (reader, l));
        future::result(len_res)
    })
}

fn decode_bulk_str<R>(reader: R) -> impl Future<Item = (R, BulkStr), Error = DecodeError> + Send
where
    R: AsyncRead + io::BufRead + Send,
{
    decode_len(reader).and_then(|(reader, len)| {
        let read_len = if len < 0 { 0 } else { len as usize + 2 }; // add CRLF
        read_exact(reader, vec![0; read_len])
            .map_err(DecodeError::Io)
            .and_then(move |(reader, line)| {
                if len < 0 {
                    return future::ok((reader, BulkStr::Nil));
                }
                if line.len() < 2 || line[line.len() - 2] != CR || line[line.len() - 1] != LF {
                    return future::err(DecodeError::InvalidProtocol);
                }
                let mut s = line;
                s.truncate(len as usize);
                future::ok((reader, BulkStr::Str(s)))
            })
    })
}

fn decode_line<R>(reader: R) -> impl Future<Item = (R, BinSafeStr), Error = DecodeError> + Send
where
    R: AsyncRead + io::BufRead + Send,
{
    read_until(reader, LF, vec![])
        .map_err(DecodeError::Io)
        .and_then(|(reader, line)| {
            let len = line.len();
            if len <= 2 || line[line.len() - 2] != CR {
                return future::err(DecodeError::InvalidProtocol);
            }
            let mut s = line;
            s.truncate(len - 2);
            future::ok((reader, s))
        })
}

pub fn decode_resp<R>(reader: R) -> impl Future<Item = (R, Resp), Error = DecodeError> + Send
where
    R: AsyncRead + io::BufRead + Send + 'static,
{
    read_exact(reader, vec![0; 1])
        .map_err(DecodeError::Io)
        .and_then(|(reader, prefix)| {
            let bf: Box<Future<Item = (R, Resp), Error = DecodeError> + Send> = match prefix[0]
                as char
            {
                '$' => Box::new(
                    decode_bulk_str(reader)
                        .and_then(|(reader, s)| future::ok((reader, Resp::Bulk(s)))),
                ),
                '+' => Box::new(
                    decode_line(reader)
                        .and_then(|(reader, s)| future::ok((reader, Resp::Simple(s)))),
                ),
                ':' => Box::new(
                    decode_line(reader)
                        .and_then(|(reader, s)| future::ok((reader, Resp::Integer(s)))),
                ),
                '-' => Box::new(
                    decode_line(reader)
                        .and_then(|(reader, s)| future::ok((reader, Resp::Error(s)))),
                ),
                '*' => Box::new(
                    decode_array(reader).and_then(|(reader, a)| future::ok((reader, Resp::Arr(a)))),
                ),
                prefix => {
                    error!("Unexpected prefix {:?}", prefix);
                    Box::new(future::err(DecodeError::InvalidProtocol))
                }
            };
            bf
        })
}

fn decode_array<R>(reader: R) -> impl Future<Item = (R, Array), Error = DecodeError> + Send
where
    R: AsyncRead + io::BufRead + Send + 'static,
{
    decode_len(reader).and_then(|(reader, len)| {
        let iter_size = max(0, len) as usize;
        let stream = stream::iter_ok(iter::repeat(()).take(iter_size));
        stream
            .fold(
                (reader, Vec::with_capacity(iter_size)),
                |(reader, acc), ()| {
                    let mut acc = acc;
                    decode_resp(reader).map(|(reader, resp)| {
                        acc.push(resp);
                        (reader, acc)
                    })
                },
            )
            .map(move |(reader, acc)| {
                if len < 0 {
                    (reader, Array::Nil)
                } else {
                    (reader, Array::Arr(acc))
                }
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str;

    #[test]
    fn test_bytes_to_int() {
        assert_eq!(
            233,
            bytes_to_int("233".as_bytes()).expect("test_bytes_to_int")
        );
        assert_eq!(
            -233,
            bytes_to_int("-233".as_bytes()).expect("test_bytes_to_int")
        );
        assert!(bytes_to_int("233a".as_bytes()).is_err())
    }

    #[test]
    fn test_decode_len() {
        let c = io::Cursor::new("233\r\n".as_bytes());
        let r = decode_len(c).wait();
        assert!(r.is_ok());
        let (_, l) = r.expect("test_decode_len");
        assert_eq!(l, 233);

        let c = io::Cursor::new("-233\r\n".as_bytes());
        let r = decode_len(c).wait();
        assert!(r.is_ok());
        let (_, l) = r.expect("test_decode_len");
        assert_eq!(l, -233);

        let c = io::Cursor::new("2a3\r\n".as_bytes());
        let r = decode_len(c).wait();
        assert!(r.is_err());
    }

    #[test]
    fn test_decode_bulk_str() {
        let c = io::Cursor::new("2\r\nab\r\n".as_bytes());
        let r = decode_bulk_str(c).wait();
        assert!(r.is_ok());
        let (_, s) = r.expect("test_decode_bulk_str");
        assert_eq!(BulkStr::Str(String::from("ab").into_bytes()), s);

        let c = io::Cursor::new("-1\r\n".as_bytes());
        let r = decode_bulk_str(c).wait();
        assert!(r.is_ok());
        let (_, s) = r.expect("test_decode_bulk_str");
        assert_eq!(BulkStr::Nil, s);

        let c = io::Cursor::new("2a3\r\nab\r\n".as_bytes());
        let r = decode_bulk_str(c).wait();
        assert!(r.is_err());

        let c = io::Cursor::new("0\r\n\r\n".as_bytes());
        let r = decode_bulk_str(c).wait();
        assert!(r.is_ok());
        let (_, s) = r.expect("test_decode_bulk_str");
        assert_eq!(BulkStr::Str(String::from("").into_bytes()), s);

        let c = io::Cursor::new("1\r\na\r\n".as_bytes());
        let r = decode_bulk_str(c).wait();
        assert!(r.is_ok());

        let c = io::Cursor::new("2\r\na\r\n".as_bytes());
        let r = decode_bulk_str(c).wait();
        assert!(r.is_err());

        let c = io::Cursor::new("2\r\nabc\r\n".as_bytes());
        let r = decode_bulk_str(c).wait();
        assert!(r.is_err());
    }

    #[test]
    fn test_decode_line() {
        let c = io::Cursor::new("233\r\n".as_bytes());
        let r = decode_line(c).wait();
        assert!(r.is_ok());
        let (_, l) = r.expect("test_decode_line");
        assert_eq!(str::from_utf8(&l[..]), Ok("233"));

        let c = io::Cursor::new("-233\r\n".as_bytes());
        let r = decode_line(c).wait();
        assert!(r.is_ok());
        let (_, l) = r.expect("test_decode_line");
        assert_eq!(str::from_utf8(&l[..]), Ok("-233"));
    }

    #[test]
    fn test_decode_array() {
        let c = io::Cursor::new("2\r\n$1\r\na\r\n$2\r\nbc\r\n".as_bytes());
        let r = decode_array(c).wait();
        assert!(r.is_ok());
        let (_, a) = r.expect("test_decode_array");
        assert_eq!(
            Array::Arr(vec![
                Resp::Bulk(BulkStr::Str(String::from("a").into_bytes())),
                Resp::Bulk(BulkStr::Str(String::from("bc").into_bytes())),
            ]),
            a
        );

        let c = io::Cursor::new("-1\r\n".as_bytes());
        let r = decode_array(c).wait();
        assert!(r.is_ok());
        let (_, a) = r.expect("test_decode_array");
        assert_eq!(Array::Nil, a);

        let c = io::Cursor::new("0\r\n".as_bytes());
        let r = decode_array(c).wait();
        assert!(r.is_ok());
        let (_, a) = r.expect("test_decode_array");
        assert_eq!(Array::Arr(vec![]), a);

        let c = io::Cursor::new("1\r\n$2\r\na\r\n".as_bytes());
        let r = decode_array(c).wait();
        assert!(r.is_err());

        let c = io::Cursor::new("1\r\n$2\r\nabc\r\n".as_bytes());
        let r = decode_array(c).wait();
        assert!(r.is_err());
    }

    #[test]
    fn test_decode_resp() {
        let c = io::Cursor::new("*-1\r\n".as_bytes());
        let r = decode_resp(c).wait();
        assert!(r.is_ok());
        let (_, a) = r.expect("test_decode_resp");
        assert_eq!(Resp::Arr(Array::Nil), a);

        let c = io::Cursor::new("*0\r\n".as_bytes());
        let r = decode_resp(c).wait();
        assert!(r.is_ok());
        let (_, a) = r.expect("test_decode_resp");
        assert_eq!(Resp::Arr(Array::Arr(vec![])), a);

        let c = io::Cursor::new("-abc\r\n".as_bytes());
        let r = decode_resp(c).wait();
        assert!(r.is_ok());
        let (_, a) = r.expect("test_decode_resp");
        assert_eq!(Resp::Error(String::from("abc").into_bytes()), a);

        let c = io::Cursor::new(":233\r\n".as_bytes());
        let r = decode_resp(c).wait();
        assert!(r.is_ok());
        let (_, a) = r.expect("test_decode_resp");
        assert_eq!(Resp::Integer(String::from("233").into_bytes()), a);

        let c = io::Cursor::new("+233\r\n".as_bytes());
        let r = decode_resp(c).wait();
        assert!(r.is_ok());
        let (_, a) = r.expect("test_decode_resp");
        assert_eq!(Resp::Simple(String::from("233").into_bytes()), a);

        let c = io::Cursor::new("$3\r\nfoo\r\n".as_bytes());
        let r = decode_resp(c).wait();
        assert!(r.is_ok());
        let (_, a) = r.expect("test_decode_resp");
        assert_eq!(
            Resp::Bulk(BulkStr::Str(String::from("foo").into_bytes())),
            a
        );
    }
}
