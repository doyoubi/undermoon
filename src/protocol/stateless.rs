use std::mem;
use std::io::{self, BufRead};
use std::error::Error;
use std::fmt;
use btoi::btoi;
use futures::{Future, Poll, Async};
use tokio::prelude::AsyncRead;
use super::resp::{Resp, BulkStr, BinSafeStr, Array};
use super::decoder::{LF, DecodeError};

#[derive(Debug)]
pub enum ParseError {
    InvalidProtocol,
    NotEnoughData,
    Io(io::Error),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for ParseError {
    fn description(&self) -> &str {
        "decode error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            ParseError::Io(err) => Some(err),
            _ => None,
        }
    }
}

pub fn stateless_decode_resp<R>(reader: R) -> impl Future<Item = (R, Resp), Error = DecodeError> + Send
    where R: AsyncRead + io::BufRead + Send + 'static
{
    RespParser::new(reader)
}

#[derive(Debug)]
enum State<R> {
    Reading(R),
    Empty,
}

pub struct RespParser<R: AsyncRead + io::BufRead + Send + 'static> {
    state: State<R>
}

impl<R: AsyncRead + io::BufRead + Send + 'static> RespParser<R> {
    fn new(reader: R) -> Self {
        Self{state: State::Reading(reader)}
    }
}

impl<R: AsyncRead + io::BufRead + Send + 'static> Future for RespParser<R> {
    type Item = (R, Resp);
    type Error = DecodeError;

    fn poll(&mut self) -> Poll<Self::Item, DecodeError> {
        let (resp, consumed) = match self.state {
            State::Reading(ref mut reader) => {
                let mut buf = match reader.fill_buf() {
                    Ok(buf) => buf,
                    Err(e) => {
                        if e.kind() == io::ErrorKind::WouldBlock {
                            return Ok(Async::NotReady);
                        }
                        error!("io error when parsing {:?}", e);
                        return Err(DecodeError::Io(e));
                    },
                };
                match parse_resp(buf) {
                    Ok(r) => r,
                    Err(ParseError::NotEnoughData) => return Ok(Async::NotReady),
                    Err(ParseError::InvalidProtocol) => return Err(DecodeError::InvalidProtocol),
                    Err(ParseError::Io(e)) => {
                        if e.kind() == io::ErrorKind::WouldBlock {
                            return Ok(Async::NotReady)
                        }
                        error!("io error when parsing {:?}", e);
                        return Err(DecodeError::Io(e))
                    },
                }
            },
            State::Empty => panic!("poll ReadUntil after it's done"),
        };

        match mem::replace(&mut self.state, State::Empty) {
            State::Reading(mut reader) => {
                reader.consume(consumed);
                Ok(Async::Ready((reader, resp)))
            },
            State::Empty => unreachable!(),
        }
    }
}

pub fn parse_resp(buf: &[u8]) -> Result<(Resp, usize), ParseError> {
    if buf.len() == 0 {
        return Err(ParseError::NotEnoughData);
    }

    let prefix = buf[0] as char;
    match prefix {
        '$' => {
            let (v, consumed) = parse_bulk_str(&buf[1..])?;
            Ok((Resp::Bulk(v), 1 + consumed))
        }
        '+' => {
            let (v, consumed) = parse_line(&buf[1..])?;
            Ok((Resp::Simple(v), 1 + consumed))
        }
        ':' => {
            let (v, consumed) = parse_line(&buf[1..])?;
            Ok((Resp::Integer(v), 1 + consumed))
        }
        '-' => {
            let (v, consumed) = parse_line(&buf[1..])?;
            Ok((Resp::Error(v), 1 + consumed))
        }
        '*' => {
            let (v, consumed) = parse_array(&buf[1..])?;
            Ok((Resp::Arr(v), 1 + consumed))
        }
        prefix => {
            error!("Unexpected prefix {:?}", prefix);
            Err(ParseError::InvalidProtocol)
        },
    }
}

fn parse_array(buf: &[u8]) -> Result<(Array, usize), ParseError> {
    let (len, mut consumed) = parse_len(buf)?;
    if len < 0 {
        return Ok((Array::Nil, consumed));
    }

    let array_size = len as usize;
    let mut array = Vec::with_capacity(array_size);

    for _ in 0..array_size {
        let (v, element_consumed) = parse_resp(&buf[consumed..])?;
        consumed += element_consumed;
        array.push(v);
    }

    Ok((Array::Arr(array), consumed))
}

fn parse_bulk_str(buf: &[u8]) -> Result<(BulkStr, usize), ParseError> {
    let (len, consumed) = parse_len(buf)?;
    if len < 0 {
        return Ok((BulkStr::Nil, consumed));
    }

    let content_size = len as usize;
    if buf.len() < consumed + content_size + 2 {
        return Err(ParseError::NotEnoughData);
    }

    Ok((
        BulkStr::Str(buf[consumed..consumed+content_size].to_vec()),
        consumed + content_size + 2
    ))
}

fn parse_len(buf: &[u8]) -> Result<(i64, usize), ParseError> {
    let (line, consumed) = parse_line(buf)?;
    // TODO: optimize it by not allocating the Vec
    let len = btoi(&line).map_err(|_| ParseError::InvalidProtocol)?;
    Ok((len, consumed))
}

fn parse_line(mut buf: &[u8]) -> Result<(BinSafeStr, usize), ParseError> {
    let mut line = Vec::new();
    let consumed = buf.read_until(LF, &mut line).map_err(ParseError::Io)?;
    if consumed == 0 || line[consumed-1] != LF {
        return Err(ParseError::NotEnoughData);
    }

    if consumed == 1 {
        return Err(ParseError::InvalidProtocol);
    }
    // s >= 2
    // Just ignore the CR
    line.truncate(consumed-2);
    Ok((line, consumed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str;

    #[test]
    fn test_parse_len() {
        let r = parse_len("233\r\n".as_bytes());
        assert!(r.is_ok());
        let (len, s) = r.unwrap();
        assert_eq!(s, 5);
        assert_eq!(len, 233);

        let r = parse_len("-233\r\n".as_bytes());
        assert!(r.is_ok());
        let (len, s) = r.unwrap();
        assert_eq!(s, 6);
        assert_eq!(len, -233);

        let r = parse_len("2a3\r\n".as_bytes());
        assert!(r.is_err());
    }

    #[test]
    fn test_parse_line() {
        let r = parse_line("233\r\n".as_bytes());
        assert!(r.is_ok());
        let (b, l) = r.unwrap();
        assert_eq!(l, 5);
        assert_eq!(str::from_utf8(&b), Ok("233"));

        let r = parse_line("\r\n".as_bytes());
        assert!(r.is_ok());
        let (b, l) = r.unwrap();
        assert_eq!(l, 2);
        assert_eq!(str::from_utf8(&b), Ok(""));
    }

    #[test]
    fn test_parse_bulk_str() {
        let r = parse_bulk_str("2\r\nab\r\n".as_bytes());
        assert!(r.is_ok());
        let (content, s) = r.unwrap();
        assert_eq!(s, 7);
        assert_eq!(BulkStr::Str(String::from("ab").into_bytes()), content);

        let r = parse_bulk_str("-1\r\n".as_bytes());
        assert!(r.is_ok());
        let (content, s) = r.unwrap();
        assert_eq!(s, 4);
        assert_eq!(BulkStr::Nil, content);

        let r = parse_bulk_str("2a3\r\nab\r\n".as_bytes());
        assert!(r.is_err());

        let r = parse_bulk_str("0\r\n\r\n".as_bytes());
        assert!(r.is_ok());
        let (content, s) = r.unwrap();
        assert_eq!(s, 5);
        assert_eq!(BulkStr::Str(String::from("").into_bytes()), content);

        let r = parse_bulk_str("1\r\na\r\n".as_bytes());
        assert!(r.is_ok());
        let (content, s) = r.unwrap();
        assert_eq!(s, 6);
        assert_eq!(BulkStr::Str(String::from("a").into_bytes()), content);

        let r = parse_bulk_str("2\r\na\r\n".as_bytes());
        assert!(r.is_err());

        // TODO: Support this check
        // let r = parse_bulk_str("2\r\nabc\r\n".as_bytes());
        // assert!(r.is_err());
    }

    #[test]
    fn test_parse_array() {
        let r = parse_array("2\r\n$1\r\na\r\n$2\r\nbc\r\n".as_bytes());
        assert!(r.is_ok());
        let (a, s) = r.unwrap();
        assert_eq!(s, 18);
        assert_eq!(Array::Arr(vec![
            Resp::Bulk(BulkStr::Str(String::from("a").into_bytes())),
            Resp::Bulk(BulkStr::Str(String::from("bc").into_bytes())),
        ]), a);

        let r = parse_array("-1\r\n".as_bytes());
        assert!(r.is_ok());
        let (a, s) = r.unwrap();
        assert_eq!(s, 4);
        assert_eq!(Array::Nil, a);

        let r = parse_array("0\r\n".as_bytes());
        assert!(r.is_ok());
        let (a, s) = r.unwrap();
        assert_eq!(s, 3);
        assert_eq!(Array::Arr(vec![]), a);

        let r = parse_array("1\r\n$2\r\na\r\n".as_bytes());
        assert!(r.is_err());

        // TODO: Support this check
        // let r = parse_array("1\r\n$2\r\nabc\r\n".as_bytes());
        // assert!(r.is_err());
    }

    #[test]
    fn test_parse_resp() {
        let r = parse_resp("*-1\r\n".as_bytes());
        assert!(r.is_ok());
        let (a, s) = r.unwrap();
        assert_eq!(s, 5);
        assert_eq!(Resp::Arr(Array::Nil), a);

        let r = parse_resp("*0\r\n".as_bytes());
        assert!(r.is_ok());
        let (a, s) = r.unwrap();
        assert_eq!(s, 4);
        assert_eq!(Resp::Arr(Array::Arr(vec![])), a);

        let r = parse_resp("-abc\r\n".as_bytes());
        assert!(r.is_ok());
        let (a, s) = r.unwrap();
        assert_eq!(s, 6);
        assert_eq!(Resp::Error(String::from("abc").into_bytes()), a);

        let r = parse_resp(":233\r\n".as_bytes());
        assert!(r.is_ok());
        let (a, s) = r.unwrap();
        assert_eq!(s, 6);
        assert_eq!(Resp::Integer(String::from("233").into_bytes()), a);

        let r = parse_resp("+233\r\n".as_bytes());
        assert!(r.is_ok());
        let (a, s) = r.unwrap();
        assert_eq!(s, 6);
        assert_eq!(Resp::Simple(String::from("233").into_bytes()), a);

        let r = parse_resp("$3\r\nfoo\r\n".as_bytes());
        assert!(r.is_ok());
        let (a, s) = r.unwrap();
        assert_eq!(s, 9);
        assert_eq!(Resp::Bulk(BulkStr::Str(String::from("foo").into_bytes())), a);
    }
}
