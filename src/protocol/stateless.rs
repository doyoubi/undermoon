use super::decoder::LF;
use super::resp::{AdvanceIndex, ArrayIndex, BulkStrIndex, DataIndex, IndexedResp, RespIndex};
use btoi::btoi;
use bytes::BytesMut;
use memchr::memchr;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum ParseError {
    InvalidProtocol,
    NotEnoughData,
    UnexpectedErr,
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

    fn cause(&self) -> Option<&dyn Error> {
        None
    }
}

pub fn parse_indexed_resp(buf: &mut BytesMut) -> Result<IndexedResp, ParseError> {
    let (resp, consumed) = parse_resp(buf)?;
    let data = buf.split_to(consumed).freeze();
    Ok(IndexedResp::new(resp, data))
}

pub fn parse_resp(buf: &[u8]) -> Result<(RespIndex, usize), ParseError> {
    if buf.is_empty() {
        return Err(ParseError::NotEnoughData);
    }

    let prefix = *buf.get(0).ok_or(ParseError::UnexpectedErr)?;
    let next_buf = buf.get(1..).ok_or(ParseError::InvalidProtocol)?;

    match prefix {
        b'$' => {
            let (mut v, consumed) = parse_bulk_str(next_buf)?;
            v.advance(1);
            Ok((RespIndex::Bulk(v), 1 + consumed))
        }
        b'+' => {
            let (mut v, consumed) = parse_line(next_buf)?;
            v.advance(1);
            Ok((RespIndex::Simple(v), 1 + consumed))
        }
        b':' => {
            let (mut v, consumed) = parse_line(next_buf)?;
            v.advance(1);
            Ok((RespIndex::Integer(v), 1 + consumed))
        }
        b'-' => {
            let (mut v, consumed) = parse_line(next_buf)?;
            v.advance(1);
            Ok((RespIndex::Error(v), 1 + consumed))
        }
        b'*' => {
            let (mut v, consumed) = parse_array(next_buf)?;
            v.advance(1);
            Ok((RespIndex::Arr(v), 1 + consumed))
        }
        prefix => {
            debug!("invalid prefix {:?}", prefix);
            Err(ParseError::InvalidProtocol)
        }
    }
}

fn parse_array(buf: &[u8]) -> Result<(ArrayIndex, usize), ParseError> {
    let (len, mut consumed) = parse_len(buf)?;
    if len < 0 {
        return Ok((ArrayIndex::Nil, consumed));
    }

    let array_size = len as usize;
    let mut array = Vec::with_capacity(array_size);

    for _ in 0..array_size {
        let next_buf = buf.get(consumed..).ok_or(ParseError::InvalidProtocol)?;
        let (mut v, element_consumed) = parse_resp(next_buf)?;
        v.advance(consumed);
        consumed += element_consumed;
        array.push(v);
    }

    Ok((ArrayIndex::Arr(array), consumed))
}

fn parse_bulk_str(buf: &[u8]) -> Result<(BulkStrIndex, usize), ParseError> {
    let (len, consumed) = parse_len(buf)?;
    if len < 0 {
        return Ok((BulkStrIndex::Nil, consumed));
    }

    let content_size = len as usize;
    if buf.len() < consumed + content_size + 2 {
        return Err(ParseError::NotEnoughData);
    }

    let s = DataIndex(consumed, consumed + content_size);
    Ok((BulkStrIndex::Str(s), consumed + content_size + 2))
}

fn parse_len(buf: &[u8]) -> Result<(i64, usize), ParseError> {
    let (data_index, consumed) = parse_line(buf)?;
    let next_buf = buf
        .get(data_index.to_range())
        .ok_or(ParseError::UnexpectedErr)?;

    let len = btoi(next_buf).map_err(|_| ParseError::InvalidProtocol)?;
    Ok((len, consumed))
}

fn parse_line(buf: &[u8]) -> Result<(DataIndex, usize), ParseError> {
    let lf_index = memchr(LF, buf).ok_or(ParseError::NotEnoughData)?;
    if lf_index == 0 {
        return Err(ParseError::InvalidProtocol);
    }

    // s >= 2
    // Just ignore the CR
    let line = DataIndex(0, lf_index + 1 - 2);
    Ok((line, lf_index + 1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::resp::{ArraySlice, BulkStrSlice, RespSlice};

    #[test]
    fn test_parse_len_bytes() {
        let r = parse_len(b"233\r\n");
        assert!(r.is_ok());
        let (len, s) = r.unwrap();
        assert_eq!(s, 5);
        assert_eq!(len, 233);

        let r = parse_len(b"-233\r\n");
        assert!(r.is_ok());
        let (len, s) = r.unwrap();
        assert_eq!(s, 6);
        assert_eq!(len, -233);

        let r = parse_len(b"2a3\r\n");
        assert!(r.is_err());
    }

    #[test]
    fn test_parse_line_bytes() {
        let data = b"233\r\n";
        let r = parse_line(data);
        assert!(r.is_ok());
        let (b, l) = r.unwrap();
        assert_eq!(l, 5);
        assert_eq!(&data[b.to_range()], b"233");

        let data = b"\r\n";
        let r = parse_line(data);
        assert!(r.is_ok());
        let (b, l) = r.unwrap();
        assert_eq!(l, 2);
        assert_eq!(&data[b.to_range()], b"".as_ref());
    }

    #[test]
    fn test_parse_bulk_str_bytes() {
        let data = b"2\r\nab\r\n";
        let r = parse_bulk_str(data);
        assert!(r.is_ok());
        let (content, s) = r.unwrap();
        assert_eq!(s, 7);
        assert_eq!(
            Some(b"ab".as_ref()),
            content.try_to_range().map(|r| &data[r])
        );

        let r = parse_bulk_str(b"-1\r\n");
        assert!(r.is_ok());
        let (content, s) = r.unwrap();
        assert_eq!(s, 4);
        assert_eq!(BulkStrIndex::Nil, content);

        let r = parse_bulk_str(b"2a3\r\nab\r\n");
        assert!(r.is_err());

        let r = parse_bulk_str(b"0\r\n\r\n");
        assert!(r.is_ok());
        let (content, s) = r.unwrap();
        assert_eq!(s, 5);
        assert_eq!(Some(b"".as_ref()), content.try_to_range().map(|r| &data[r]));

        let r = parse_bulk_str(b"1\r\na\r\n");
        assert!(r.is_ok());
        let (content, s) = r.unwrap();
        assert_eq!(s, 6);
        assert_eq!(
            Some(b"a".as_ref()),
            content.try_to_range().map(|r| &data[r])
        );

        let r = parse_bulk_str(b"2\r\na\r\n");
        assert!(r.is_err());

        // TODO: Support this check
        // let r = parse_bulk_str("2\r\nabc\r\n".as_bytes());
        // assert!(r.is_err());
    }

    #[test]
    fn test_parse_array_bytes() {
        let data = b"2\r\n$1\r\na\r\n$2\r\nbc\r\n";
        let r = parse_array(data);
        assert!(r.is_ok());
        let (a, s) = r.unwrap();
        assert_eq!(s, 18);
        let arr = a.map_to_slice(data);
        assert_eq!(
            ArraySlice::Arr(vec![
                RespSlice::Bulk(BulkStrSlice::Str(b"a")),
                RespSlice::Bulk(BulkStrSlice::Str(b"bc")),
            ]),
            arr
        );

        let r = parse_array(b"-1\r\n");
        assert!(r.is_ok());
        let (a, s) = r.unwrap();
        assert_eq!(s, 4);
        assert_eq!(ArrayIndex::Nil, a);

        let r = parse_array(b"0\r\n");
        assert!(r.is_ok());
        let (a, s) = r.unwrap();
        assert_eq!(s, 3);
        assert_eq!(ArrayIndex::Arr(vec![]), a);

        let r = parse_array(b"1\r\n$2\r\na\r\n");
        assert!(r.is_err());

        // TODO: Support this check
        // let r = parse_array("1\r\n$2\r\nabc\r\n".as_bytes());
        // assert!(r.is_err());
    }

    #[test]
    fn test_parse_resp_bytes() {
        let r = parse_resp(b"*-1\r\n");
        assert!(r.is_ok());
        let (a, s) = r.unwrap();
        assert_eq!(s, 5);
        assert_eq!(RespIndex::Arr(ArrayIndex::Nil), a);

        let r = parse_resp(b"*0\r\n");
        assert!(r.is_ok());
        let (a, s) = r.unwrap();
        assert_eq!(s, 4);
        assert_eq!(RespIndex::Arr(ArrayIndex::Arr(vec![])), a);

        let data = b"-abc\r\n";
        let r = parse_resp(data);
        assert!(r.is_ok());
        let (a, s) = r.unwrap();
        assert_eq!(s, 6);
        assert_eq!(RespSlice::Error(b"abc"), a.map_to_slice(data));

        let data = b":233\r\n";
        let r = parse_resp(data);
        assert!(r.is_ok());
        let (a, s) = r.unwrap();
        assert_eq!(s, 6);
        assert_eq!(RespSlice::Integer(b"233"), a.map_to_slice(data));

        let data = b"+233\r\n";
        let r = parse_resp(data);
        assert!(r.is_ok());
        let (a, s) = r.unwrap();
        assert_eq!(s, 6);
        assert_eq!(RespSlice::Simple(b"233"), a.map_to_slice(data));

        let data = b"$3\r\nfoo\r\n";
        let r = parse_resp(data);
        assert!(r.is_ok());
        let (a, s) = r.unwrap();
        assert_eq!(s, 9);
        assert_eq!(
            RespSlice::Bulk(BulkStrSlice::Str(b"foo")),
            a.map_to_slice(data),
        );
    }
}
