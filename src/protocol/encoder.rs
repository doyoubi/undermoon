use std::io;
use std::io::Write;
use super::resp::{Resp, BulkStr, BinSafeStr, Array};

pub fn resp_to_buf(buf: &mut Vec<u8>, resp: &Resp) {
    encode_resp(buf, resp).unwrap();
}

pub fn encode_resp<W>(writer: &mut W, resp: &Resp) -> io::Result<usize>
    where W: io::Write
{
    match resp {
        Resp::Error(s) => encode_simple_element(writer, b"-", s),
        Resp::Simple(s) => encode_simple_element(writer, b"+", s),
        Resp::Integer(s) => encode_simple_element(writer, b":", s),
        Resp::Bulk(bulk) => encode_bulk_str(writer, bulk),
        Resp::Arr(array) => encode_array(writer, array),
    }
}

fn encode_array<W>(writer: &mut W, array: &Array) -> io::Result<usize>
    where W: io::Write
{
    match array {
        &Array::Nil => writer.write(b"*-1\r\n"),
        &Array::Arr(ref arr) => {
            encode_simple_element(writer, b"*", &arr.len().to_string().into_bytes())?;
            let mut l = 0;
            for element in arr {
                l += encode_resp(writer, element)?;
            }
            Ok(l)
        }
    }
}

fn encode_bulk_str<W>(writer: &mut W, bulk_str: &BulkStr) -> io::Result<usize>
    where W: io::Write
{
    match bulk_str {
        &BulkStr::Nil => writer.write(b"$-1\r\n"),
        &BulkStr::Str(ref s) => {
            encode_simple_element(writer, b"$", &s.len().to_string().into_bytes())?;
            writer.write(s)?;
            writer.write(b"\r\n")
        },
    }
}

fn encode_simple_element<W>(writer: &mut W, prefix: &[u8], s: &BinSafeStr) -> io::Result<usize>
    where W: io::Write
{
    writer.write(prefix)?;
    writer.write(s)?;
    writer.write(b"\r\n")
}
