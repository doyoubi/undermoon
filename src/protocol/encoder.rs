use super::resp::{Array, BinSafeStr, BulkStr, Resp};
use std::io;

pub fn command_to_buf(buf: &mut Vec<u8>, command: Vec<BinSafeStr>) {
    let arr: Vec<Resp> = command
        .into_iter()
        .map(|s| Resp::Bulk(BulkStr::Str(s)))
        .collect();
    let resp = Resp::Arr(Array::Arr(arr));
    resp_to_buf(buf, &resp);
}

pub fn resp_to_buf(buf: &mut Vec<u8>, resp: &Resp) {
    encode_resp(buf, resp).expect("resp_to_buf"); // TODO: remove expect
}

pub fn encode_resp<W>(writer: &mut W, resp: &Resp) -> io::Result<usize>
where
    W: io::Write,
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
where
    W: io::Write,
{
    match *array {
        Array::Nil => writer.write(b"*-1\r\n"),
        Array::Arr(ref arr) => {
            let mut l = encode_simple_element(writer, b"*", &arr.len().to_string().into_bytes())?;
            for element in arr {
                l += encode_resp(writer, element)?;
            }
            Ok(l)
        }
    }
}

fn encode_bulk_str<W>(writer: &mut W, bulk_str: &BulkStr) -> io::Result<usize>
where
    W: io::Write,
{
    match *bulk_str {
        BulkStr::Nil => writer.write(b"$-1\r\n"),
        BulkStr::Str(ref s) => Ok(encode_simple_element(
            writer,
            b"$",
            &s.len().to_string().into_bytes(),
        )? + writer.write(s)?
            + writer.write(b"\r\n")?),
    }
}

fn encode_simple_element<W>(writer: &mut W, prefix: &[u8], s: &[u8]) -> io::Result<usize>
where
    W: io::Write,
{
    Ok(writer.write(prefix)? + writer.write(s)? + writer.write(b"\r\n")?)
}
