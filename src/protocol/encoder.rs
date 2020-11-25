use super::resp::{Array, BinSafeStr, BulkStr, Resp, RespVec};
use std::error::Error;
use std::fmt;
use std::io;

pub fn command_to_buf(buf: &mut Vec<u8>, command: Vec<BinSafeStr>) -> io::Result<usize> {
    let arr: Vec<RespVec> = command
        .into_iter()
        .map(|s| Resp::Bulk(BulkStr::Str(s)))
        .collect();
    let resp = Resp::Arr(Array::Arr(arr));
    resp_to_buf(buf, &resp)
}

pub fn resp_to_buf(buf: &mut Vec<u8>, resp: &RespVec) -> io::Result<usize> {
    encode_resp(buf, resp)
}

pub fn encode_resp<W, T: AsRef<[u8]>>(writer: &mut W, resp: &Resp<T>) -> io::Result<usize>
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

fn encode_array<W, T: AsRef<[u8]>>(writer: &mut W, array: &Array<T>) -> io::Result<usize>
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

fn encode_bulk_str<W, T: AsRef<[u8]>>(writer: &mut W, bulk_str: &BulkStr<T>) -> io::Result<usize>
where
    W: io::Write,
{
    match *bulk_str {
        BulkStr::Nil => writer.write(b"$-1\r\n"),
        BulkStr::Str(ref s) => Ok(encode_simple_element(
            writer,
            b"$",
            &s.as_ref().len().to_string().into_bytes(),
        )? + writer.write(s.as_ref())?
            + writer.write(b"\r\n")?),
    }
}

fn encode_simple_element<W, T: AsRef<[u8]>>(
    writer: &mut W,
    prefix: &[u8],
    b: T,
) -> io::Result<usize>
where
    W: io::Write,
{
    Ok(writer.write(prefix)? + writer.write(b.as_ref())? + writer.write(b"\r\n")?)
}

#[derive(Debug)]
pub enum EncodeError<T> {
    NotReady(T),
    Io(io::Error),
}

impl<T> fmt::Display for EncodeError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            Self::NotReady(_) => "EncodeError::NotReady".to_string(),
            Self::Io(err) => format!("EncodeError::Io({})", err),
        };
        write!(f, "{}", s)
    }
}

impl<T: fmt::Debug> Error for EncodeError<T> {
    fn description(&self) -> &str {
        "decode error"
    }

    fn cause(&self) -> Option<&dyn Error> {
        match self {
            EncodeError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl<T> From<io::Error> for EncodeError<T> {
    fn from(e: io::Error) -> Self {
        EncodeError::Io(e)
    }
}

pub fn get_resp_size_hint<T: AsRef<[u8]>>(resp: &Resp<T>) -> io::Result<usize> {
    let mut writer = SizeHintWriter {};
    encode_resp(&mut writer, resp)
}

struct SizeHintWriter {}

impl io::Write for SizeHintWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
