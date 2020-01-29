use std::error::Error;
use std::fmt;
use std::io;

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

    fn cause(&self) -> Option<&dyn Error> {
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

pub const LF: u8 = b'\n';
