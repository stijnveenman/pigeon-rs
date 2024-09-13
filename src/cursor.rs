use bytes::Buf;
use std::fmt;
use std::io::Cursor;

#[derive(Debug)]
pub enum Error {
    /// Not enough data is available to parse a message
    Incomplete,

    /// Invalid message encoding
    Other(crate::Error),
}

pub fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u8())
}

pub fn get_i16(src: &mut Cursor<&[u8]>) -> Result<i16, Error> {
    if src.remaining() < 2 {
        return Err(Error::Incomplete);
    }

    Ok(src.get_i16())
}

pub fn get_i32(src: &mut Cursor<&[u8]>) -> Result<isize, Error> {
    if src.remaining() < 4 {
        return Err(Error::Incomplete);
    }

    Ok(src.get_i32() as isize)
}

pub fn get_u32(src: &mut Cursor<&[u8]>) -> Result<usize, Error> {
    if src.remaining() < 4 {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u32() as usize)
}

pub fn get_u16(src: &mut Cursor<&[u8]>) -> Result<usize, Error> {
    if src.remaining() < 2 {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u16() as usize)
}

pub fn get_string(src: &mut Cursor<&[u8]>) -> Result<String, Error> {
    let len = get_u16(src)?;

    let start = src.position() as usize;
    let end = start + len;
    let buf = &src.get_ref()[start..end];

    String::from_utf8(buf.to_vec()).map_err(|e| e.to_string().into())
}

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}
