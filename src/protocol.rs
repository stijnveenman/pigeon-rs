use bytes::Buf;
use std::fmt;
use std::io::Cursor;
use tokio::{
    io::{self, AsyncWriteExt, BufWriter},
    net::TcpStream,
};
use tracing::instrument::WithSubscriber;

#[derive(Debug)]
pub enum Error {
    /// Not enough data is available to parse a message
    Incomplete,

    /// Invalid message encoding
    Other(crate::Error),
}

#[allow(async_fn_in_trait)]
pub trait Framing {
    fn check(src: &mut Cursor<&[u8]>, api_version: i16) -> Result<(), Error>;
    fn parse(src: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self, Error>
    where
        Self: Sized;
    async fn write_to(&self, dst: &mut BufWriter<TcpStream>, api_version: i16) -> io::Result<()>;
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

impl<T: Framing> Framing for Vec<T> {
    fn check(src: &mut Cursor<&[u8]>, api_version: i16) -> Result<(), Error> {
        let len = get_u32(src)?;

        for _ in 0..len {
            T::parse(src, api_version)?;
        }

        Ok(())
    }

    fn parse(src: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let len = get_u32(src)?;

        let mut v = Vec::with_capacity(len);

        for _ in 0..len {
            v.push(T::parse(src, api_version)?);
        }

        Ok(v)
    }

    async fn write_to(&self, dst: &mut BufWriter<TcpStream>, api_version: i16) -> io::Result<()> {
        dst.write_u32(self.len() as u32).await?;

        for i in self {
            i.write_to(dst, api_version).await?;
        }

        Ok(())
    }
}

impl Framing for String {
    fn check(src: &mut Cursor<&[u8]>, _api_version: i16) -> Result<(), Error> {
        let len = get_u16(src)?;

        if src.remaining() < len {
            return Err(Error::Incomplete);
        }

        src.advance(len);

        Ok(())
    }

    fn parse(src: &mut Cursor<&[u8]>, _api_version: i16) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let len = get_u16(src)?;

        if src.remaining() < len {
            return Err(Error::Incomplete);
        }

        let start = src.position() as usize;
        let end = start + len;
        let buf = &src.get_ref()[start..end];

        src.advance(len);

        String::from_utf8(buf.to_vec()).map_err(|e| e.to_string().into())
    }

    async fn write_to(
        &self,
        dst: &mut tokio::io::BufWriter<tokio::net::TcpStream>,
        _api_version: i16,
    ) -> std::io::Result<()> {
        dst.write_u16(self.len() as u16).await?;
        dst.write_all(self.as_bytes()).await?;

        Ok(())
    }
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
