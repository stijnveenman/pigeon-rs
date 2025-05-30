use std::string;

use bytes::{Buf, BufMut, Bytes};
use thiserror::Error;

pub trait BinarySerialize {
    fn binary_size(&self) -> usize;
    fn serialize(&self, buf: &mut impl BufMut);
}

pub trait BinaryDeserialize: Sized {
    fn deserialize(buf: &mut impl Buf) -> Result<Self, DeserializeError>;
}

#[derive(Error, Debug)]
pub enum DeserializeError {
    #[error("Not enough bytes to get next item")]
    TryGet(#[from] bytes::TryGetError),
    #[error("Not enough bytes remaining")]
    NotEnoughBytes,
    #[error("Unable to read string as utf8 string")]
    FromUtf8(#[from] string::FromUtf8Error),
}

impl BinarySerialize for Bytes {
    fn binary_size(&self) -> usize {
        4 + self.len()
    }

    fn serialize(&self, buf: &mut impl BufMut) {
        buf.put_u32(self.len() as u32);
        buf.put(self.clone());
    }
}

impl BinarySerialize for String {
    fn binary_size(&self) -> usize {
        4 + self.len()
    }

    fn serialize(&self, buf: &mut impl BufMut) {
        buf.put_u32(self.len() as u32);
        buf.put(self.as_bytes());
    }
}

impl<B1, B2> BinarySerialize for (B1, B2)
where
    B1: BinarySerialize,
    B2: BinarySerialize,
{
    fn binary_size(&self) -> usize {
        self.0.binary_size() + self.1.binary_size()
    }

    fn serialize(&self, buf: &mut impl BufMut) {
        self.0.serialize(buf);
        self.1.serialize(buf);
    }
}

impl<B> BinarySerialize for &[B]
where
    B: BinarySerialize,
{
    fn binary_size(&self) -> usize {
        4 + self.iter().map(|s| s.binary_size()).sum::<usize>()
    }

    fn serialize(&self, buf: &mut impl BufMut) {
        buf.put_u32(self.len() as u32);
        for b in self.iter() {
            b.serialize(buf);
        }
    }
}

impl<B> BinarySerialize for Vec<B>
where
    B: BinarySerialize,
{
    fn binary_size(&self) -> usize {
        self.as_slice().binary_size()
    }

    fn serialize(&self, buf: &mut impl BufMut) {
        self.as_slice().serialize(buf);
    }
}

impl BinaryDeserialize for Bytes {
    fn deserialize(buf: &mut impl Buf) -> Result<Self, DeserializeError> {
        let length = buf.try_get_u32()? as usize;

        if length > buf.remaining() {
            return Err(DeserializeError::NotEnoughBytes);
        }

        let bytes = buf.copy_to_bytes(length);

        Ok(bytes)
    }
}

impl<B> BinaryDeserialize for Vec<B>
where
    B: BinaryDeserialize,
{
    fn deserialize(buf: &mut impl Buf) -> Result<Self, DeserializeError> {
        let length = buf.try_get_u32()? as usize;

        let mut v = Vec::with_capacity(length);
        for _ in 0..length {
            v.push(B::deserialize(buf)?);
        }

        Ok(v)
    }
}

impl<B1, B2> BinaryDeserialize for (B1, B2)
where
    B1: BinaryDeserialize,
    B2: BinaryDeserialize,
{
    fn deserialize(buf: &mut impl Buf) -> Result<Self, DeserializeError> {
        let l = B1::deserialize(buf)?;
        let r = B2::deserialize(buf)?;

        Ok((l, r))
    }
}

impl BinaryDeserialize for String {
    fn deserialize(buf: &mut impl Buf) -> Result<Self, DeserializeError> {
        let length = buf.try_get_u32()? as usize;

        if length > buf.remaining() {
            return Err(DeserializeError::NotEnoughBytes);
        }

        let mut v = Vec::with_capacity(length);
        v.put(buf.take(length));

        let string = String::from_utf8(v)?;

        Ok(string)
    }
}
