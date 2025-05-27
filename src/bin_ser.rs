use bytes::{Buf, BufMut};

pub trait BinarySerialize {
    fn serialize(&self, buf: &mut impl BufMut);
}

pub trait BinaryDeserialize {
    fn deserialize(&self, buf: &impl Buf) -> Self;
}
