use bytes::{Buf, BufMut, Bytes};

pub trait BinarySerialize {
    fn serialize(&self, buf: &mut impl BufMut);
}

pub trait BinaryDeserialize {
    fn deserialize(&self, buf: &impl Buf) -> Self;
}

impl BinarySerialize for Bytes {
    fn serialize(&self, buf: &mut impl BufMut) {
        buf.put_u32(self.len() as u32);
        buf.put(self.clone());
    }
}

impl BinarySerialize for String {
    fn serialize(&self, buf: &mut impl BufMut) {
        buf.put_u32(self.len() as u32);
        buf.put(self.as_bytes());
    }
}
