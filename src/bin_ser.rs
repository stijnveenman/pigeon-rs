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

impl<B1, B2> BinarySerialize for (B1, B2)
where
    B1: BinarySerialize,
    B2: BinarySerialize,
{
    fn serialize(&self, buf: &mut impl BufMut) {
        self.0.serialize(buf);
        self.1.serialize(buf);
    }
}

impl<B> BinarySerialize for &[B]
where
    B: BinarySerialize,
{
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
    fn serialize(&self, buf: &mut impl BufMut) {
        self.as_slice().serialize(buf);
    }
}
