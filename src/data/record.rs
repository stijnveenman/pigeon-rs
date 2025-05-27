use bytes::Bytes;

use crate::bin_ser::BinarySerialize;

pub struct Record {
    // TODO add timestamp
    offset: u64,
    key: Bytes,
    value: Bytes,
    headers: Vec<(String, Bytes)>,
}

impl BinarySerialize for Record {
    fn serialize(&self, buf: &mut impl bytes::BufMut) {
        buf.put_u64_ne(self.offset);

        self.key.serialize(buf);
        self.value.serialize(buf);

        self.headers.serialize(buf);
    }
}
