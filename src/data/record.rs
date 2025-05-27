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

        // TODO Vec, Tuple
        buf.put_u32(self.headers.len() as u32);
        for header in &self.headers {
            header.serialize(buf);
        }
    }
}
