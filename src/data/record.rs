use bytes::Bytes;

use crate::bin_ser::{BinaryDeserialize, BinarySerialize};

pub struct Record {
    // TODO add timestamp
    offset: u64,
    key: Bytes,
    value: Bytes,
    headers: Vec<(String, Bytes)>,
}

impl BinarySerialize for Record {
    fn serialize(&self, buf: &mut impl bytes::BufMut) {
        buf.put_u64(self.offset);

        self.key.serialize(buf);
        self.value.serialize(buf);

        self.headers.serialize(buf);
    }
}

impl BinaryDeserialize for Record {
    fn deserialize(buf: &mut impl bytes::Buf) -> Result<Self, crate::bin_ser::DeserializeError> {
        let offset = buf.try_get_u64()?;

        let key = Bytes::deserialize(buf)?;
        let value = Bytes::deserialize(buf)?;

        let headers = Vec::<(String, Bytes)>::deserialize(buf)?;

        Ok(Self {
            offset,
            key,
            value,
            headers,
        })
    }
}
