use bytes::Bytes;

use crate::bin_ser::{BinaryDeserialize, BinarySerialize, DynamicBinarySize};

use super::timestamp::Timestamp;

#[derive(Debug, PartialEq, Eq)]
pub struct RecordHeader {
    pub key: String,
    pub value: Bytes,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Record {
    pub offset: u64,
    pub timestamp: Timestamp,
    pub key: Bytes,
    pub value: Bytes,
    pub headers: Vec<RecordHeader>,
}

impl DynamicBinarySize for RecordHeader {
    fn binary_size(&self) -> usize {
        self.key.binary_size() + self.value.binary_size()
    }
}

impl BinarySerialize for RecordHeader {
    fn serialize(&self, buf: &mut impl bytes::BufMut) {
        self.key.serialize(buf);
        self.value.serialize(buf);
    }
}

impl BinaryDeserialize for RecordHeader {
    fn deserialize(buf: &mut impl bytes::Buf) -> Result<Self, crate::bin_ser::DeserializeError> {
        let key = String::deserialize(buf)?;
        let value = Bytes::deserialize(buf)?;

        Ok(Self { key, value })
    }
}

impl DynamicBinarySize for Record {
    fn binary_size(&self) -> usize {
        8 + self.timestamp.binary_size()
            + self.key.binary_size()
            + self.value.binary_size()
            + self.headers.binary_size()
    }
}

impl BinarySerialize for Record {
    fn serialize(&self, buf: &mut impl bytes::BufMut) {
        buf.put_u64(self.offset);
        self.timestamp.serialize(buf);

        self.key.serialize(buf);
        self.value.serialize(buf);

        self.headers.serialize(buf);
    }
}

impl BinaryDeserialize for Record {
    fn deserialize(buf: &mut impl bytes::Buf) -> Result<Self, crate::bin_ser::DeserializeError> {
        let offset = buf.try_get_u64()?;
        let timestamp = Timestamp::deserialize(buf)?;

        let key = Bytes::deserialize(buf)?;
        let value = Bytes::deserialize(buf)?;

        let headers = Vec::<RecordHeader>::deserialize(buf)?;

        Ok(Self {
            offset,
            timestamp,
            key,
            value,
            headers,
        })
    }
}
