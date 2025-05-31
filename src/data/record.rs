use bytes::Bytes;
use fake::Dummy;

use crate::bin_ser::{BinaryDeserialize, BinarySerialize, DynamicBinarySize};
use crate::fake::BytesFake;

use super::timestamp::Timestamp;

#[derive(Debug, PartialEq, Eq, Dummy)]
pub struct RecordHeader {
    pub key: String,
    #[dummy(faker = "BytesFake")]
    pub value: Bytes,
}

#[derive(Debug, PartialEq, Eq, Dummy)]
pub struct Record {
    pub offset: u64,
    pub timestamp: Timestamp,
    #[dummy(faker = "BytesFake")]
    pub key: Bytes,
    #[dummy(faker = "BytesFake")]
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

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use fake::{
        rand::{rngs::StdRng, SeedableRng},
        Fake, Faker,
    };

    use crate::bin_ser::{BinaryDeserialize, BinarySerialize, DynamicBinarySize};

    use super::Record;

    // TODO: binary ser easy test suite for dummy tested
    #[test]
    fn test_serialize_and_deserialize() {
        let rng = &mut StdRng::seed_from_u64(1023489710234894);

        for _ in 0..100 {
            let record: Record = Faker.fake_with_rng(rng);

            let mut v = vec![];
            // TODO: add serialize buf function
            record.serialize(&mut v);
            assert_eq!(
                record.binary_size(),
                v.len(),
                "expected a binary_size of {}, got a buffer with len {}",
                record.binary_size(),
                v.len()
            );

            let result =
                Record::deserialize(&mut Bytes::from(v)).expect("failed to deserialize buf");
            assert_eq!(record, result);
        }
    }
}
