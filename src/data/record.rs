use bytes::Bytes;
use fake::Dummy;

use crate::bin_ser::{BinaryDeserialize, BinarySerialize};
use crate::fake::BytesFake;

use super::timestamp::Timestamp;

#[derive(Debug, PartialEq, Eq, Dummy)]
pub struct RecordHeader {
    key: String,
    #[dummy(faker = "BytesFake")]
    value: Bytes,
}

#[derive(Debug, PartialEq, Eq, Dummy)]
pub struct Record {
    offset: u64,
    timestamp: Timestamp,
    #[dummy(faker = "BytesFake")]
    key: Bytes,
    #[dummy(faker = "BytesFake")]
    value: Bytes,
    headers: Vec<RecordHeader>,
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

    use crate::bin_ser::{BinaryDeserialize, BinarySerialize};

    use super::Record;

    #[test]
    fn test_serialize_and_deserialize() {
        let rng = &mut StdRng::seed_from_u64(1023489710234894);

        for _ in 0..100 {
            let record: Record = Faker.fake_with_rng(rng);

            let mut v = vec![];
            // TODO add serialize buf function
            record.serialize(&mut v);

            let result =
                Record::deserialize(&mut Bytes::from(v)).expect("failed to deserialize buf");
            assert_eq!(record, result);
        }
    }
}
