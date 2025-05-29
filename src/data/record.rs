use bytes::Bytes;
use fake::{Dummy, Fake, Faker};

use crate::bin_ser::{BinaryDeserialize, BinarySerialize};

use super::timestamp::Timestamp;

struct BytesFake;

impl Dummy<BytesFake> for Bytes {
    fn dummy_with_rng<R: fake::Rng + ?Sized>(_config: &BytesFake, rng: &mut R) -> Self {
        let len: usize = (10..50).fake_with_rng(rng);
        let data: Vec<u8> = (0..len).map(|_| Faker.fake_with_rng(rng)).collect();

        data.into()
    }
}

#[derive(Debug, PartialEq, Eq, Dummy)]
pub struct Record {
    offset: u64,
    timestamp: Timestamp,
    #[dummy(faker = "BytesFake")]
    key: Bytes,
    #[dummy(faker = "BytesFake")]
    value: Bytes,
    #[dummy(expr = "Vec::new()")]
    headers: Vec<(String, Bytes)>,
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

        let headers = Vec::<(String, Bytes)>::deserialize(buf)?;

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
