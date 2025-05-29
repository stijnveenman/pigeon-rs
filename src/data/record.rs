use bytes::Bytes;

use crate::bin_ser::{BinaryDeserialize, BinarySerialize};

#[derive(Debug)]
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

#[cfg(test)]
mod test {
    use fake::{Dummy, Fake, Faker};

    use super::Record;

    impl Dummy<Faker> for Record {
        fn dummy_with_rng<R: fake::Rng + ?Sized>(_config: &Faker, rng: &mut R) -> Self {
            Record {
                offset: Faker.fake_with_rng(rng),
                key: Faker.fake_with_rng::<String, _>(rng).into(),
                value: Faker.fake_with_rng::<String, _>(rng).into(),
                headers: (0..rng.random_range(1..10))
                    .map(|_| {
                        (
                            Faker.fake_with_rng::<String, _>(rng),
                            Faker.fake_with_rng::<String, _>(rng).into(),
                        )
                    })
                    .collect(),
            }
        }
    }

    #[test]
    fn test_serialize_and_deserialize() {
        let record: Record = Faker.fake();

        println!("{:?}", record);
        // assert_eq!(record.offset, 2);
    }
}
