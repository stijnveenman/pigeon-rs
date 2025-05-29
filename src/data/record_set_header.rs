use fake::Dummy;

use crate::bin_ser::{BinaryDeserialize, BinarySerialize};

#[derive(Debug, PartialEq, Eq, Dummy)]
pub struct RecordSetHeader {
    length: u32,
    start_offset: u64,
    end_offset: u64,
    crc: u32,
    record_count: u32,
}

impl BinarySerialize for RecordSetHeader {
    fn serialize(&self, buf: &mut impl bytes::BufMut) {
        buf.put_u32(self.length);
        buf.put_u64(self.start_offset);
        buf.put_u64(self.end_offset);
        buf.put_u32(self.crc);
        buf.put_u32(self.record_count);
    }
}

impl BinaryDeserialize for RecordSetHeader {
    fn deserialize(buf: &mut impl bytes::Buf) -> Result<Self, crate::bin_ser::DeserializeError> {
        let length = buf.try_get_u32()?;
        let start_offset = buf.try_get_u64()?;
        let end_offset = buf.try_get_u64()?;
        let crc = buf.try_get_u32()?;
        let record_count = buf.try_get_u32()?;

        Ok(Self {
            length,
            start_offset,
            end_offset,
            crc,
            record_count,
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

    use super::RecordSetHeader;

    #[test]
    fn test_serialize_and_deserialize() {
        let rng = &mut StdRng::seed_from_u64(1023489710234894);

        for _ in 0..100 {
            let record: RecordSetHeader = Faker.fake_with_rng(rng);

            let mut v = vec![];
            // TODO add serialize buf function
            record.serialize(&mut v);

            let result = RecordSetHeader::deserialize(&mut Bytes::from(v))
                .expect("failed to deserialize buf");
            assert_eq!(record, result);
        }
    }
}
