use fake::Dummy;

use crate::bin_ser::{BinaryDeserialize, BinarySerialize, DynamicBinarySize, StaticBinarySize};

use super::record::Record;

#[derive(Debug, PartialEq, Eq, Dummy)]
pub struct RecordSetHeader {
    pub length: u32,
    pub start_offset: u64,
    pub end_offset: u64,
    pub crc: u32,
    pub record_count: u32,
}

impl RecordSetHeader {
    #![allow(dead_code)]
    pub fn empty() -> Self {
        Self {
            length: 0,
            start_offset: 0,
            end_offset: 0,
            crc: 0,
            record_count: 0,
        }
    }

    pub fn for_records(records: &[Record]) -> Self {
        let mut length = 0;
        let mut start_offset = u64::MAX;
        let mut end_offset = 0;

        for record in records {
            length += record.binary_size() as u32;
            start_offset = start_offset.min(record.offset);
            end_offset = end_offset.max(record.offset)
        }

        Self {
            length,
            start_offset,
            end_offset,
            // TODO implement actual crc
            crc: 0,
            record_count: records.len() as u32,
        }
    }
}

impl StaticBinarySize for RecordSetHeader {
    fn binary_size() -> usize {
        4 + 8 + 8 + 4 + 4
    }
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

    use crate::bin_ser::{BinaryDeserialize, BinarySerialize, DynamicBinarySize};

    use super::RecordSetHeader;

    #[test]
    fn test_serialize_and_deserialize() {
        let rng = &mut StdRng::seed_from_u64(1023489710234894);

        for _ in 0..100 {
            let record: RecordSetHeader = Faker.fake_with_rng(rng);

            let mut v = vec![];
            // TODO add serialize buf function
            record.serialize(&mut v);

            assert_eq!(record.binary_size(), v.len());

            let result = RecordSetHeader::deserialize(&mut Bytes::from(v))
                .expect("failed to deserialize buf");
            assert_eq!(record, result);
        }
    }
}
