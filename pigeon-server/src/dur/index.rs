use std::collections::BTreeMap;

use bytes::Buf;
use pigeon_core::PError;

#[derive(Default, Debug)]
pub struct Index(BTreeMap<u64, u64>);

impl Index {
    pub fn insert(&mut self, offset: u64, byte_offset: u64) -> Result<(), PError> {
        if self.0.last_entry().is_some_and(|v| offset <= *v.key()) {
            return Err(PError::IndexOffsetNotAllowed);
        }

        self.0.insert(offset, byte_offset);

        Ok(())
    }

    pub fn get(&self, offset: u64) -> Option<&u64> {
        self.0.get(&offset)
    }
}

impl TryFrom<&[u8]> for Index {
    type Error = PError;

    fn try_from(mut value: &[u8]) -> Result<Self, Self::Error> {
        let mut index = Index::default();
        while !value.is_empty() {
            let offset = value.try_get_u64().map_err(|_| PError::IndexParseFailed)?;
            let byte_offset = value.try_get_u64().map_err(|_| PError::IndexParseFailed)?;

            index
                .insert(offset, byte_offset)
                .map_err(|_| PError::IndexParseFailed)?;
        }

        Ok(index)
    }
}

#[cfg(test)]
mod test {
    use pigeon_core::PError;

    use crate::dur::index::Index;

    #[test]
    fn insert_and_get() {
        let mut index = Index::default();

        index.insert(0, 1).unwrap();
        index.insert(1, 2).unwrap();

        assert_eq!(index.get(0), Some(&1));
        assert_eq!(index.get(1), Some(&2));
    }

    #[test]
    fn can_only_insert_at_the_end() {
        let mut index = Index::default();

        assert_eq!(index.insert(10, 10), Ok(()));
        assert_eq!(index.insert(0, 0), Err(PError::IndexOffsetNotAllowed));
    }

    #[test]
    fn parse_index_from_bytes() {
        let bytes: Vec<u8> = [
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05],
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03],
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A],
        ]
        .into_iter()
        .flatten()
        .collect();

        let index = Index::try_from(bytes.as_slice()).unwrap();

        assert_eq!(index.get(0), Some(&0));
        assert_eq!(index.get(1), Some(&5));
        assert_eq!(index.get(3), Some(&10));
    }

    #[test]
    fn error_on_incomplete_index() {
        let mut bytes: Vec<u8> = [
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05],
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03],
        ]
        .into_iter()
        .flatten()
        .collect();
        bytes.append(&mut [0x00, 0x00, 0x00, 0x00].to_vec());

        let index = Index::try_from(bytes.as_slice());
        assert_eq!(index.err(), Some(PError::IndexParseFailed));
    }

    #[test]
    fn error_on_misordered_index() {
        let mut bytes: Vec<u8> = [
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05],
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        ]
        .into_iter()
        .flatten()
        .collect();
        bytes.append(&mut [0x00, 0x00, 0x00, 0x00].to_vec());

        let index = Index::try_from(bytes.as_slice());
        assert_eq!(index.err(), Some(PError::IndexParseFailed));
    }
}
