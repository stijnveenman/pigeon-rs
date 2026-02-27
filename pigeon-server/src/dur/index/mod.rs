pub mod index_writer;

use std::{
    collections::{BTreeMap, btree_map::Range},
    ops::RangeBounds,
    path::Path,
};

use bytes::Buf;
use pigeon_core::PError;
use tokio::{fs::File, io::AsyncReadExt};

pub const INDEX_EXTENSION: &str = "index";

#[derive(Default, Debug)]
pub struct Index(BTreeMap<u64, u64>);

impl Index {
    pub fn append(&mut self, offset: u64, byte_offset: u64) -> Result<(), PError> {
        if self.0.last_entry().is_some_and(|v| offset <= *v.key()) {
            return Err(PError::IndexOffsetNotAllowed);
        }

        self.0.insert(offset, byte_offset);

        Ok(())
    }

    pub fn get(&self, offset: u64) -> Option<&u64> {
        self.0.get(&offset)
    }

    pub fn range<R>(&self, range: R) -> Range<'_, u64, u64>
    where
        R: RangeBounds<u64>,
    {
        self.0.range(range)
    }

    pub async fn from_file(base_dir: &str, start_offset: u64) -> Result<Index, PError> {
        let path = Path::new(base_dir)
            .join(Path::new(&start_offset.to_string()).with_extension(INDEX_EXTENSION));

        let mut file = File::options()
            .read(true)
            .open(path)
            .await
            .map_err(|_| PError::IndexNotFound)?;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .await
            .map_err(|_| PError::IndexReadFailed)?;

        Index::try_from(bytes.as_slice())
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
                .append(offset, byte_offset)
                .map_err(|_| PError::IndexParseFailed)?;
        }

        Ok(index)
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use pigeon_core::PError;
    use tempfile::tempdir;

    use crate::dur::index::{Index, index_writer::IndexWriter};

    #[test]
    fn append_and_get() {
        let mut index = Index::default();

        index.append(0, 1).unwrap();
        index.append(1, 2).unwrap();

        assert_eq!(index.get(0), Some(&1));
        assert_eq!(index.get(1), Some(&2));
    }

    #[test]
    fn can_only_append_at_the_end() {
        let mut index = Index::default();

        assert_eq!(index.append(10, 10), Ok(()));
        assert_eq!(index.append(0, 0), Err(PError::IndexOffsetNotAllowed));
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

    #[tokio::test]
    async fn write_index_and_read() {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let mut writer = IndexWriter::open(Path::new(base_dir), 0).await.unwrap();

        writer.append(0, 10).await.unwrap();
        writer.append(1, 20).await.unwrap();

        writer.close().await.unwrap();

        let index = Index::from_file(base_dir, 0).await.unwrap();

        assert_eq!(index.get(0), Some(&10));
        assert_eq!(index.get(1), Some(&20));

        drop(dir);
    }
}
