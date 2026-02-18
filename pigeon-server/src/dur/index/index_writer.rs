use std::path::{Path, PathBuf};

use pigeon_core::PError;
use tokio::{
    fs::{File, remove_file},
    io::AsyncWriteExt,
};

use crate::dur::index::INDEX_EXTENSION;

pub struct IndexWriter {
    path: PathBuf,
    file: File,
}

impl IndexWriter {
    pub async fn open(base_dir: &str, start_offset: u64) -> Result<IndexWriter, PError> {
        let path = Path::new(base_dir)
            .join(Path::new(&start_offset.to_string()).with_extension(INDEX_EXTENSION));

        let file = File::options()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|_| PError::IndexOpenFailed)?;

        Ok(IndexWriter { file, path })
    }

    pub async fn close(&mut self) -> Result<(), PError> {
        Ok(())
    }

    pub async fn delete(mut self) -> Result<(), PError> {
        self.file
            .flush()
            .await
            .map_err(|_| PError::LogWriteFailed)?;
        drop(self.file);

        remove_file(self.path)
            .await
            .map_err(|_| PError::IndexWriteFailed)?;

        Ok(())
    }

    pub async fn append(&mut self, offset: u64, byte_offset: u64) -> Result<(), PError> {
        self.file
            .write_u64(offset)
            .await
            .map_err(|_| PError::IndexWriteFailed)?;
        self.file
            .write_u64(byte_offset)
            .await
            .map_err(|_| PError::IndexWriteFailed)?;
        self.file
            .flush()
            .await
            .map_err(|_| PError::IndexWriteFailed)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;

    use crate::dur::index::index_writer::IndexWriter;

    #[tokio::test]
    async fn create_index_and_write() {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let mut writer = IndexWriter::open(base_dir, 0).await.unwrap();
        assert_eq!(writer.append(10, 10).await, Ok(()));
    }

    #[tokio::test]
    async fn open_should_create_file() {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let mut writer = IndexWriter::open(base_dir, 0).await.unwrap();
        writer.close().await.unwrap();

        let file = dir.path().join("0.index");
        assert!(file.exists())
    }

    #[tokio::test]
    async fn open_and_delete() {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let writer = IndexWriter::open(base_dir, 0).await.unwrap();

        let file = dir.path().join("0.index");
        assert!(file.exists());

        writer.delete().await.unwrap();
        assert!(!file.exists());
    }
}
