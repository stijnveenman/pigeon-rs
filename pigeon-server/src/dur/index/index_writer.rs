use std::path::{Path, PathBuf};

use pigeon_core::PError;
use tokio::{fs::File, io::AsyncWriteExt};

use crate::dur::index::INDEX_EXTENSION;

pub struct IndexWriter {
    path: PathBuf,
    file: Option<File>,
}

impl IndexWriter {
    pub fn new(base_dir: &str, start_offset: u64) -> IndexWriter {
        let path = Path::new(base_dir)
            .with_file_name(start_offset.to_string())
            .with_extension(INDEX_EXTENSION);

        IndexWriter { path, file: None }
    }

    pub async fn open(&mut self) -> Result<&mut File, PError> {
        match self.file {
            None => {
                let file = File::options()
                    .create(true)
                    .append(true)
                    .open(&self.path)
                    .await
                    .map_err(|_| PError::IndexOpenFailed)?;

                Ok(self.file.insert(file))
            }
            Some(_) => Ok(self.file.as_mut().unwrap()),
        }
    }

    pub async fn close(&mut self) -> Result<(), PError> {
        if let Some(mut file) = self.file.take() {
            file.flush().await.map_err(|_| PError::IndexWriteFailed)?;
        }

        Ok(())
    }

    pub async fn append(&mut self, offset: u64, byte_offset: u64) -> Result<(), PError> {
        let file = self.open().await?;

        file.write_u64(offset)
            .await
            .map_err(|_| PError::IndexWriteFailed)?;
        file.write_u64(byte_offset)
            .await
            .map_err(|_| PError::IndexWriteFailed)?;
        file.flush().await.map_err(|_| PError::IndexWriteFailed)?;

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

        let mut writer = IndexWriter::new(base_dir, 0);
        assert_eq!(writer.append(10, 10).await, Ok(()));
    }

    #[tokio::test]
    async fn open_should_create_file() {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let mut writer = IndexWriter::new(base_dir, 0);
        writer.open().await.unwrap();
        writer.close().await.unwrap();

        let file = dir.path().with_file_name("0.index");
        assert!(file.exists())
    }
}
