use std::path::{Path, PathBuf};

use pigeon_core::{PError, record::Record};
use tokio::{
    fs::{File, remove_file},
    io::{AsyncSeekExt, AsyncWriteExt},
};

use crate::dur::log::LOG_EXTENSION;

pub struct LogWriter {
    file: File,
    path: PathBuf,
    file_position: u64,
}

impl LogWriter {
    // TODO: only create when we want to, ie; when TopicSystem is creating a new topic
    pub async fn open(base_dir: &Path, start_offset: u64) -> Result<LogWriter, PError> {
        let path =
            base_dir.join(Path::new(&start_offset.to_string()).with_extension(LOG_EXTENSION));

        let mut file = File::options()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|_| PError::LogOpenFailed)?;

        let file_position = file
            .stream_position()
            .await
            .map_err(|_| PError::LogOpenFailed)?;

        Ok(LogWriter {
            file,
            path,
            file_position,
        })
    }

    pub async fn delete(mut self) -> Result<(), PError> {
        self.file
            .flush()
            .await
            .map_err(|_| PError::LogWriteFailed)?;
        drop(self.file);

        remove_file(self.path)
            .await
            .map_err(|_| PError::LogWriteFailed)?;

        Ok(())
    }

    pub async fn append(&mut self, record: &Record) -> Result<u64, PError> {
        let mut written = 0u64;

        self.file
            .write_u64(record.offset)
            .await
            .map_err(|_| PError::LogWriteFailed)?;
        written += 8;

        self.file
            .write_u64(record.key.len() as u64)
            .await
            .map_err(|_| PError::LogWriteFailed)?;
        written += 8;

        self.file
            .write_all(&record.key)
            .await
            .map_err(|_| PError::LogWriteFailed)?;
        written += record.key.len() as u64;

        self.file
            .write_u64(record.value.len() as u64)
            .await
            .map_err(|_| PError::LogWriteFailed)?;
        written += 8;

        self.file
            .write_all(&record.value)
            .await
            .map_err(|_| PError::LogWriteFailed)?;
        written += record.value.len() as u64;

        self.file
            .flush()
            .await
            .map_err(|_| PError::LogWriteFailed)?;

        let prev_position = self.file_position;
        self.file_position += written;
        Ok(prev_position)
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use pigeon_core::record::Record;
    use tempfile::tempdir;

    use crate::dur::log::log_writer::LogWriter;

    #[tokio::test]
    async fn create_log_and_write() {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let mut writer = LogWriter::open(Path::new(base_dir), 0).await.unwrap();
        let record = Record {
            offset: 1,
            key: "hello".as_bytes().to_vec(),
            value: "world".as_bytes().to_vec(),
        };

        writer.append(&record).await.unwrap();
    }

    #[tokio::test]
    async fn open_should_create_file() {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let writer = LogWriter::open(Path::new(base_dir), 0).await.unwrap();
        drop(writer);

        let file = dir.path().join("0.log");
        assert!(file.exists())
    }

    #[tokio::test]
    async fn open_and_delete() {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let writer = LogWriter::open(Path::new(base_dir), 0).await.unwrap();

        let file = dir.path().join("0.log");
        assert!(file.exists());

        writer.delete().await.unwrap();
        assert!(!file.exists());
    }
}
