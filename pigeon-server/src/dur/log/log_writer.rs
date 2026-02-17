use std::path::{Path, PathBuf};

use pigeon_core::PError;
use tokio::{
    fs::{File, remove_file},
    io::AsyncWriteExt,
};

use crate::dur::log::LOG_EXTENSION;

pub struct LogWriter {
    path: PathBuf,
    file: Option<File>,
}

impl LogWriter {
    pub fn new(base_dir: &str, start_offset: u64) -> LogWriter {
        let path = Path::new(base_dir)
            .join(Path::new(&start_offset.to_string()).with_extension(LOG_EXTENSION));

        LogWriter { path, file: None }
    }

    pub async fn open(&mut self) -> Result<&mut File, PError> {
        match self.file {
            None => {
                let file = File::options()
                    .create(true)
                    .append(true)
                    .open(&self.path)
                    .await
                    .map_err(|_| PError::LogOpenFailed)?;

                Ok(self.file.insert(file))
            }
            Some(_) => Ok(self.file.as_mut().unwrap()),
        }
    }

    pub async fn close(&mut self) -> Result<(), PError> {
        if let Some(mut file) = self.file.take() {
            file.flush().await.map_err(|_| PError::LogWriteFailed)?;
        }

        Ok(())
    }

    pub async fn delete(mut self) -> Result<(), PError> {
        self.close().await?;

        remove_file(self.path)
            .await
            .map_err(|_| PError::LogWriteFailed)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;

    use crate::dur::log::log_writer::LogWriter;

    #[tokio::test]
    async fn create_log_and_write() {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let mut _writer = LogWriter::new(base_dir, 0);
        // TODO:
    }

    #[tokio::test]
    async fn open_should_create_file() {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let mut writer = LogWriter::new(base_dir, 0);
        writer.open().await.unwrap();
        writer.close().await.unwrap();

        let file = dir.path().join("0.log");
        assert!(file.exists())
    }

    #[tokio::test]
    async fn open_and_delete() {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let mut writer = LogWriter::new(base_dir, 0);
        writer.open().await.unwrap();

        let file = dir.path().join("0.log");
        assert!(file.exists());

        writer.delete().await.unwrap();
        assert!(!file.exists());
    }
}
