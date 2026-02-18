use std::{
    fs::File,
    io::{self, Read, Seek},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::{Arc, RwLock, RwLockReadGuard},
};

use bytes::{Buf, Bytes};
use pigeon_core::{PError, record::Record};
use tokio::task::spawn_blocking;

use crate::dur::log::LOG_EXTENSION;

pub struct LogReader {
    file: Arc<File>,
}

impl LogReader {
    pub async fn open(base_dir: &str, start_offset: u64) -> Result<LogReader, PError> {
        let path = Path::new(base_dir)
            .join(Path::new(&start_offset.to_string()).with_extension(LOG_EXTENSION));

        let file = File::options()
            .read(true)
            .open(path)
            .map_err(|_| PError::IndexNotFound)?;

        Ok(LogReader {
            file: Arc::new(file),
        })
    }

    async fn read_from(&self, mut start_offset: u64) -> Result<Vec<u8>, PError> {
        let file = self.file.clone();

        let bytes: Result<_, PError> = spawn_blocking(move || {
            let mut bytes = Vec::new();

            loop {
                match file.read_at(&mut bytes, start_offset) {
                    Ok(0) => break,
                    Ok(n) => {
                        start_offset += n as u64;
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => return Err(PError::LogReadFailed),
                }
            }

            Ok(bytes)
        })
        .await
        .map_err(|_| PError::LogReadFailed)?;
        let bytes = bytes?;

        Ok(bytes)
    }

    async fn read_range(&self, start_offset: u64, end_offset: u64) -> Result<Vec<u8>, PError> {
        let byte_length = end_offset - start_offset;
        let file = self.file.clone();

        let bytes: Result<_, PError> = spawn_blocking(move || {
            let mut bytes = vec![0; byte_length as usize];
            file.read_exact_at(&mut bytes, start_offset)
                .map_err(|_| PError::LogReadFailed)?;

            Ok(bytes)
        })
        .await
        .map_err(|_| PError::LogReadFailed)?;
        let bytes = bytes?;

        Ok(bytes)
    }

    pub async fn read_record(
        &self,
        start_offset: u64,
        end_offset: Option<u64>,
    ) -> Result<Record, PError> {
        let bytes = match end_offset {
            Some(end_offset) => self.read_range(start_offset, end_offset).await,
            None => self.read_from(start_offset).await,
        }?;

        let mut bytes = Bytes::from(bytes);

        let offset = bytes.try_get_u64().map_err(|_| PError::ParseRecordFailed)?;

        let key_len = bytes.try_get_u64().map_err(|_| PError::ParseRecordFailed)? as usize;
        if bytes.remaining() < key_len {
            return Err(PError::ParseRecordFailed);
        }
        let key = bytes.slice(0..key_len);
        bytes.advance(key_len);

        let value_len = bytes.try_get_u64().map_err(|_| PError::ParseRecordFailed)? as usize;
        if bytes.remaining() < value_len {
            return Err(PError::ParseRecordFailed);
        }
        let value = bytes.slice(0..key_len);
        bytes.advance(value_len);

        Ok(Record {
            offset,
            key: key.to_vec(),
            value: value.to_vec(),
        })
    }
}
