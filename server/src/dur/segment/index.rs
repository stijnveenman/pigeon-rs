use std::{
    collections::{BTreeMap, btree_map::Range},
    io::ErrorKind,
    ops::RangeBounds,
};

use tokio::{
    fs::{File, OpenOptions, remove_file},
    io::{self, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
};

use crate::dur::error::Result;

pub struct Index {
    index: BTreeMap<u64, u64>,
    file: File,
    path: String,
}

impl Index {
    pub async fn load_from_disk(path: &str) -> Result<Self> {
        let index_file = match OpenOptions::new().read(true).open(path).await {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Self::new(path, BTreeMap::default()).await;
            }
            Err(err) => return Err(err.into()),
        };

        let mut index = BTreeMap::new();
        let mut reader = BufReader::new(index_file);

        loop {
            let offset = match reader.read_u64().await {
                Ok(offset) => offset,
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            };

            let position = reader.read_u64().await?;

            index.insert(offset, position);
        }

        Self::new(path, index).await
    }

    async fn new(path: &str, index: BTreeMap<u64, u64>) -> Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        Ok(Self {
            file,
            index,
            path: path.to_string(),
        })
    }

    pub async fn append(&mut self, offset: u64, file_offset: u64) -> Result<()> {
        let mut writer = BufWriter::new(&mut self.file);
        writer.write_u64(offset).await?;
        writer.write_u64(file_offset).await?;
        writer.flush().await?;

        self.index.insert(offset, file_offset);

        Ok(())
    }

    pub fn range<R>(&self, range: R) -> Range<'_, u64, u64>
    where
        R: RangeBounds<u64>,
    {
        self.index.range(range)
    }

    pub fn max_offset(&self) -> Option<u64> {
        self.index.last_key_value().map(|e| *e.0)
    }

    pub fn min_offset(&self) -> Option<u64> {
        self.index.first_key_value().map(|e| *e.0)
    }

    pub async fn delete(self) -> Result<()> {
        let Self { file, path, .. } = self;

        drop(file);

        remove_file(path).await?;

        Ok(())
    }
}
