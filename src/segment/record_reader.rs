use std::{os::unix::fs::FileExt, sync::Arc};

use bytes::Bytes;
use tokio::{fs::OpenOptions, task::spawn_blocking};

use crate::{
    bin_ser::{BinaryDeserialize, StaticBinarySize},
    data::{record::Record, record_set::RecordSet},
};

pub struct RecordReader {
    file: Arc<std::fs::File>,
}

impl RecordReader {
    pub async fn new(file_path: &str) -> Result<Self, tokio::io::Error> {
        let file = OpenOptions::new().read(true).open(file_path).await?;

        Ok(Self {
            file: Arc::new(file.into_std().await),
        })
    }

    pub async fn load_messages_at(
        &mut self,
        start_file_offset: u64,
        offset: u64,
        count: u64,
    ) -> Result<Vec<Record>, std::io::Error> {
        let file = self.file.clone();

        spawn_blocking(move || {
            let mut file_offset = start_file_offset;
            loop {
                let mut header_buf = vec![0; RecordSet::binary_size()];
                file.read_exact_at(&mut header_buf, file_offset)?;
                file_offset += header_buf.len() as u64;

                // TODO: handle deserialize errors
                let header = RecordSet::deserialize(&mut Bytes::from(header_buf))
                    .expect("Failed to deserialize RecordSetHeader");

                // check if header contains the message we want
                if header.start_offset <= offset && header.end_offset >= offset {
                    let to_idx = offset + count - 1;
                    if to_idx > header.end_offset {
                        // TODO: we need more messages
                        unimplemented!()
                    }

                    // Read messages
                    let mut buf = vec![0; header.length as usize];
                    file.read_exact_at(&mut buf, file_offset)?;
                    let mut buf = Bytes::from(buf);
                    let records = (0..header.record_count)
                        .map(|_| {
                            Record::deserialize(&mut buf).expect("Failed to deserialize Record")
                        })
                        .skip((offset - header.start_offset) as usize)
                        .collect();

                    return Ok(records);
                } else {
                    // else skip this header
                    file_offset += header.length as u64;
                }
            }
        })
        .await?
    }
}
