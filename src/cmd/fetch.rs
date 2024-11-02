use std::{pin::Pin, time::Duration};

use tokio::{select, time};
use tokio_stream::{Stream, StreamExt, StreamMap};
use tracing::debug;

use crate::{
    db::{Db, DbErr, DbResult},
    parse::Parse,
    shutdown::Shutdown,
    Connection, Frame, Message,
};

#[derive(Debug)]
pub struct FetchConfig {
    pub timeout_ms: u64,
    pub topics: Vec<FetchTopicConfig>,
}

#[derive(Debug)]
pub struct FetchTopicConfig {
    pub topic: String,
    pub partitions: Vec<FetchPartitionConfig>,
}

#[derive(Debug)]
pub struct FetchPartitionConfig {
    pub partition: u64,
    pub offset: u64,
}

impl FetchConfig {
    pub(crate) async fn apply(
        self,
        db: &mut Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        let response = match self.fetch(db, shutdown).await {
            Ok(Some(message)) => {
                let mut frame = Frame::array();

                frame.push_bulk(message.key);
                frame.push_bulk(message.data);

                frame
            }
            Ok(None) => Frame::Null,
            Err(e) => Frame::Error(e.to_string()),
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn fetch(&self, db: &mut Db, shutdown: &mut Shutdown) -> DbResult<Option<Message>> {
        for topic in &self.topics {
            match topic.fetch(db).await? {
                Some(message) => return Ok(Some(message)),
                None => continue,
            }
        }

        let mut map = StreamMap::new();
        for topic in &self.topics {
            for partition in &topic.partitions {
                let rx = partition.fetch(db, &topic.topic)?;
                map.insert((&topic.topic, partition.partition), rx);
            }
        }

        select! {
            message = map.next() => {
                Ok(message.map(|m| m.1))
            }
            _ = time::sleep(Duration::from_millis(self.timeout_ms)) => {
                Ok(None)
            }
            _ = shutdown.recv() => {
                debug!("received shutdown signal waiting for fetch");
                Err(DbErr::ShuttingDown)
            }
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Self> {
        let timeout_ms = parse.next_int()?;

        let topics = parse
            .next_vec()?
            .into_iter()
            .map(|frame| {
                let mut parse = Parse::new(frame)?;

                FetchTopicConfig::parse_frames(&mut parse)
            })
            .collect::<crate::Result<_>>()?;

        Ok(Self { timeout_ms, topics })
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();

        frame.push_bulk("fetch".as_bytes().into());

        frame.push_int(self.timeout_ms);

        let v = self.topics.into_iter().map(|t| t.into_frame()).collect();
        frame.push_frame(Frame::from_vec(v));

        frame
    }
}

impl FetchTopicConfig {
    async fn fetch(&self, db: &mut Db) -> DbResult<Option<Message>> {
        for partition in &self.partitions {
            if let Some(message) = db.fetch(&self.topic, partition.partition, partition.offset)? {
                return Ok(Some(message));
            }
        }

        Ok(None)
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Self> {
        let topic = parse.next_string()?;

        let partitions = parse
            .next_vec()?
            .into_iter()
            .map(|frame| {
                let mut parse = Parse::new(frame)?;

                FetchPartitionConfig::parse_frames(&mut parse)
            })
            .collect::<crate::Result<_>>()?;

        Ok(Self { topic, partitions })
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();

        frame.push_string(self.topic);

        let v = self
            .partitions
            .into_iter()
            .map(|p| p.into_frame())
            .collect();

        frame.push_frame(Frame::from_vec(v));

        frame
    }
}

impl FetchPartitionConfig {
    fn fetch(
        &self,
        db: &mut Db,
        topic: &str,
    ) -> DbResult<Pin<Box<dyn Stream<Item = Message> + Send>>> {
        let mut rx = db.fetch_subscribe(topic, self.partition)?;
        let from_offset = self.offset;

        let rx = Box::pin(async_stream::stream! {
              while let Ok((offset, message)) = rx.recv().await {
                  if from_offset == 0 || offset > from_offset {
                      yield message;
                  }
              }
        }) as Pin<Box<dyn Stream<Item = Message> + Send>>;

        Ok(rx)
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Self> {
        Ok(Self {
            partition: parse.next_int()?,
            offset: parse.next_int()?,
        })
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();

        frame.push_int(self.partition);
        frame.push_int(self.offset);

        frame
    }
}
