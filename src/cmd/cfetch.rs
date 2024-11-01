use std::pin::Pin;

use tokio::select;
use tokio_stream::{Stream, StreamExt, StreamMap};
use tracing::{debug, info};

use crate::{
    cmd::fetch::make_message_frame,
    db::{Db, DbResult},
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
        let response = match self.fetch(db).await {
            Ok(Some(message)) => make_message_frame(message),
            Ok(None) => Frame::Null,
            Err(e) => Frame::Error(e.to_string()),
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn fetch(&self, db: &mut Db) -> DbResult<Option<Message>> {
        for topic in &self.topics {
            match topic.fetch(db).await? {
                Some(message) => return Ok(Some(message)),
                None => continue,
            }
        }

        let mut map = StreamMap::new();
        for topic in &self.topics {
            // TODO partition and handle error
            let mut rx = db.fetch_subscribe(&topic.topic, 2).expect("test");
            let rx = Box::pin(async_stream::stream! {
                  while let Ok(item) = rx.recv().await {
                      yield item;
                  }
            }) as Pin<Box<dyn Stream<Item = Message> + Send>>;

            map.insert(&topic.topic, rx);
        }

        let message = map.next().await.map(|m| m.1);
        info!("used streammap");
        Ok(message)
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

        frame.push_bulk("cfetch".as_bytes().into());

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
