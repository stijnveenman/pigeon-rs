use tokio::select;
use tracing::debug;

use crate::{
    db::{Db, DbErr},
    parse::Parse,
    shutdown::Shutdown,
    Connection, Frame, Message,
};

#[derive(Debug)]
pub struct Fetch {
    topic: String,
    partition: u64,
    offset: u64,
}

#[derive(Debug)]
pub struct FetchConfig {
    timeout_ms: u64,
    topics: Vec<FetchTopicConfig>,
}

#[derive(Debug)]
pub struct FetchTopicConfig {
    topic: String,
    partitions: Vec<FetchPartitionConfig>,
}

#[derive(Debug)]
pub struct FetchPartitionConfig {
    partition: u64,
    offset: u64,
}

impl FetchConfig {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Self> {
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

        frame.push_int(self.timeout_ms);

        let v = self.topics.into_iter().map(|t| t.into_frame()).collect();
        frame.push_frame(Frame::from_vec(v));

        frame
    }
}

impl FetchTopicConfig {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Self> {
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
        let v = self
            .partitions
            .into_iter()
            .map(|p| p.into_frame())
            .collect();

        Frame::from_vec(v)
    }
}

impl FetchPartitionConfig {
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Self> {
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

impl Fetch {
    pub fn new(topic: String, partition: u64, offset: u64) -> Fetch {
        Fetch {
            topic,
            partition,
            offset,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Fetch> {
        Ok(Fetch {
            topic: parse.next_string()?,
            partition: parse.next_int()?,
            offset: parse.next_int()?,
        })
    }

    async fn wait_for_message(self, db: &mut Db, shutdown: &mut Shutdown) -> Frame {
        let mut rx = match db.fetch_subscribe(&self.topic, self.partition) {
            Ok(rx) => rx,
            Err(e) => return Frame::Error(e.to_string()),
        };

        select! {
            message = rx.recv() => {
                match message {
                    Ok(message) => make_message_frame(message),
                    Err(_) => Frame::Error(DbErr::RecvError.to_string()),
                }
            },
            _ = shutdown.recv() => {
                debug!("received shutdown signal waiting for fetch");
                Frame::Error(DbErr::ShuttingDown.to_string())
            }
        }
    }

    pub(crate) async fn apply(
        self,
        db: &mut Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        let response = match db.fetch(&self.topic, self.partition, self.offset) {
            Ok(Some(message)) => make_message_frame(message),
            Ok(None) => self.wait_for_message(db, shutdown).await,
            Err(e) => Frame::Error(e.to_string()),
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();

        frame.push_bulk("fetch".as_bytes().into());

        frame.push_string(self.topic);
        frame.push_int(self.partition);
        frame.push_int(self.offset);

        frame
    }
}

fn make_message_frame(message: Message) -> Frame {
    let mut frame = Frame::array();
    frame.push_bulk(message.key);
    frame.push_bulk(message.data);
    frame
}
