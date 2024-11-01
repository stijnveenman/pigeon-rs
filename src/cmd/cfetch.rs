use crate::{parse::Parse, Frame};

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
