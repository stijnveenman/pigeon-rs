use tracing::debug;

use crate::{db::Db, parse::Parse, Connection, Frame};

#[derive(Debug)]
pub struct Fetch {
    topic: String,
    partition: u64,
    offset: u64,
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

    pub(crate) async fn apply(self, db: &mut Db, dst: &mut Connection) -> crate::Result<()> {
        let response = match db.fetch(&self.topic, self.partition, self.offset) {
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

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();

        frame.push_bulk("fetch".as_bytes().into());

        frame.push_string(self.topic);
        frame.push_int(self.partition);
        frame.push_int(self.offset);

        frame
    }
}
