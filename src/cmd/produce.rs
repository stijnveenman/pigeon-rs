use bytes::Bytes;
use tracing::{debug, instrument};

use crate::{db::Db, parse::Parse, Connection, Frame};

#[derive(Debug)]
pub struct Produce {
    topic: String,
    key: Bytes,
    data: Bytes,
}

impl Produce {
    pub fn new(topic: String, key: Bytes, data: Bytes) -> Produce {
        Produce { topic, key, data }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Produce> {
        Ok(Produce {
            topic: parse.next_string()?,
            key: parse.next_bytes()?,
            data: parse.next_bytes()?,
        })
    }

    #[instrument(skip(self, dst, db))]
    pub(crate) async fn apply(self, db: &mut Db, dst: &mut Connection) -> crate::Result<()> {
        let response = match db.produce(self.topic, self.key, self.data) {
            Ok((partition, offset)) => {
                let mut frame = Frame::array();
                frame.push_int(partition);
                frame.push_int(offset);
                frame
            }
            Err(e) => Frame::Error(e.to_string()),
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();

        frame.push_bulk(Bytes::from("produce".as_bytes()));

        frame.push_string(self.topic);
        frame.push_bulk(self.key);
        frame.push_bulk(self.data);

        frame
    }
}
