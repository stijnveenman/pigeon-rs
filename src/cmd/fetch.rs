use tracing::debug;

use crate::{
    db::{Db, DbErr, DbResult},
    parse::Parse,
    Connection, Frame, Message,
};

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

    async fn wait_for_message(self, db: &mut Db) -> DbResult<Message> {
        let mut rx = db.fetch_subscribe(&self.topic, self.partition)?;

        let message = rx.recv().await.map_err(|_| DbErr::RecvError)?;

        Ok(message)
    }

    pub(crate) async fn apply(self, db: &mut Db, dst: &mut Connection) -> crate::Result<()> {
        let response = match db.fetch(&self.topic, self.partition, self.offset) {
            Ok(Some(message)) => make_message_frame(message),
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

fn make_message_frame(message: Message) -> Frame {
    let mut frame = Frame::array();
    frame.push_bulk(message.key);
    frame.push_bulk(message.data);
    frame
}
