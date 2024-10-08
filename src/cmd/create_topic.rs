use bytes::Bytes;
use tracing::{debug, instrument};

use crate::{db::Db, parse::Parse, Connection, Frame};

/// Creates a new topic
/// returns OK if the topic was succesfully created
#[derive(Debug)]
pub struct CreateTopic {
    name: String,
    partitions: u64,
}

impl CreateTopic {
    pub fn new(name: String, partitions: u64) -> CreateTopic {
        CreateTopic { name, partitions }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<CreateTopic> {
        Ok(CreateTopic {
            name: parse.next_string()?,
            partitions: parse.next_int()?,
        })
    }

    #[instrument(skip(self, dst, db))]
    pub(crate) async fn apply(self, db: &mut Db, dst: &mut Connection) -> crate::Result<()> {
        let response = match db.create_topic(self.name, self.partitions) {
            Ok(()) => Frame::Simple("OK".to_string()),
            Err(e) => Frame::Error(e.to_string()),
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("ctopic".as_bytes()));

        frame.push_string(self.name);
        frame.push_int(self.partitions);

        frame
    }
}
