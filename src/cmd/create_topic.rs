use serde::{Deserialize, Serialize};

use crate::db;

use super::{Db, Rpc};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Request {
    name: String,
    partitions: u64,
}

impl Rpc for Request {
    type Response = ();

    async fn apply(self, db: &mut Db) -> Result<Self::Response, db::Error> {
        db.create_topic(self.name, self.partitions)
    }

    fn to_request(self) -> super::Command {
        super::Command::CreateTopic(self)
    }
}

impl Request {
    pub fn new(name: String, partitions: u64) -> Request {
        Request { name, partitions }
    }
}
