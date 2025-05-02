use serde::{Deserialize, Serialize};

use crate::db;

use super::{Db, Rpc, Shutdown};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Request {
    pub name: String,
    pub partitions: u64,
}

impl Rpc for Request {
    type Response = ();

    async fn apply(
        self,
        db: &mut Db,
        _shutdown: &mut Shutdown,
    ) -> Result<Self::Response, db::Error> {
        db.create_topic(self.name, self.partitions)
    }

    fn to_request(self) -> super::Command {
        super::Command::CreateTopic(self)
    }
}
