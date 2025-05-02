use serde::{Deserialize, Serialize};
use tracing::instrument;

use super::{Db, Rpc, Shutdown};
use crate::db;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Request {
    pub name: String,
    pub partitions: u64,
}

impl Rpc for Request {
    type Response = ();

    #[instrument(skip(self, db, _shutdown))]
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
