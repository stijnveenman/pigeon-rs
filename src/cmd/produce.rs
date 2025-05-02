use serde::{Deserialize, Serialize};

use super::Rpc;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Request {
    pub topic: String,
    pub key: Vec<u8>,
    pub data: Vec<u8>,
}

impl Rpc for Request {
    type Response = (u64, u64);

    fn to_request(self) -> super::Command {
        super::Command::Produce(self)
    }

    async fn apply(self, db: &mut super::Db) -> Result<Self::Response, crate::db::Error> {
        db.produce(&self.topic, self.key.into(), self.data.into())
    }
}
