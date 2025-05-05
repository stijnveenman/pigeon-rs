use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tracing::instrument;

use super::{Rpc, Shutdown};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Request {
    pub topic: String,
    pub key: ByteBuf,
    pub data: ByteBuf,
}

impl Rpc for Request {
    type Response = (u64, u64);

    fn to_request(self) -> super::Command {
        super::Command::Produce(self)
    }

    #[instrument(skip(self, db, _shutdown))]
    async fn apply(
        self,
        db: &mut super::Db,
        _shutdown: &mut Shutdown,
    ) -> Result<Self::Response, crate::db::Error> {
        db.produce(&self.topic, self.key, self.data)
    }
}
