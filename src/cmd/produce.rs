use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tracing::instrument;

use super::Rpc;

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

    #[instrument(skip(self, ctx))]
    async fn apply(self, ctx: &mut super::RpcContext) -> Result<Self::Response, crate::db::Error> {
        ctx.db.produce(&self.topic, self.key, self.data)
    }
}
