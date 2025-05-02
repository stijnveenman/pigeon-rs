pub mod create_topic;
pub mod fetch;
pub mod ping;
pub mod produce;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    connection::{self},
    db::{self, Db},
    shutdown::Shutdown,
    Connection,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Ping(ping::Request),
    CreateTopic(create_topic::Request),
    Produce(produce::Request),
    Fetch(fetch::Request),
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Error in underlying connection")]
    Connection(#[from] connection::Error),
}

pub trait Rpc {
    type Response;
    fn to_request(self) -> Command;
    async fn apply(self, db: &mut Db, shutdown: &mut Shutdown)
        -> Result<Self::Response, db::Error>;
}

impl Command {
    pub(crate) async fn apply(
        self,
        db: &mut Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> Result<(), connection::Error> {
        use Command::*;

        match self {
            Ping(request) => dst.write_response(&request.apply(db, shutdown).await).await,
            CreateTopic(request) => dst.write_response(&request.apply(db, shutdown).await).await,
            Produce(request) => dst.write_response(&request.apply(db, shutdown).await).await,
            Fetch(request) => dst.write_response(&request.apply(db, shutdown).await).await,
        }
    }
}
