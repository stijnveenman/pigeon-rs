pub mod create_topic;
pub mod ping;
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
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Error in underlying connection")]
    Connection(#[from] connection::Error),
}

pub trait Transaction {
    type Response;
    fn to_request(self) -> Command;
    async fn apply(self, db: &mut Db) -> Result<Self::Response, db::Error>;
}

impl Command {
    pub(crate) async fn apply(
        self,
        db: &mut Db,
        dst: &mut Connection,
        _shutdown: &mut Shutdown,
    ) -> Result<(), connection::Error> {
        use Command::*;

        match self {
            Ping(request) => dst.write_response(&request.apply(db).await).await,
            CreateTopic(request) => dst.write_response(&request.apply(db).await).await,
        }
    }
}
