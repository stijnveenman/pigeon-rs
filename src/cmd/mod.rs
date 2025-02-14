mod ping;
pub use ping::Ping;
use serde::{Deserialize, Serialize};

use crate::{
    db::{Db, DbErr},
    shutdown::Shutdown,
    Connection,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Ping(Ping),
}

pub type ServerResponse<T> = Result<T, DbErr>;

impl Command {
    pub(crate) async fn apply(
        self,
        _db: &mut Db,
        dst: &mut Connection,
        _shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use Command::*;

        match self {
            Ping(cmd) => cmd.apply(dst).await,
        }
    }
}
