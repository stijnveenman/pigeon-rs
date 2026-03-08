use std::sync::Arc;

use axum::{Json, Router, extract::State, routing::post};
use pigeon_core::{PError, rpc::create_topic::CreateTopic};

use crate::{http::error::HttpResult, systems::SystemContext};

async fn create_topic(
    state: State<Arc<SystemContext>>,
    command: Json<CreateTopic>,
) -> HttpResult<String> {
    dbg!(command);
    None.ok_or(PError::IndexParseFailed)?;

    Ok(Json("foo".to_string()))
}

pub fn router() -> Router<Arc<SystemContext>> {
    Router::<Arc<SystemContext>>::new().route("/", post(create_topic))
}
