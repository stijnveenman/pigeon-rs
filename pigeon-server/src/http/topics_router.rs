use std::sync::Arc;

use axum::{Json, Router, routing::get};
use pigeon_core::PError;

use crate::{http::error::HttpResult, systems::SystemContext};

async fn create_topic() -> HttpResult<String> {
    None.ok_or(PError::IndexParseFailed)?;

    Ok(Json("foo".to_string()))
}

pub fn router() -> Router<Arc<SystemContext>> {
    Router::new().route("/", get(create_topic))
}
