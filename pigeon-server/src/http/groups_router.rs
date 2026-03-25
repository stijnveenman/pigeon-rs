use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    routing::post,
};

use crate::{
    http::error::HttpResult,
    systems::{SystemContext, execution_context::ExecutionContext},
};

async fn create_consumer_group(
    context: ExecutionContext,
    Path(group_id): Path<String>,
) -> HttpResult<()> {
    context.create_consumer_group(&group_id)?;

    Ok(Json(()))
}

pub fn router() -> Router<Arc<SystemContext>> {
    Router::<Arc<SystemContext>>::new().route("/{group_id}", post(create_consumer_group))
}
