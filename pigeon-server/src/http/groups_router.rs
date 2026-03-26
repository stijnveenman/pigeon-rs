use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query},
    routing::{get, post},
};
use pigeon_core::rpc::{
    consumer_group_respones::ConsumerGroupResponse, join_consumer_group::JoinConsumerGroupParams,
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

async fn join_consumer_group(
    context: ExecutionContext,
    Path((group_id, consumer_id)): Path<(String, String)>,
    Query(params): Query<JoinConsumerGroupParams>,
) -> HttpResult<ConsumerGroupResponse> {
    let group = context.join_consumer_group(&group_id, &consumer_id, params.epoch)?;

    Ok(Json(group))
}

async fn get_consumer_group(
    context: ExecutionContext,
    Path(group_id): Path<String>,
) -> HttpResult<ConsumerGroupResponse> {
    let group = context.get_group_response(&group_id)?;

    Ok(Json(group))
}

pub fn router() -> Router<Arc<SystemContext>> {
    Router::<Arc<SystemContext>>::new()
        .route(
            "/{group_id}",
            get(get_consumer_group).post(create_consumer_group),
        )
        .route("/{group_id}/{consumer_id}", post(join_consumer_group))
}
