use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    routing::{delete, get, post},
};
use pigeon_core::{
    record::Record,
    rpc::{append_record::AppendRecord, create_topic::CreateTopic},
};

use crate::{
    http::error::HttpResult,
    systems::{SystemContext, execution_context::ExecutionContext},
};

#[axum::debug_handler]
async fn create_topic(
    State(state): State<Arc<SystemContext>>,
    command: Json<CreateTopic>,
) -> HttpResult<()> {
    ExecutionContext::user(state)
        .create_topic(&command.topic_name, command.num_partitions)
        .await?;

    Ok(Json(()))
}

async fn read_record(
    State(state): State<Arc<SystemContext>>,
    Path((topic_name, partition_id, offset)): Path<(String, u64, u64)>,
) -> HttpResult<Record> {
    let record = ExecutionContext::user(state)
        .read_record(&topic_name, partition_id, offset)
        .await?;

    Ok(Json(record))
}

async fn append_record(
    State(state): State<Arc<SystemContext>>,
    Path((topic_name, partition_id)): Path<(String, u64)>,
    command: Json<AppendRecord>,
) -> HttpResult<u64> {
    let offset = ExecutionContext::user(state)
        .append_record(
            &topic_name,
            partition_id,
            Record::new(&command.key, &command.value),
        )
        .await?;

    Ok(Json(offset))
}

async fn delete_topic(
    State(state): State<Arc<SystemContext>>,
    Path(topic_name): Path<String>,
) -> HttpResult<()> {
    state
        .clone()
        .delete_topic(&ExecutionContext::user(state), &topic_name)
        .await?;

    Ok(Json(()))
}

pub fn router() -> Router<Arc<SystemContext>> {
    Router::<Arc<SystemContext>>::new()
        .route("/", post(create_topic))
        .route("/{topic_name}", delete(delete_topic))
        .route("/{topic_name}/{partition_id}", post(append_record))
        .route("/{topic_name}/{partition_id}/{offset}", get(read_record))
}
