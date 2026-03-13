use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    routing::{get, post},
};
use pigeon_core::{
    record::Record,
    rpc::{append_record::AppendRecord, create_topic::CreateTopic},
};

use crate::{http::error::HttpResult, systems::SystemContext};

async fn create_topic(
    state: State<Arc<SystemContext>>,
    command: Json<CreateTopic>,
) -> HttpResult<()> {
    state
        .topics
        .create_topic(
            &command.topic_name,
            command
                .num_partitions
                .unwrap_or(state.config.topics.default_partitions),
        )
        .await?;

    Ok(Json(()))
}

async fn read_record(
    state: State<Arc<SystemContext>>,
    Path((topic_name, partition_id, offset)): Path<(String, u64, u64)>,
) -> HttpResult<Record> {
    let record = state
        .topics
        .read_record(&topic_name, partition_id, offset)
        .await?;

    Ok(Json(record))
}

async fn append_record(
    state: State<Arc<SystemContext>>,
    Path((topic_name, partition_id)): Path<(String, u64)>,
    command: Json<AppendRecord>,
) -> HttpResult<u64> {
    let offset = state
        .topics
        .append_record(
            &topic_name,
            partition_id,
            Record::new(&command.key, &command.value),
        )
        .await?;

    Ok(Json(offset))
}

pub fn router() -> Router<Arc<SystemContext>> {
    Router::<Arc<SystemContext>>::new()
        .route("/", post(create_topic))
        .route("/{topic_name}/{partition_id}", post(append_record))
        .route("/{topic_name}/{partition_id}/{offset}", get(read_record))
}
