use std::sync::Arc;

use axum::{Json, Router, extract::State, routing::post};
use pigeon_core::rpc::create_topic::CreateTopic;

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

pub fn router() -> Router<Arc<SystemContext>> {
    Router::<Arc<SystemContext>>::new().route("/", post(create_topic))
}
