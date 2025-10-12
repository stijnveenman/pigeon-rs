mod app_error;
pub mod responses;

use std::collections::HashMap;

use app_error::AppResult;
use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use responses::create_topic_response::CreateTopicResponse;
use responses::produce_response::ProduceResponse;
use responses::record_response::RecordResponse;
use tokio::net::TcpListener;
use tracing::info;

use crate::app::App;
use crate::commands::create_topic::CreateTopic;
use crate::commands::fetch::Fetch;
use crate::commands::produce::Produce;
use crate::data::record::Record;
use crate::data::state::topic_state::TopicState;
use crate::data::timestamp::Timestamp;

pub struct HttpServer {
    router: Router,
    address: String,
}

async fn create_topic(
    State(app): State<App>,
    Json(create_topic): Json<CreateTopic>,
) -> AppResult<CreateTopicResponse> {
    let mut lock = app.write().await;

    let topic_id = lock
        .create_topic(
            create_topic.topic_id,
            &create_topic.name,
            create_topic.partitions,
        )
        .await?;

    Ok(Json(CreateTopicResponse { topic_id }))
}

async fn produce(
    State(app): State<App>,
    Json(produce): Json<Produce>,
) -> AppResult<ProduceResponse> {
    let mut lock = app.write().await;

    let record = Record {
        key: produce.encoding.decode(&produce.key)?,
        value: produce.encoding.decode(&produce.value)?,
        timestamp: Timestamp::now(),
        offset: 0,
        headers: vec![],
    };

    let offset = lock
        .produce(produce.topic, produce.partition_id, record)
        .await?;

    Ok(Json(ProduceResponse { offset }))
}

async fn get_topic_state(
    State(app): State<App>,
    Path(name): Path<String>,
) -> AppResult<TopicState> {
    let lock = app.read().await;

    let state = lock.get_topic_by_name(&name)?.state();

    Ok(Json(state))
}

async fn get_all_topics_state(State(app): State<App>) -> AppResult<HashMap<u64, TopicState>> {
    let lock = app.read().await;

    Ok(Json(lock.topic_states()))
}

async fn fetch(State(app): State<App>, Json(fetch): Json<Fetch>) -> AppResult<RecordResponse> {
    let lock = app.read().await;

    let record = lock
        .read_exact(&fetch.topic, fetch.partition_id, fetch.offset)
        .await?;

    Ok(Json(RecordResponse::from(record, fetch.encoding)?))
}

impl HttpServer {
    pub fn new(host: &str, port: u16, app: App) -> Self {
        let router = Router::new()
            .route("/topics", post(create_topic))
            .route("/topics", get(get_all_topics_state))
            .route("/topics/{name}/state", get(get_topic_state))
            .route("/topics/records", post(produce))
            .route("/topics/records", get(fetch))
            .with_state(app);

        let address = format!("{}:{}", host, port);

        HttpServer { router, address }
    }

    pub async fn serve(self) -> tokio::io::Result<()> {
        let listener = TcpListener::bind(&self.address).await?;

        info!("Starting listener on {}", &self.address);

        axum::serve(listener, self.router)
            .await
            .expect("axum::serve failed");

        Ok(())
    }
}
