mod app_error;
pub mod responses;

use app_error::AppResult;
use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use responses::create_topic_response::CreateTopicResponse;
use responses::produce_response::ProduceResponse;
use tokio::net::TcpListener;
use tracing::info;

use crate::app::App;
use crate::commands::create_topic::CreateTopic;
use crate::commands::produce::ProduceString;
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
) -> AppResult<Json<CreateTopicResponse>> {
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
    Json(produce): Json<ProduceString>,
) -> AppResult<Json<ProduceResponse>> {
    let mut lock = app.write().await;

    let record = Record {
        key: produce.key.into(),
        value: produce.value.into(),
        timestamp: Timestamp::now(),
        offset: 0,
        headers: vec![],
    };

    let offset = lock
        .produce(produce.topic, produce.partition_id, record)
        .await?;

    Ok(Json(ProduceResponse { offset }))
}

async fn get_state(
    State(app): State<App>,
    Path(name): Path<String>,
) -> AppResult<Json<TopicState>> {
    let lock = app.read().await;

    let state = lock.get_topic_by_name(&name)?.state();

    Ok(Json(state))
}

impl HttpServer {
    pub fn new(host: &str, port: u16, app: App) -> Self {
        let router = Router::new()
            .route("/topics", post(create_topic))
            .route("/topics/records", post(produce))
            .route("/topics/{identifier}/state", get(get_state))
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
