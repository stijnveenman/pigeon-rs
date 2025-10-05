mod app_error;
pub mod responses;

use app_error::AppResult;
use axum::extract::State;
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

    let topic_id = lock.create_topic(create_topic).await?;

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
        .produce(produce.topic_id, produce.partition_id, record)
        .await?;

    Ok(Json(ProduceResponse { offset }))
}

impl HttpServer {
    pub fn new(host: &str, port: u16, app: App) -> Self {
        let router = Router::new()
            .route("/", get(|| async { "Hello world" }))
            .route("/topics", post(create_topic))
            .route("/topics/records", post(produce))
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
