pub mod responses;

use axum::routing::{get, post};
use axum::{Json, Router};
use responses::create_topic_response::CreateTopicResponse;
use tokio::io::Result;
use tokio::net::TcpListener;
use tracing::info;

use crate::commands::create_topic::CreateTopic;

pub struct HttpServer {
    app: Router,
    address: String,
}

async fn create_topic(Json(create_topic): Json<CreateTopic>) -> Json<CreateTopicResponse> {
    Json(CreateTopicResponse { topic_id: 2 })
}

impl HttpServer {
    pub fn new(host: &str, port: u16) -> Self {
        let app = Router::new()
            .route("/", get(|| async { "Hello world" }))
            .route("/topics", post(create_topic));

        let address = format!("{}:{}", host, port);

        HttpServer { app, address }
    }

    pub async fn serve(self) -> Result<()> {
        let listener = TcpListener::bind(&self.address).await?;

        info!("Starting listener on {}", &self.address);

        axum::serve(listener, self.app)
            .await
            .expect("axum::serve failed");

        Ok(())
    }
}
