pub mod responses;

use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use responses::create_topic_response::CreateTopicResponse;
use tokio::io::Result;
use tokio::net::TcpListener;
use tracing::info;

use crate::app::App;
use crate::commands::create_topic::CreateTopic;

pub struct HttpServer {
    router: Router,
    address: String,
}

async fn create_topic(
    State(app): State<App>,
    Json(create_topic): Json<CreateTopic>,
) -> Json<CreateTopicResponse> {
    let mut lock = app.write().await;

    // TODO: proper axum error handling
    let topic_id = lock
        .create_topic(create_topic)
        .await
        .expect("Failed to create_topic");

    Json(CreateTopicResponse { topic_id })
}

impl HttpServer {
    pub fn new(host: &str, port: u16, app: App) -> Self {
        let router = Router::new()
            .route("/", get(|| async { "Hello world" }))
            .route("/topics", post(create_topic))
            .with_state(app);

        let address = format!("{}:{}", host, port);

        HttpServer { router, address }
    }

    pub async fn serve(self) -> Result<()> {
        let listener = TcpListener::bind(&self.address).await?;

        info!("Starting listener on {}", &self.address);

        axum::serve(listener, self.router)
            .await
            .expect("axum::serve failed");

        Ok(())
    }
}
