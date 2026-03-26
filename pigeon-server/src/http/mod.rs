mod error;
mod groups_router;
mod topics_router;

use std::sync::Arc;

use anyhow::Result;
use axum::{Router, routing::get};
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::systems::SystemContext;

async fn health() -> &'static str {
    "OK"
}

pub async fn serve(server: Arc<SystemContext>) -> Result<()> {
    let app = Router::new()
        .route("/health", get(health))
        .nest("/topics", topics_router::router())
        .nest("/groups", groups_router::router())
        .with_state(server.clone())
        .layer(TraceLayer::new_for_http());

    let listener =
        TcpListener::bind((server.config.http.address.as_str(), server.config.http.port)).await?;

    info!("Started HTTP listener on {}", listener.local_addr()?);

    axum::serve(listener, app).await?;

    Ok(())
}
