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

pub fn http_router() -> Router<Arc<SystemContext>> {
    Router::new()
        .route("/health", get(health))
        .nest("/topics", topics_router::router())
        .nest("/groups", groups_router::router())
        .layer(TraceLayer::new_for_http())
}

pub async fn serve(server: Arc<SystemContext>) -> Result<()> {
    let app = http_router();

    let listener =
        TcpListener::bind((server.config.http.address.as_str(), server.config.http.port)).await?;

    info!("Started HTTP listener on {}", listener.local_addr()?);

    axum::serve(listener, app.with_state(server)).await?;

    Ok(())
}
