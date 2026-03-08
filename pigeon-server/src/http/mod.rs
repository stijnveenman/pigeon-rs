use std::sync::Arc;

use anyhow::Result;
use axum::{Router, routing::get};
use tokio::net::TcpListener;
use tracing::info;

use crate::systems::SystemContext;

async fn health() -> &'static str {
    "OK"
}

pub async fn serve(server: Arc<SystemContext>) -> Result<()> {
    let app = Router::new()
        .route("/health", get(health))
        .with_state(server.clone());

    let listener =
        TcpListener::bind((server.config.http.address.as_str(), server.config.http.port)).await?;

    info!("Started HTTP listener on {}", listener.local_addr()?);

    axum::serve(listener, app).await?;

    Ok(())
}
