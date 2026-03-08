mod error;

use std::sync::Arc;

use anyhow::Result;
use axum::{Json, Router, routing::get};
use pigeon_core::PError;
use tokio::net::TcpListener;
use tracing::info;

use crate::{http::error::HttpResult, systems::SystemContext};

async fn health() -> &'static str {
    "OK"
}

async fn create_topic() -> HttpResult<String> {
    None.ok_or(PError::IndexParseFailed)?;

    Ok(Json("foo".to_string()))
}

pub async fn serve(server: Arc<SystemContext>) -> Result<()> {
    let app = Router::new()
        .route("/health", get(health))
        .route("/topics", get(create_topic))
        .with_state(server.clone());

    let listener =
        TcpListener::bind((server.config.http.address.as_str(), server.config.http.port)).await?;

    info!("Started HTTP listener on {}", listener.local_addr()?);

    axum::serve(listener, app).await?;

    Ok(())
}
