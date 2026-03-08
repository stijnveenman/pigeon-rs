use anyhow::Result;
use axum::{Router, routing::get};
use tokio::net::TcpListener;
use tracing::info;

use crate::config::ServerConfig;

async fn health() -> &'static str {
    "OK"
}

pub async fn serve(config: &ServerConfig) -> Result<()> {
    let app = Router::new().route("/health", get(health));

    let listener = TcpListener::bind((config.http.address.as_str(), config.http.port)).await?;

    info!("Started HTTP listener on {}", listener.local_addr()?);

    axum::serve(listener, app).await?;

    Ok(())
}

