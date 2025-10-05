use axum::routing::get;
use axum::Router;
use tokio::io::Result;
use tokio::net::TcpListener;
use tracing::info;

pub struct HttpServer {
    app: Router,
    address: String,
}

impl HttpServer {
    pub fn new(host: &str, port: u16) -> Self {
        let app = Router::new().route("/", get(|| async { "Hello world" }));

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
