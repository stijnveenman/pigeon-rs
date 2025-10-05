use anyhow::Result;
use clap::Parser;
use pigeon_rs::{
    app::App, config::Config, http::HttpServer, logging::set_up_logging, DEFAULT_PORT,
};

#[derive(Parser, Debug)]
#[command(name = "pigeon", version, author, about = "Run pegon server")]
struct Cli {
    #[arg(long)]
    port: Option<u16>,

    #[arg(long, short, action = clap::ArgAction::Count)]
    verbose: u8,

    #[arg(long, short, action = clap::ArgAction::Count)]
    quiet: u8,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let cli = Cli::parse();
    set_up_logging(cli.verbose, cli.quiet)?;

    let config = Config::default();
    let app = App::load_from_disk(config)
        .await
        .expect("Failed to load app state");

    let port = cli.port.unwrap_or(DEFAULT_PORT);

    let http = HttpServer::new("127.0.0.1", port, app);

    http.serve().await.expect("http server failed");

    Ok(())
}
