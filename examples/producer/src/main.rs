use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use client::http_client::HttpClient;
use shared::{
    commands::produce_command::ProduceCommand,
    consts::DEFAULT_PORT,
    data::{encoding::Encoding, identifier::Identifier},
};
use tokio::time::sleep;

#[derive(Parser, Debug)]
#[command()]
struct Cli {
    #[arg(default_value_t = {"bar".to_string()}, short, long)]
    topic: String,
    #[arg(default_value_t = 1000, short, long)]
    sleep_ms: u64,
    #[arg(default_value_t = 0, short, long)]
    partition_id: u64,
    #[arg(short, long)]
    create: bool,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = HttpClient::new(format!("http://127.0.0.1:{}", DEFAULT_PORT))?;

    if cli.create {
        println!("Creating topic if not existing");
        let _ = client
            .create_topic(&cli.topic, Some(cli.partition_id.max(1)))
            .await;
    }

    let identifier = Identifier::Name(cli.topic);
    let mut idx = 0;
    loop {
        let response = client
            .produce(ProduceCommand {
                topic: identifier.clone(),
                partition_id: 0,
                key: format!("{}", idx),
                value: format!("Idx: {}", idx),
                encoding: Encoding::Utf8,
                headers: None,
            })
            .await;

        idx += 1;
        match response {
            Ok(e) => println!("Produced: {}", e.offset),
            Err(e) => println!("Error: {e}"),
        };

        sleep(Duration::from_millis(cli.sleep_ms)).await;
    }
}
