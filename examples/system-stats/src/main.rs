use clap::Parser;
use std::time::Duration;

use anyhow::Result;
use client::http_client::HttpClient;
use shared::{
    commands::produce_command::ProduceCommand,
    consts::DEFAULT_PORT,
    data::{encoding::Encoding, identifier::Identifier, partitioner::Partitioner},
};
use sys_info::{boottime, cpu_speed, disk_info, hostname, loadavg, mem_info, proc_total};
use tokio::time::sleep;

#[derive(Parser, Debug)]
#[command()]
struct Cli {
    #[arg(default_value_t = {"system_stats".to_string()}, short, long)]
    topic: String,
    #[arg(default_value_t = 1000, short, long)]
    sleep_ms: u64,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = HttpClient::new(format!("http://127.0.0.1:{}", DEFAULT_PORT))?;

    let topic = client
        .create_topic_if_not_exists(&cli.topic, Some(10))
        .await?;
    let topic_id = topic.topic_id;
    let partitioner = Partitioner::Default(topic.partitions.len() as u64);

    loop {
        let items = vec![
            ("boottime", boottime()?.tv_sec.to_string()),
            ("disk_free", disk_info()?.free.to_string()),
            ("proc_total", proc_total()?.to_string()),
            ("cpu_speed", cpu_speed()?.to_string()),
            ("hostname", hostname()?.to_string()),
            ("mem_free", mem_info()?.free.to_string()),
            ("load_average:1", loadavg()?.one.to_string()),
            ("load_average:5", loadavg()?.five.to_string()),
            ("load_average:15", loadavg()?.fifteen.to_string()),
        ];

        for item in items {
            produce(&client, topic_id, &partitioner, item.0, item.1).await?;
        }

        println!("Batch produced");
        sleep(Duration::from_millis(cli.sleep_ms)).await;
    }
}

async fn produce(
    client: &HttpClient,
    topic_id: u64,
    partitioner: &Partitioner,
    key: &str,
    value: String,
) -> Result<u64> {
    Ok(client
        .produce(ProduceCommand {
            topic: Identifier::Id(topic_id),
            partition_id: partitioner.select_partition(key.to_string()),
            key: key.into(),
            value,
            encoding: Encoding::Utf8,
            headers: None,
        })
        .await
        .map(|result| result.offset)?)
}
