use std::collections::HashMap;

use anyhow::Result;
use clap::{Parser, Subcommand};
use pigeon_rs::{
    client::HttpClient,
    commands::{
        fetch::{Fetch, FetchPartition, FetchTopic},
        produce::Produce,
    },
    data::{encoding::Encoding, identifier::Identifier, offset_selection::OffsetSelection},
    logging::set_up_logging,
    DEFAULT_PORT,
};
use tracing::{debug, info};

#[derive(Parser, Debug)]
#[command(name = "pigeon-cli", version, author, about = "Run pegon server")]
struct Cli {
    #[clap(subcommand)]
    command: Command,

    #[arg(long, short, action = clap::ArgAction::Count)]
    verbose: u8,

    #[arg(long, short, action = clap::ArgAction::Count)]
    quiet: u8,
}

#[derive(Subcommand, Debug)]
enum Command {
    Topics {
        #[clap(subcommand)]
        subcommand: TopicCommand,
    },
    Produce {
        name: String,
        partition_id: u64,
        key: String,
        value: String,
    },
    Fetch {
        topic: String,
        partition: u64,
        start_offset: u64,
        #[arg(default_value_t = 10000)]
        timeout_ms: u64,
    },
}

#[derive(Subcommand, Debug)]
enum TopicCommand {
    State {
        topic: String,
    },
    List,
    Delete {
        topic: String,
    },
    Create {
        name: String,
        partitions: Option<u64>,
    },
    Listen {
        topic: String,
        #[arg(default_value_t = 10000)]
        timeout_ms: u64,
        #[arg(default_value_t = 20)]
        min_bytes: usize,
        #[arg(short = 'b', long)]
        from_beginning: bool,
    },
}

async fn listen_to_topic(
    client: &HttpClient,
    name: &str,
    from_beginning: bool,
    timeout_ms: u64,
    min_bytes: usize,
) -> Result<()> {
    let state = client.get_topic(name).await?;

    debug!("Got existing topic state {state:#?}");
    let partition_offsets = state
        .partitions
        .iter()
        .map(|partition| {
            (
                partition.partition_id,
                match from_beginning {
                    true => 0,
                    false => partition.current_offset,
                },
            )
        })
        .collect::<HashMap<_, _>>();
    debug!("Partition offset: {partition_offsets:#?}");

    loop {
        let response = client
            .fetch(Fetch {
                encoding: Encoding::Utf8,
                topics: vec![FetchTopic {
                    identifier: Identifier::Id(state.topic_id),
                    partitions: partition_offsets
                        .iter()
                        .map(|p| FetchPartition {
                            id: *p.0,
                            offset: OffsetSelection::From(*p.1),
                        })
                        .collect(),
                }],
                timeout_ms,
                min_bytes,
            })
            .await?;

        for record in response.records {
            // TODO: we need partition to updat offset and add to log
            info!(
                "{} {}:{} - {} - {}",
                name, 0, record.offset, record.key, record.value
            );
        }
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let cli = Cli::parse();
    set_up_logging(cli.verbose, cli.quiet)?;

    let client = HttpClient::new(format!("http://127.0.0.1:{}", DEFAULT_PORT))?;

    match cli.command {
        Command::Topics { subcommand } => {
            match subcommand {
                TopicCommand::State { topic } => {
                    let state = client.get_topic(&topic).await?;
                    info!("{state:#?}");
                }
                TopicCommand::Delete { topic } => {
                    client.delete_topic(&topic).await?;
                }
                TopicCommand::List => {
                    let state = client.get_topics().await?;
                    info!("{state:#?}");
                }
                TopicCommand::Create { name, partitions } => {
                    let result = client.create_topic(&name, partitions).await?;

                    info!("Created topic with id {}", result.topic_id);
                }
                TopicCommand::Listen {
                    topic,
                    timeout_ms,
                    min_bytes,
                    from_beginning,
                } => {
                    listen_to_topic(&client, &topic, from_beginning, timeout_ms, min_bytes).await?;
                }
            };
        }
        Command::Produce {
            name,
            partition_id,
            key,
            value,
        } => {
            let response = client
                .produce(Produce {
                    topic: Identifier::Name(name),
                    partition_id,
                    key,
                    value,
                    encoding: Encoding::Utf8,
                    headers: None,
                })
                .await?;

            info!("Produced offset {}", response.offset);
        }
        Command::Fetch {
            topic,
            partition,
            start_offset,
            timeout_ms,
        } => {
            let response = client
                .fetch(Fetch {
                    encoding: Encoding::Utf8,
                    timeout_ms,
                    min_bytes: 0,
                    topics: vec![FetchTopic {
                        identifier: Identifier::Name(topic),
                        partitions: vec![FetchPartition {
                            id: partition,
                            offset: OffsetSelection::From(start_offset),
                        }],
                    }],
                })
                .await;

            info!("{:#?}", response);
        }
    };

    Ok(())
}
