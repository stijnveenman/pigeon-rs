use anyhow::Result;
use clap::{Parser, Subcommand};
use client::http_client::HttpClient;
use shared::{
    commands::{
        fetch_command::{FetchCommand, FetchPartitionCommand, FetchTopicCommand},
        produce_command::ProduceCommand,
    },
    consts::DEFAULT_PORT,
    data::{encoding::Encoding, identifier::Identifier, offset_selection::OffsetSelection},
    logging::set_up_logging,
};
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "pigeon-cli", version, author, about = "Pigeon-rs CLI")]
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
            };
        }
        Command::Produce {
            name,
            partition_id,
            key,
            value,
        } => {
            let response = client
                .produce(ProduceCommand {
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
                .fetch(FetchCommand {
                    encoding: Encoding::Utf8,
                    timeout_ms,
                    max_bytes: None,
                    min_bytes: 0,
                    topics: vec![FetchTopicCommand {
                        identifier: Identifier::Name(topic),
                        partitions: vec![FetchPartitionCommand {
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
