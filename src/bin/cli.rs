use core::str;

use anyhow::Result;
use bytes::Bytes;
use clap::{Args, Parser, Subcommand};
use pigeon_rs::{
    client::{self},
    fetch,
    logging::set_up_logging,
    DEFAULT_PORT,
};

#[derive(Parser, Debug)]
#[command(
    name = "mini-redis-cli",
    version,
    author,
    about = "Issue Redis commands"
)]
struct Cli {
    #[clap(subcommand)]
    command: Command,

    #[arg(id = "hostname", long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,

    #[arg(long, short, action = clap::ArgAction::Count)]
    verbose: u8,

    #[arg(long, short, action = clap::ArgAction::Count)]
    quiet: u8,
}

#[derive(Subcommand, Debug)]
enum Command {
    Ping {
        /// Message to ping
        msg: Option<String>,
    },
    Topic {
        #[clap(subcommand)]
        subcommand: TopicCommand,
    },
    Produce {
        topic: String,
        key: Bytes,
        data: Bytes,
    },
    Fetch {
        #[arg(long, default_value_t = 1000)]
        timeout_ms: u64,

        #[arg(long, short = 't')]
        topic: String,

        #[arg(long, short = 'p')]
        partition: u64,

        #[arg(long, short = 'o', default_value_t = 0)]
        offset: u64,
    },
    Consume {
        #[arg(long, short = 't')]
        topic: String,
    },
}

#[derive(Debug, Args, Clone)]
struct TopicParameters {
    name: String,
    partition: u64,
}

#[derive(Subcommand, Debug)]
enum TopicCommand {
    Create {
        /// Name of topic to create
        name: String,
        /// Number of partitions to create
        partitions: u64,
    },
    Describe {
        /// Name of the topic to describe
        name: String,
    },
}

/// Entry point for CLI tool.
///
/// `flavor = "current_thread"` is used here to avoid spawning background
/// threads. The CLI tool use case benefits more by being lighter instead of
/// multi-threaded.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    set_up_logging(cli.verbose, cli.quiet)?;

    let addr = format!("{}:{}", cli.host, cli.port);

    let mut client = client::connect(&addr).await?;

    match cli.command {
        Command::Ping { msg } => {
            let response = client.ping(msg.as_deref()).await?;
            println!("{:?}", response);
        }
        Command::Produce { topic, key, data } => {
            let value = client.produce(topic, key.to_vec(), data.to_vec()).await?;
            println!("produced {}:{}", value.0, value.1);
        }
        Command::Fetch {
            timeout_ms,
            topic,
            partition,
            offset,
        } => {
            let request = fetch::Request {
                timeout_ms,
                topics: vec![fetch::TopicsRequest {
                    topic,
                    partitions: vec![fetch::PartitionRequest { partition, offset }],
                }],
            };

            let value = client.fetch(request).await;
            println!("fetched {:?}", value)
        }
        Command::Topic { subcommand } => match subcommand {
            TopicCommand::Create { name, partitions } => {
                client.create_topic(name, partitions).await?;
            }
            TopicCommand::Describe { name } => {
                let result = client.describe_topic(name).await?;

                println!("{:#?}", result);
            }
        },
        Command::Consume { topic } => {
            let mut consumer = client::consumer(client, topic).await?;

            while let Ok(message) = consumer.next_message().await {
                println!("fetched {:?}", message)
            }
        }
    }

    Ok(())
}
