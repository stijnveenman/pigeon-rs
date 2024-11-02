use core::str;

use bytes::Bytes;
use clap::{Args, Parser, Subcommand};
use pigeon_rs::{
    logging::set_up_logging, Client, FetchConfig, FetchPartitionConfig, FetchTopicConfig,
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
}

#[derive(Subcommand, Debug)]
enum Command {
    Ping {
        /// Message to ping
        msg: Option<Bytes>,
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

        #[arg(id = "partition", long, short='p', num_args(1..),required=true)]
        partitions: Vec<u64>,
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
}

/// Entry point for CLI tool.
///
/// `flavor = "current_thread"` is used here to avoid spawning background
/// threads. The CLI tool use case benefits more by being lighter instead of
/// multi-threaded.
#[tokio::main(flavor = "current_thread")]
async fn main() -> pigeon_rs::Result<()> {
    set_up_logging()?;

    let cli = Cli::parse();

    let addr = format!("{}:{}", cli.host, cli.port);

    let mut client = Client::connect(&addr).await?;

    match cli.command {
        Command::Ping { msg } => {
            let value = client.ping(msg).await?;
            print_result(&value)
        }
        Command::Produce { topic, key, data } => {
            let value = client.produce(topic, key, data).await?;
            println!("partiton: {} offset {}", value.0, value.1)
        }
        Command::Fetch {
            timeout_ms,
            topic,
            partitions,
        } => {
            let config = FetchConfig {
                timeout_ms,
                topics: vec![FetchTopicConfig {
                    topic,
                    partitions: partitions
                        .into_iter()
                        .map(|p| FetchPartitionConfig {
                            partition: p,
                            offset: 3,
                        })
                        .collect(),
                }],
            };

            let value = client.cfetch(config).await?;
            println!("fetched: {:?}", value)
        }
        Command::Topic { subcommand } => match subcommand {
            TopicCommand::Create { name, partitions } => {
                let value = client.create_topic(name, partitions).await?;
                print_result(&value)
            }
        },
    }

    Ok(())
}

fn print_result(value: &Bytes) {
    if let Ok(string) = str::from_utf8(value) {
        println!("\"{}\"", string);
    } else {
        println!("{:?}", value);
    }
}
