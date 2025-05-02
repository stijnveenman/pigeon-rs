use core::str;

use anyhow::Result;
use bytes::Bytes;
use clap::{Args, Parser, Subcommand};
use pigeon_rs::{logging::set_up_logging, Client, DEFAULT_PORT};
use tracing::debug;

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
async fn main() -> Result<()> {
    set_up_logging()?;

    let cli = Cli::parse();

    let addr = format!("{}:{}", cli.host, cli.port);

    let mut client = Client::connect(&addr).await?;

    match cli.command {
        Command::Ping { msg } => {
            let response = client.ping(msg.map(|msg| msg.to_vec())).await?;
            print_result(&response);
        }
        Command::Produce { topic, key, data } => {
            let value = client.produce(topic, key.to_vec(), data.to_vec()).await?;
            println!("produced {}:{}", value.0, value.1);
        }
        Command::Fetch {
            timeout_ms,
            topic,
            partitions,
        } => {}
        Command::Topic { subcommand } => match subcommand {
            TopicCommand::Create { name, partitions } => {
                client.create_topic(name, partitions).await?;
            }
        },
    }

    Ok(())
}

fn print_result(value: &[u8]) {
    if let Ok(string) = str::from_utf8(value) {
        println!("\"{}\"", string);
    } else {
        println!("{:?}", value);
    }
}
