use clap::{Parser, Subcommand};
use pigeon_sdk::{pigeon_sdk::PigeonSdk, rpc::append_record::AppendRecord};

#[derive(Debug, Parser)]
#[command(name = "pg", version, author)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Topic {
        #[command(subcommand)]
        command: TopicCommands,
    },
}

#[derive(Debug, Subcommand)]
enum TopicCommands {
    Create {
        name: String,
        partitions: Option<u64>,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let sdk = PigeonSdk::new("http://localhost:4111");

    match cli.command {
        Command::Topic { command } => match command {
            TopicCommands::Create { name, partitions } => {
                match sdk.create_topic(&name, partitions).await {
                    Ok(()) => println!("Created topic {name} succesfully"),
                    Err(e) => eprintln!("Error creating topic: {e} [{e:?}]"),
                }
            }
        },
    }
}
