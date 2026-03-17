use clap::{Parser, Subcommand};
use pigeon_sdk::{pigeon_sdk::PigeonSdk, rpc::append_record::AppendRecord};

#[derive(Debug, Parser)]
#[command(name = "pg", version, author)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Topic {
        #[command(subcommand)]
        command: TopicCommands,
    },
    Produce {
        topic: String,
        partition: u64,
        key: String,
        value: String,
    },
    Fetch {
        topic: String,
        partition: u64,
        offset: u64,
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
        Commands::Topic { command } => match command {
            TopicCommands::Create { name, partitions } => {
                match sdk.create_topic(&name, partitions).await {
                    Ok(()) => println!("Created topic {name} succesfully"),
                    Err(e) => eprintln!("Error creating topic: {e} [{e:?}]"),
                }
            }
        },
        Commands::Produce {
            topic,
            partition,
            key,
            value,
        } => match sdk
            .append_record(&topic, partition, AppendRecord { key, value })
            .await
        {
            Ok(offset) => println!("Produced record with offset {offset}"),
            Err(e) => eprintln!("Error producing record: {e} [{e:?}]"),
        },
        Commands::Fetch {
            topic,
            partition,
            offset,
        } => match sdk.read_record(&topic, partition, offset).await {
            Ok(e) => println!(
                "Record {:4} {}:{}",
                e.offset,
                e.key_text().unwrap(),
                e.text().unwrap()
            ),
            Err(e) => eprintln!("Error reading record {e}"),
        },
    }
}
