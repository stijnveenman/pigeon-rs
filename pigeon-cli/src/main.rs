use clap::{Parser, Subcommand};
use pigeon_sdk::{pigeon_sdk::PigeonSdk, rpc::append_record::AppendRecord};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "pg", version, author)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Topics {
        #[command(subcommand)]
        command: TopicCommands,
    },
    Groups {
        #[command(subcommand)]
        command: GroupsCommands,
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
    Delete {
        name: String,
    },
}

#[derive(Debug, Subcommand)]
enum GroupsCommands {
    Create {
        group_id: String,
    },
    Join {
        group_id: String,
        consumer_id: Option<String>,
        epoch: Option<usize>,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let sdk = PigeonSdk::new("http://localhost:4111");

    match cli.command {
        Commands::Topics { command } => match command {
            TopicCommands::Create { name, partitions } => {
                match sdk.create_topic(&name, partitions).await {
                    Ok(()) => println!("Created topic {name} succesfully"),
                    Err(e) => eprintln!("Error creating topic: {e} [{e:?}]"),
                }
            }
            TopicCommands::Delete { name } => match sdk.delete_topic(&name).await {
                Ok(()) => println!("Deleted topic {name} succesfully"),
                Err(e) => eprintln!("Error deleting topic: {e} [{e:?}]"),
            },
        },
        Commands::Groups { command } => match command {
            GroupsCommands::Create { group_id } => {
                match sdk.create_consumer_group(&group_id).await {
                    Ok(()) => println!("Created group {group_id} succesfully"),
                    Err(e) => eprintln!("Error creating group: {e} [{e:?}]"),
                }
            }
            GroupsCommands::Join {
                group_id,
                consumer_id,
                epoch,
            } => match sdk
                .join_consumer_group(
                    &group_id,
                    &consumer_id.unwrap_or(Uuid::new_v4().to_string()),
                    epoch,
                )
                .await
            {
                Ok(epoch) => println!("Joined group {group_id} with epoch {epoch}"),
                Err(e) => println!("Error joining group: {e}"),
            },
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
