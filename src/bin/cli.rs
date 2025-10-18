use anyhow::Result;
use clap::{Parser, Subcommand};
use pigeon_rs::{client::HttpClient, logging::set_up_logging, DEFAULT_PORT};
use tracing::info;

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
}

#[derive(Subcommand, Debug)]
enum TopicCommand {
    State { topic: String },
    List,
    Delete { topic: String },
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
            };
        }
    };

    Ok(())
}
