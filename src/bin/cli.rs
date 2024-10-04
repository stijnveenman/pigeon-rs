use pigeon_rs::{logging::set_up_logging, Client};

#[tokio::main]
async fn main() -> pigeon_rs::Result<()> {
    set_up_logging()?;

    let mut client = match Client::connect("localhost:6394").await {
        Ok(client) => client,
        Err(_) => panic!("failed to establish connection"),
    };

    Ok(())
}
