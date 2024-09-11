use pigeon_rs::Client;

#[tokio::main]
async fn main() {
    let client = match Client::connect("localhost:6394").await {
        Ok(client) => client,
        Err(_) => panic!("failed to establish connection"),
    };
}
