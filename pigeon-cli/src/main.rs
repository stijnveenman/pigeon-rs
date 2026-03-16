use pigeon_sdk::pigeon_sdk::PigeonSdk;

#[tokio::main]
async fn main() {
    let sdk = PigeonSdk::new("http://localhost:4111");

    let result = sdk.create_topic("foobar", None).await;
    dbg!(result);

    let result = sdk.read_record("foo", 0, 5).await;
    dbg!(result.unwrap());
}
