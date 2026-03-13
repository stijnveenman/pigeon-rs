use reqwest::{Client, ClientBuilder, Method, Request, Url};

pub struct PigeonSdk {
    pub base_url: Url,
    client: Client,
}

impl PigeonSdk {
    fn request(&self, method: Method, path: &str) -> Request {
        Request::new(method, self.base_url.join(path).unwrap())
    }

    fn get(&self, path: &str) -> Request {
        self.request(Method::GET, path)
    }

    pub fn new(base_url: impl Into<String>) -> Self {
        let mut base_url = base_url.into();
        if !base_url.ends_with("/") {
            base_url += "/";
        }

        Self {
            base_url: Url::parse(&base_url).unwrap(),
            client: ClientBuilder::new()
                .user_agent("PigeonSdk")
                .build()
                .unwrap(),
        }
    }

    pub async fn read_record(&self, topic_name: &str, partition_id: u64, offset: u64) {
        let response = self
            .client
            .execute(self.get(&format!("/topics/{topic_name}/{partition_id}/{offset}")))
            .await
            .unwrap();

        dbg!(&response);
        dbg!(response.text().await);
    }
}
