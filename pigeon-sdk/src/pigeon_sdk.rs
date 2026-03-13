use reqwest::{Client, ClientBuilder, Method, Request, Url};

pub struct PigeonSdk {
    pub base_url: Url,
    client: Client,
}

impl PigeonSdk {
    fn request(&self, method: Method, path: &str) -> Request {
        Request::new(method, self.base_url.join(path).unwrap())
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
}
