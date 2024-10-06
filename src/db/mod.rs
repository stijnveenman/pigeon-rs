use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub(crate) struct Db {
    shared: Arc<Mutex<State>>,
}

#[derive(Debug)]
struct State {}

impl Db {
    pub(crate) fn new() -> Db {
        Db {
            shared: Arc::new(Mutex::new(State {})),
        }
    }
}
