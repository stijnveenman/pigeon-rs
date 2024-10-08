mod topics;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use strum_macros::EnumString;
use topics::Topic;

#[derive(Debug, PartialEq, EnumString, strum_macros::Display)]
pub enum DbErr {
    NameInUse,
}

#[derive(Debug, Clone)]
pub(crate) struct Db {
    shared: Arc<Mutex<State>>,
}

#[derive(Debug, Default)]
struct State {
    topics: HashMap<String, Topic>,
}

impl Db {
    pub(crate) fn new() -> Db {
        Db {
            shared: Arc::new(Mutex::new(State::default())),
        }
    }
}
