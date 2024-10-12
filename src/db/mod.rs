mod topics;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use strum_macros::EnumString;

pub use topics::Message;
use topics::Topic;

#[derive(Debug, PartialEq, EnumString, strum_macros::Display)]
pub enum DbErr {
    NameInUse,
    NotFound,
}

pub type DbResult<T> = Result<T, DbErr>;

#[derive(Clone)]
pub(crate) struct Db {
    shared: Arc<Mutex<State>>,
}

#[derive(Default)]
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
