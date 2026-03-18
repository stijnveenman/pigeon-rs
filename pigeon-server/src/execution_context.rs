use pigeon_core::PError;

#[derive(PartialEq, Eq)]
pub enum User {
    System,
    Anonymous,
}

pub struct ExecutionContext {
    pub user: User,
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self {
            user: User::Anonymous,
        }
    }
}

impl ExecutionContext {
    pub fn can_read_topic(&self, topic_name: &str) -> Result<(), PError> {
        Ok(())
    }

    pub fn can_write_topic(&self, topic_name: &str) -> Result<(), PError> {
        if topic_name.starts_with(".") && self.user != User::System {
            return Err(PError::Unauthorized);
        }
        Ok(())
    }

    pub fn system() -> Self {
        Self { user: User::System }
    }
}
