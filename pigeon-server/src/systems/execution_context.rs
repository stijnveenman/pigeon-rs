use std::{convert::Infallible, sync::Arc};

use axum::{extract::FromRequestParts, http::request::Parts};
use pigeon_core::PError;

use crate::{config::ServerConfig, systems::SystemContext};

#[derive(PartialEq, Eq)]
pub enum User {
    System,
    Anonymous,
}

pub struct ExecutionContext {
    pub config: Arc<ServerConfig>,
    pub system: Arc<SystemContext>,
    pub user: User,
}

impl FromRequestParts<Arc<SystemContext>> for ExecutionContext {
    type Rejection = Infallible;

    async fn from_request_parts(
        _parts: &mut Parts,
        state: &Arc<SystemContext>,
    ) -> Result<Self, Self::Rejection> {
        Ok(ExecutionContext::user(state.clone()))
    }
}

impl ExecutionContext {
    pub fn can_read_topic(&self, _topic_name: &str) -> Result<(), PError> {
        Ok(())
    }

    pub fn can_write_topic(&self, topic_name: &str) -> Result<(), PError> {
        if topic_name.starts_with(".") && self.user != User::System {
            return Err(PError::Unauthorized);
        }
        Ok(())
    }

    pub fn system(system: Arc<SystemContext>) -> Self {
        Self {
            user: User::System,
            config: system.config.clone(),
            system,
        }
    }

    pub fn user(system: Arc<SystemContext>) -> Self {
        Self {
            user: User::Anonymous,
            config: system.config.clone(),
            system,
        }
    }

    pub fn elevate(&self) -> Self {
        Self {
            user: User::System,
            config: self.config.clone(),
            system: self.system.clone(),
        }
    }
}
