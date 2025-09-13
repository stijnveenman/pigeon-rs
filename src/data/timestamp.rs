use core::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Timestamp(SystemTime);

pub const UTC_TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

impl Timestamp {
    pub fn now() -> Self {
        Self::default().as_micros().into()
    }

    pub fn to_utc_string(self, format: &str) -> String {
        DateTime::<Utc>::from(self.0).format(format).to_string()
    }

    pub fn to_secs(self) -> u64 {
        self.0.duration_since(UNIX_EPOCH).unwrap().as_secs()
    }

    pub fn as_micros(&self) -> u64 {
        self.0.duration_since(UNIX_EPOCH).unwrap().as_micros() as u64
    }
}

impl Default for Timestamp {
    fn default() -> Self {
        Timestamp(SystemTime::now())
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_utc_string(UTC_TIME_FORMAT))
    }
}

impl From<u64> for Timestamp {
    fn from(timestamp: u64) -> Self {
        Timestamp(UNIX_EPOCH + Duration::from_micros(timestamp))
    }
}

impl From<Timestamp> for u64 {
    fn from(timestamp: Timestamp) -> u64 {
        timestamp.as_micros()
    }
}

impl From<SystemTime> for Timestamp {
    fn from(timestamp: SystemTime) -> Self {
        Timestamp(timestamp)
    }
}
