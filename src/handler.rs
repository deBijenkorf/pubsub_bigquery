use std::error;
use std::fmt;
use std::io;
use std::time::{Duration, SystemTime};
use uuid::Uuid;

use crate::settings::Limits;

#[derive(Debug, Clone)]
pub struct HandlingError {
    pub kind: String,
    pub message: String,
}

pub type HandlingResult = bool;

impl error::Error for HandlingError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

impl fmt::Display for HandlingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "error: {}, kind: {}", &self.message, &self.kind)
    }
}

impl From<io::Error> for HandlingError {
    fn from(error: io::Error) -> Self {
        HandlingError {
            kind: String::from("io"),
            message: error.to_string(),
        }
    }
}

impl From<google_bigquery2::Error> for HandlingError {
    fn from(error: google_bigquery2::Error) -> Self {
        let mut message = String::from("custom: ");
        message.push_str(&error.to_string());
        HandlingError {
            kind: String::from("google_bigquery"),
            message,
        }
    }
}

pub struct MessageCounter {
    pub max_messages: u32,
    pub max_duration: u64,
    pub current_messages: u32,
    pub current_file: String,
    pub current_checkpoint: SystemTime,
}

impl MessageCounter {
    pub fn new(limits: Limits) -> Self {
        MessageCounter {
            max_messages: limits.bigquery_max_messages,
            max_duration: limits.bigquery_time_limit,
            current_messages: 0,
            current_file: Uuid::new_v4().to_string(),
            current_checkpoint: SystemTime::now(),
        }
    }

    pub fn increase(&mut self, amount: u32) {
        self.current_messages += amount;
    }

    pub fn reached_threshold(&self) -> bool {
        self.current_messages >= self.max_messages
            || self.current_checkpoint.elapsed().unwrap() >= Duration::from_secs(self.max_duration)
    }

    pub fn reset(&mut self) {
        self.current_messages = 0;
        self.current_checkpoint = SystemTime::now();
        self.current_file = Uuid::new_v4().to_string();
    }
}

pub trait Handler {
    fn handle(&mut self, messages: Vec<String>) -> Result<HandlingResult, HandlingError>;
}
