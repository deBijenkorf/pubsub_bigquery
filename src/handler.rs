use std::time::{Duration, SystemTime};
use uuid::Uuid;

use crate::settings::Limits;

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
    fn handle(&mut self, messages: Vec<String>) -> bool;
}
