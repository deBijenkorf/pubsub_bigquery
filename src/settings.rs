use config::{Config, ConfigError};
use config::File;
use serde_derive::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Google {
    pub project_id: String,
    pub auth_key_file: String,
    pub pubsub_subscription: String,
    pub pubsub_topic: String,
    pub bigquery_dataset: String,
    pub bigquery_table: String,
}

#[derive(Debug, Deserialize)]
pub struct Limits {
    pub pubsub_max_messages: i32,
    pub handler_max_messages: u32,
}

#[derive(Debug, Deserialize)]
pub enum Mode {
    Publish,
    Subscribe,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub debug: bool,
    pub mode: Mode,
    pub google: Google,
    pub limits: Limits,
}

impl Settings {
    pub fn new(path: &str) -> Result<Settings, ConfigError> {
        let mut cfg = Config::new();
        cfg.merge(File::with_name(path))?;
        cfg.try_into()
    }
}
