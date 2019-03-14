use config::{Config, ConfigError, File};
use serde_derive::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct PubsubSettings {
    pub project_id: String,
    pub subscription: String,
    pub topic: String,
}

#[derive(Debug, Deserialize)]
pub struct BigQuerySettings {
    pub project_id: String,
    pub dataset: String,
    pub table: String,
    pub format: String,
    pub delimiter: String,
    pub quote: String,
    pub auto_detect: bool,
    pub allow_jagged_rows: bool,
}

#[derive(Debug, Deserialize)]
pub struct Limits {
    pub pubsub_max_messages: i32,
    pub bigquery_max_messages: u32,
    pub bigquery_time_limit: u64,
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
    pub pubsub: PubsubSettings,
    pub bigquery: BigQuerySettings,
    pub limits: Limits,
}

impl Settings {
    pub fn new(path: &str) -> Result<Settings, ConfigError> {
        let mut cfg = Config::new();
        cfg.merge(File::with_name(path))?;
        cfg.try_into()
    }
}
