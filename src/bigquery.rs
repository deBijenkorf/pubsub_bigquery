use std::fs::{File, OpenOptions, remove_file};
use std::io::{self, Write};

use google_bigquery2::{
    Error,
    Job,
    JobConfiguration,
    JobConfigurationLoad,
    TableReference,
};
use log::info;

use crate::auth::Authenticator;
use crate::handler::{Handler, HandlingError, MessageCounter, HandlingResult};
use crate::settings::BigQuerySettings;

pub struct BigQuerySink {
    bigquery: BigQuerySettings,
    counter: MessageCounter,
    client: BigQueryClient<'static>,
}

type BigQueryClient<'a> =
google_bigquery2::Bigquery<hyper::Client, oauth::ServiceAccountAccess<hyper::Client>>;

impl From<&BigQuerySettings> for JobConfigurationLoad {
    fn from(custom: &BigQuerySettings) -> Self {
        let table = TableReference {
            project_id: Some(custom.project_id.to_string()),
            table_id: Some(custom.table.to_string()),
            dataset_id: Some(custom.dataset.to_string()),
        };

        let mut config = JobConfigurationLoad::default();
        config.autodetect = Some(custom.auto_detect);
        config.allow_jagged_rows = Some(custom.allow_jagged_rows);
        config.quote = Some(custom.quote.to_string());
        config.source_format = Some(custom.format.to_string());
        config.field_delimiter = Some(custom.delimiter.to_string());
        config.destination_table = Some(table);
        config
    }
}

impl BigQuerySink {
    pub fn new(bigquery: BigQuerySettings, counter: MessageCounter, auth: Authenticator) -> Self {
        let client = google_bigquery2::Bigquery::new(auth.client, auth.access);
        BigQuerySink {
            bigquery,
            counter,
            client,
        }
    }

    fn generate_job(&self) -> Job {
        let mut job_config = JobConfiguration::default();
        job_config.load = Some(JobConfigurationLoad::from(&self.bigquery));

        let mut job = Job::default();
        job.configuration = Some(job_config);
        job
    }

    pub fn upload_csv(&self, path: &str) -> Result<(), Error> {
        let file = File::open(path).unwrap();
        self.client
            .jobs()
            .insert(BigQuerySink::generate_job(&self), &self.bigquery.project_id)
            .upload(file, "text/csv".parse().unwrap())
            .and_then(move |(response, job)| {
                if response.status.is_success() && job.status.unwrap().error_result.is_none() {
                    info!(
                        "upload of csv with name: {} has status: {}",
                        &self.counter.current_file, response.status
                    );
                    Ok(())
                } else {
                    Err(Error::Failure(response))
                }
            })
    }
}

impl Handler for BigQuerySink {
    fn handle(&mut self, messages: Vec<String>) -> Result<HandlingResult, HandlingError> {
        let message_count = messages.len();

        if message_count > 0 {
            self.counter.increase(messages.len() as u32);
            write_to_file(&self.counter.current_file.as_ref(), messages)?;
        }

        if self.counter.reached_threshold() {
            self.upload_csv(&self.counter.current_file)?;
            remove_file(&self.counter.current_file)?;
            self.counter.reset();
            return Ok(true);
        }
        Ok(false)
    }
}

//todo: move this elsewhere
fn write_to_file(file_name: &str, messages: Vec<String>) -> io::Result<File> {
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(file_name)?;

    for msg in messages {
        writeln!(file, "{}", msg)?;
    }
    Ok(file)
}
