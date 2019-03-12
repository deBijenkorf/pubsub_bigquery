use std::fs::{File, OpenOptions, remove_file};
use std::io::Write;

use google_bigquery2::{
    Job,
    JobConfiguration,
    JobConfigurationLoad,
    TableReference,
};
use google_bigquery2::Error;
use log::{error, info};
use oauth::ServiceAccountAccess;

use crate::auth::Authenticator;
use crate::auth::Connector;
use crate::handler::Handler;
use crate::handler::MessageCounter;
use crate::settings::GoogleSettings;
use hyper::Response;

pub struct BigQuerySink<'a> {
    pub google: GoogleSettings,
    pub delimiter: &'a str,
    counter: MessageCounter,
    client: BigQueryClient<'a>,
}

type BigQueryClient<'a> = google_bigquery2::Bigquery<Connector, ServiceAccountAccess<Connector>>;

impl<'a> BigQuerySink<'a> {
    pub fn new(google: GoogleSettings, delimiter: &'a str, counter: MessageCounter, auth: Authenticator) -> Self {
        let client = google_bigquery2::Bigquery::new(auth.client, auth.access);
        BigQuerySink { google, delimiter, counter, client }
    }

    fn table_reference(&self) -> TableReference {
        TableReference {
            project_id: Some(self.google.project_id.to_string()),
            table_id: Some(self.google.bigquery_table.to_string()),
            dataset_id: Some(self.google.bigquery_dataset.to_string()),
        }
    }

    fn generate_job(&self) -> Job {
        let table = BigQuerySink::table_reference(&self);

        let mut load_config = JobConfigurationLoad::default();

        load_config.autodetect = Some(true);
        load_config.quote = Some(String::from(""));
        load_config.destination_table = Some(table);
        load_config.field_delimiter = Some(self.delimiter.to_string());
        let mut job = Job::default();

        let mut job_config = JobConfiguration::default();
        job_config.load = Some(load_config);

        job.configuration = Some(job_config);
        job
    }

    pub fn upload_csv(&self, path: &str) {
        let res: &Result<(Response, Job), Error> = &self.client.jobs()
            .insert(BigQuerySink::generate_job(&self), &self.google.project_id)
            .upload(File::open(path).unwrap(), "text/csv".parse().unwrap());

        match res {
            Err(err) => error!("{}", err.to_string()),
            Ok((resp, job)) => {
                info!("http status: {:?}, job status: {:?}", &resp.status, &job.status);
            }
        }
    }
}

impl<'a> Handler for BigQuerySink<'a> {
    fn handle(&mut self, messages: Vec<String>) {
        let message_count = messages.len();

        if message_count > 0 {
            self.counter.increase(messages.len() as u32);
            write_to_file(&self.counter.current_file.as_ref(), messages);
        }

        if self.counter.reached_threshold() {
            self.upload_csv(&self.counter.current_file);
            remove_file(&self.counter.current_file)
                .expect("can't delete files in current location.");
            self.counter.reset();
        }
    }
}

//todo: move this elsewhere
fn write_to_file(file_name: &str, messages: Vec<String>) {
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(file_name)
        .unwrap();

    for msg in messages {
        writeln!(file, "{}", msg).expect("can't write to file.");
    }
}
