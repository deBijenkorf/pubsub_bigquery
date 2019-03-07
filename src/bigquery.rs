use std::fs::{File, OpenOptions, remove_file};
use std::io::Write;

use google_bigquery2::{
    Job,
    JobConfiguration,
    JobConfigurationLoad,
    TableReference,
};
use log::{error, info};

use crate::auth::Authenticator;
use crate::handler::Handler;
use crate::handler::MessageCounter;

pub struct BigQuerySink {
    pub project_id: String,
    pub dataset_id: String,
    pub table_id: String,
    counter: MessageCounter,
    client: BigQueryClient<'static>,
}

type BigQueryClient<'a> = google_bigquery2::Bigquery<
    hyper::Client,
    oauth::ServiceAccountAccess<hyper::Client>>;

impl BigQuerySink {
    pub fn new(project_id: String, dataset_id: String, table_id: String, max_messages: u32, auth: Authenticator) -> Self {
        let client = google_bigquery2::Bigquery::new(auth.client, auth.access);

        BigQuerySink {
            project_id,
            dataset_id,
            table_id,
            counter: MessageCounter::new(max_messages),
            client,
        }
    }

    fn table_reference(&self) -> TableReference {
        TableReference {
            project_id: Some((&self.project_id).to_string()),
            dataset_id: Some((&self.dataset_id).to_string()),
            table_id: Some((&self.table_id).to_string()),
        }
    }

    fn generate_job(&self) -> Job {
        let table = BigQuerySink::table_reference(&self);

        let mut load_config = JobConfigurationLoad::default();

        load_config.autodetect = Some(true);
        load_config.quote = Some(String::from(""));
        load_config.destination_table = Some(table);

        let mut job = Job::default();

        let mut job_config = JobConfiguration::default();
        job_config.load = Some(load_config);

        job.configuration = Some(job_config);
        job
    }

    pub fn upload_csv(&self, path: &str) {
        let res = &self.client.jobs().insert(BigQuerySink::generate_job(&self), &self.project_id)
            .upload(File::open(path).unwrap(), "text/csv".parse().unwrap());

        match res {
            Err(why) => error!("{}", why.to_string()),
            Ok(_) => info!("upload of csv with name: {} has status: SUCCESS", path)
        }
    }
}

impl Handler for BigQuerySink {
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
