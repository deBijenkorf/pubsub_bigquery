extern crate base64;
extern crate config;
extern crate google_bigquery2;
extern crate google_pubsub1_beta2;
extern crate hyper;
extern crate hyper_rustls;
extern crate log;
extern crate serde;
extern crate serde_derive;
extern crate simple_logger;
extern crate uuid;
extern crate yup_oauth2 as oauth;

use std::env;

use log::{error, info};
use log::Level;

mod settings;
mod handler;
mod pubsub;
mod bigquery;

pub fn start() {
    let args: Vec<String> = env::args().skip(1).collect();
    let settings = settings::Settings::new(args[0].as_ref());

    match settings {
        Err(err) => error!("{}", err.to_string()),
        Ok(set) => {
            info!("debug mode: {}", &set.debug);
            info!("application mode: {:?}", &set.mode);

            simple_logger::init_with_level(Level::Info).unwrap();
            info!("starting application.");

            let sink = bigquery::BigQuerySink::new(
                set.google.project_id,
                set.google.bigquery_dataset,
                set.google.bigquery_table,
                set.limits.handler_max_messages,
            );

            let source = pubsub::PubsubSource::new(set.limits.pubsub_max_messages);

            match &set.mode {
                settings::Mode::Publish => {
                    let count = args[1].parse::<u32>().unwrap();

                    let messages: Vec<String> = (0..count)
                        .map(|_| "hello,there,1".to_string())
                        .collect();

                    source.publish(messages, &set.google.pubsub_topic);
                }
                settings::Mode::Subscribe => {
                    source.subscribe(&set.google.pubsub_subscription, sink)
                }
            }
        }
    }
}
