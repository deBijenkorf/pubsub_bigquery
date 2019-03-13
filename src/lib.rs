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

use crate::handler::MessageCounter;
use crate::pubsub::PubsubSource;
use crate::settings::Settings;
use crate::auth::Authenticator;

mod auth;
mod settings;
mod handler;
mod pubsub;
mod bigquery;

pub fn start() {
    let args: Vec<String> = env::args().skip(1).collect();
    let settings = settings::Settings::new(args[0].as_ref());
    simple_logger::init_with_level(Level::Info).unwrap();

    match settings {
        Err(err) => error!("{}", err.to_string()),
        Ok(set) => {
            info!("debug mode: {}", &set.debug);
            info!("application mode: {:?}", &set.mode);
            info!("starting application.");

            match &set.mode {
                settings::Mode::Publish => {
                    let message_count = args[1].parse::<u32>().unwrap();
                    start_publisher(set, message_count)
                }
                settings::Mode::Subscribe => start_subscriber(set)
            }
        }
    }
}

fn start_publisher(settings: Settings, message_count: u32) {
    let source = PubsubSource::new(
        settings.limits.pubsub_max_messages,
        Authenticator::authenticate(&settings.google.auth_key_file)
    );

    info!("start publishing {} messages.", &message_count);
    let messages: Vec<String> = (0..message_count)
        .map(|_| "hello,there,1".to_string())
        .collect();

    source.publish(messages, &settings.google.pubsub_topic);
}

fn start_subscriber(settings: Settings) {
    let google = settings.google.clone();
    let key_file = settings.google.auth_key_file;

    let mut source = PubsubSource::new(
        settings.limits.pubsub_max_messages,
        Authenticator::authenticate(&key_file)
    );

    let sink = bigquery::BigQuerySink::new(
        google,
        settings.delimiter,
        MessageCounter::new(settings.limits.handler_max_messages),
        Authenticator::authenticate(&key_file)
    );

    source.subscribe(&settings.google.pubsub_subscription, sink);
}