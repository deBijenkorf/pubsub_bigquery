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

use log::{error, info, Level};

use crate::auth::Authenticator;
use crate::handler::MessageCounter;
use crate::pubsub::PubsubSource;
use crate::settings::Settings;

mod auth;
mod settings;
mod handler;
mod pubsub;
mod bigquery;

pub fn start() {
    let args: Vec<String> = env::args().skip(1).collect();
    let settings = settings::Settings::new(args[0].as_ref());
    let secret = env::var("GOOGLE_APPLICATION_CREDENTIALS");
    simple_logger::init_with_level(Level::Info).unwrap();

    let secret = match secret {
        Ok(s) => s,
        _ => panic!("GOOGLE_APPLICATION_CREDENTIALS must be set."),
    };

    match settings {
        Err(err) => error!("{}", err.to_string()),
        Ok(set) => {
            info!("debug mode: {}", &set.debug);
            info!("application mode: {:?}", &set.mode);
            info!("starting application.");

            match &set.mode {
                settings::Mode::Publish => {
                    let message_count = args[1].parse::<u32>().unwrap();
                    start_publisher(set, message_count, secret.as_ref())
                }
                settings::Mode::Subscribe => start_subscriber(set, secret.as_ref())
            }
        }
    }
}

fn start_publisher(settings: Settings, message_count: u32, secret: &str) {
    let source = PubsubSource::new(
        settings.limits.pubsub_max_messages,
        Authenticator::authenticate(secret),
    );

    info!("start publishing {} messages.", &message_count);
    let messages: Vec<String> = (0..message_count)
        .map(|_| "hello,there,1".to_string())
        .collect();

    source.publish(messages, &settings.pubsub.topic);
}

fn start_subscriber(settings: Settings, secret: &str) {
    let mut source = PubsubSource::new(
        settings.limits.pubsub_max_messages,
        Authenticator::authenticate(secret),
    );

    let sink = bigquery::BigQuerySink::new(
        settings.bigquery,
        MessageCounter::new(settings.limits),
        Authenticator::authenticate(secret),
    );

    source.subscribe(&settings.pubsub.subscription, sink);
}