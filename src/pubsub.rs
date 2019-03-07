use google_pubsub1_beta2::AcknowledgeRequest;
use google_pubsub1_beta2::PublishRequest;
use google_pubsub1_beta2::PubsubMessage;
use google_pubsub1_beta2::PullRequest;
use google_pubsub1_beta2::ReceivedMessage;
use log::{error, info, trace, warn};

use crate::auth::Authenticator;
use crate::handler::Handler;

type PubsubClient<'a> = google_pubsub1_beta2::Pubsub<
    hyper::Client,
    oauth::ServiceAccountAccess<hyper::Client>>;

pub struct PubsubSource {
    max_messages: i32,
    client: PubsubClient<'static>,
}

impl PubsubSource {
    pub fn new(max_messages: i32, auth: Authenticator) -> Self {
        let client = google_pubsub1_beta2::Pubsub::new(auth.client, auth.access);

        PubsubSource {
            max_messages,
            client,
        }
    }

    pub fn subscribe<T: Handler>(&self, subscription: &str, mut handler: T) {
        let request = PullRequest {
            return_immediately: Some(false),
            max_messages: Some(self.max_messages),
        };

        loop {
            let result = self.client.projects()
                .subscriptions_pull(request.clone(), subscription)
                .doit();

            match result {
                Err(e) => {
                    error!("Pull error: {}", e);
                }
                Ok((_response, response)) => {
                    let received_messages: Vec<ReceivedMessage> = response.received_messages
                        .unwrap_or(Vec::new());

                    let messages: Vec<String> = received_messages.clone().into_iter()
                        .map(|msg| PubsubSource::decode_message(msg.message.unwrap_or(Default::default())))
                        .collect();

                    let ack_ids: Vec<String> = received_messages.clone().into_iter()
                        .map(|msg| msg.ack_id.unwrap_or(String::new()))
                        .collect();

                    handler.handle(messages);
                    PubsubSource::acknowledge(&self, subscription, ack_ids);
                }
            }
        }
    }

    pub fn publish(&self, messages: Vec<String>, topic: &str) {
        if messages.len() == 0 {
            warn!("message length is 0, publishing has been stopped");
            return;
        }

        let pubsub_messages = messages.iter()
            .map(|msg| PubsubMessage {
                data: Some(base64::encode(msg.as_bytes())),
                ..Default::default()
            })
            .collect();

        let request = PublishRequest { messages: Some(pubsub_messages) };
        let result = self.client.projects().topics_publish(request.clone(), topic).doit();

        match result {
            Err(e) => {
                error!("Publish error: {}", e);
            }
            Ok((_response, response)) => {
                for msg in response.message_ids.unwrap_or(Vec::new()) {
                    trace!("Published message #{}", msg);
                }
            }
        }
    }

    fn acknowledge(&self, subscription: &str, ack_ids: Vec<String>) {
        let ack_count = ack_ids.len();

        if ack_count > 0 {
            let request = AcknowledgeRequest {
                ack_ids: Some(ack_ids)
            };
            let result = &self.client.projects()
                .subscriptions_acknowledge(request, subscription)
                .doit();

            match result {
                Err(e) => {
                    error!("Ack error: {:?}", e);
                }
                Ok(_) => ()
            }
        }
        info!("acknowledged {} messages", &ack_count);
    }

    fn decode_message(message: PubsubMessage) -> String {
        let decoded = base64::decode(&message.data.unwrap_or(String::new())).unwrap();
        String::from_utf8(decoded).unwrap()
    }
}
