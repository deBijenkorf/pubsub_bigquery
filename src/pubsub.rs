use google_pubsub1_beta2::{
    AcknowledgeRequest, PublishRequest, PubsubMessage, PullRequest, ReceivedMessage,
};
use log::{error, info, trace, warn};

use crate::auth::Authenticator;
use crate::handler::Handler;

type PubsubClient<'a> =
google_pubsub1_beta2::Pubsub<hyper::Client, oauth::ServiceAccountAccess<hyper::Client>>;

pub struct PubsubSource {
    max_messages: i32,
    client: PubsubClient<'static>,
    buffered_ack_ids: Vec<String>,
}

impl PubsubSource {
    pub fn new(max_messages: i32, auth: Authenticator) -> Self {
        let client =
            google_pubsub1_beta2::Pubsub::new(auth.client, auth.access);

        PubsubSource {
            max_messages,
            client,
            buffered_ack_ids: vec![],
        }
    }

    pub fn subscribe<T: Handler>(&mut self, subscription: &str, mut handler: T) {
        let request = PullRequest {
            return_immediately: Some(false),
            max_messages: Some(self.max_messages),
        };

        loop {
            let result = self
                .client
                .projects()
                .subscriptions_pull(request.clone(), subscription)
                .doit();

            match result {
                Err(e) => {
                    error!("Pull error: {}", e);
                }
                Ok((_response, response)) => {
                    let received_messages: Vec<ReceivedMessage> =
                        response.received_messages.unwrap_or_default();

                    let messages: Vec<String> = received_messages
                        .clone()
                        .into_iter()
                        .map(|msg| PubsubSource::decode_message(msg.message.unwrap_or_default()))
                        .collect();

                    let mut ack_ids: Vec<String> = received_messages
                        .clone()
                        .into_iter()
                        .map(|msg| msg.ack_id.unwrap_or_default())
                        .collect();

                    self.buffered_ack_ids.append(&mut ack_ids);

                    match handler.handle(messages) {
                        Ok(true) => {
                            let buffered_acks = self.buffered_ack_ids.to_owned();
                            PubsubSource::acknowledge(&self, subscription, buffered_acks);
                            self.buffered_ack_ids.clear();
                        }
                        Ok(false) => {
                            trace!("{} messages in buffer", &self.buffered_ack_ids.len())
                        }
                        Err(err) => error!("encountered error during message handling: {}", err),
                    }
                }
            }
        }
    }

    pub fn publish(&self, messages: Vec<String>, topic: &str) {
        if messages.is_empty() {
            warn!("message length is 0, publishing has been stopped");
            return;
        }

        let pubsub_messages = messages
            .iter()
            .map(|msg| PubsubMessage {
                data: Some(base64::encode(msg.as_bytes())),
                ..Default::default()
            })
            .collect();

        let request = PublishRequest {
            messages: Some(pubsub_messages),
        };
        let result = self
            .client
            .projects()
            .topics_publish(request.clone(), topic)
            .doit();

        match result {
            Err(e) => {
                error!("Publish error: {}", e);
            }
            Ok((_response, response)) => {
                for msg in response.message_ids.unwrap_or_default() {
                    trace!("Published message #{}", msg);
                }
            }
        }
    }

    fn acknowledge(&self, subscription: &str, ack_ids: Vec<String>) {
        if ack_ids.is_empty() {
            return;
        }
        let message_count = ack_ids.len();

        // chunk per 400 ids due to Google API limit.
        ack_ids.chunks(400).for_each(|chunk| {
            let request = AcknowledgeRequest {
                ack_ids: Some(Vec::from(chunk)),
            };

            let result = &self
                .client
                .projects()
                .subscriptions_acknowledge(request, subscription)
                .doit();

            match result {
                Err(e) => error!("Ack error: {:?}", e),
                Ok(_) => (),
            }
        });

        info!("acknowledged {} messages", message_count);
    }

    fn decode_message(message: PubsubMessage) -> String {
        let decoded = base64::decode(&message.data.unwrap_or_default()).unwrap();
        String::from_utf8(decoded).unwrap()
    }
}
