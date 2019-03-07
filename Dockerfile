FROM scratch

ADD config/subscribe-example.toml /
ADD auth.json /
ADD target/x86_64-unknown-linux-musl/release/pubsub_bigquery /

CMD ["/pubsub_bigquery", "subscribe-example.toml"]
