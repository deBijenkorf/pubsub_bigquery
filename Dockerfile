FROM scratch

ADD config/subscribe-example.toml \
    auth.json \
    target/x86_64-unknown-linux-musl/release/pubsub_bigquery /

CMD ["/pubsub_bigquery", "subscribe-example.toml"]
