FROM scratch

ENV GOOGLE_APPLICATION_CREDENTIALS="auth.json"

ADD config/example.toml \
    auth.json \
    target/x86_64-unknown-linux-musl/release/pubsub_bigquery /

ENTRYPOINT ["/pubsub_bigquery", "example.toml"]
