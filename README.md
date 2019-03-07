# pubsub_bigquery

[![Build Status](https://travis-ci.org/zwennesm/pubsub_bigquery.svg?branch=master)](https://travis-ci.org/zwennesm/pubsub_bigquery)

Project status: ALPHA

A Google BigQuery sink for Google Pubsub events written in Rust. This project originated after
some frustrations writing another version of this flow using the Apache Beam SDK. Although
the Apache Beam SDK is relatively straight forward, I noticed that it's either:

* Expensive (when using the Google Dataflow runner)
* Slow (when using the SparkRunner)
* And in both cases quite memory intensive

Because of these reasons it seemed like a good idea to implement this flow in Rust using the
Google API's directly.

## Quickstart

The application requires a few settings to be configured through a config file. The example
below illustrates a simple setup:

1. Build the application using Cargo
    ```
    cargo build --release
    ```

2. Create a configuration file:
    ```
    #config.toml
    
    debug = false
    mode = "subscribe" // or `publish` for testing
    
    [google]
    project_id = "PROJECT_ID"
    pubsub_subscription = "projects/PROJECT_ID/subscriptions/SUBSCRIPTION"
    pubsub_topic = "projects/PROJECT_ID/topics/TOPIC"
    bigquery_dataset = "DATASET_NAME"
    bigquery_table = "TABLE_NAME"
    
    [limits]
    pubsub_max_messages = 200
    handler_max_messages = 1000
    ```
3. Run the application
    ```
    cd target/release
    ./pubsub_bigquery config.toml
    ```


## Creating a Docker image

It's possible to create a Docker container with a size of approximately 10MB by creating a static
Rust binary. You will need the [rust-musl-builder](https://github.com/emk/rust-musl-builder) for it.
More information about this can be found [here](https://blog.semicolonsoftware.de/building-minimal-docker-containers-for-rust-applications/)

1. First create a release using the following command:

    ```
    alias rust-musl-builder='docker run --rm -it -v "$(pwd)":/home/rust/src ekidd/rust-musl-builder'
    rust-musl-builder cargo build --release
    ```

2. Then create a Docker image using the supplied `Dockerfile`:

    ```
    docker build -t pubsub-bigquery:0.1 .
    ```
    
3. And lastly, run the Docker container:

    ```
    docker run pubsub-bigquery:0.1
    ```