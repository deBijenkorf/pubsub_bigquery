use std::time::Duration;

use hyper::Client;
use hyper::net::{HttpConnector, HttpsConnector};
use hyper_rustls::TlsClient;
use log::info;
use oauth::ServiceAccountAccess;

pub struct Authenticator {
    pub client: Client,
    pub access: ServiceAccountAccess<hyper::Client>,
}

impl Authenticator {
    fn get_https_client() -> HttpsConnector<TlsClient, HttpConnector> {
        HttpsConnector::new(hyper_rustls::TlsClient::new())
    }

    pub fn authenticate(path: &str) -> Authenticator {
        info!("requesting new GCP access token");
        let client_secret = oauth::service_account_key_from_file(&path.to_string()).unwrap();
        let client = hyper::Client::with_connector(Authenticator::get_https_client());
        let access = oauth::ServiceAccountAccess::new(client_secret, client);
        let mut client = hyper::Client::with_connector(Authenticator::get_https_client());
        {
            client.set_read_timeout(Some(Duration::from_secs(60)));
            client.set_write_timeout(Some(Duration::from_secs(60)));
        }

        Authenticator { client, access }
    }
}
