use hyper::{
    Body,
    Client,
    client::connect::dns::GaiResolver,
    client::HttpConnector,
};
use hyper_tls::HttpsConnector;
use log::info;
use oauth::ServiceAccountAccess;

pub type Connector = Client<HttpsConnector<HttpConnector<GaiResolver>>, Body>;

pub struct Authenticator {
    pub client: Connector,
    pub access: ServiceAccountAccess<Connector>,
}

impl Authenticator {
    fn get_https_client() -> Client<HttpsConnector<HttpConnector<GaiResolver>>, Body> {
        let https = HttpsConnector::new(5).expect("TLS initialization failed");
        Client::builder()
            .build::<_, Body>(https)
    }

    pub fn authenticate(path: &str) -> Authenticator {
        info!("requesting new GCP access token");
        let client_secret = oauth::service_account_key_from_file(&path.to_string()).unwrap();


        let client = hyper::Client::with_connector(Authenticator::get_https_client());
        let access = oauth::ServiceAccountAccess::new(client_secret, client);
        let client = hyper::Client::with_connector(Authenticator::get_https_client());

        Authenticator {
            client,
            access,
        }
    }
}
