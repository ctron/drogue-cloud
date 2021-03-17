use crate::endpoints::eval_endpoints;
use anyhow::Context;
use core::fmt::{Debug, Formatter};
use drogue_cloud_service_api::endpoints::Endpoints;
use envconfig::Envconfig;
use failure::Fail;
use openid::biscuit::jws::Compact;
use openid::{Client, Empty, Jws, StandardClaims};
use reqwest::Certificate;
use std::{fs::File, io::Read, path::Path};
use thiserror::Error;
use url::Url;

const SERVICE_CA_CERT: &str = "/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt";

#[derive(Debug, Envconfig)]
pub struct AuthenticatorConfig {
    #[envconfig(from = "CLIENT_ID")]
    pub client_id: String,
    #[envconfig(from = "CLIENT_SECRET")]
    pub client_secret: String,
    // Note: "roles" may be required for the "aud" claim when using Keycloak
    #[envconfig(from = "SCOPES", default = "openid profile email")]
    pub scopes: String,
}

#[derive(Debug, Error)]
pub enum AuthenticatorError {
    #[error("Missing authenticator instance")]
    Missing,
    #[error("Authentication failed")]
    Failed,
}

/// An authenticator to authenticate incoming requests.
pub struct Authenticator {
    pub client: openid::Client,
}

impl Debug for Authenticator {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        let mut d = f.debug_struct("Authenticator");
        d.field("client", &"...");
        d.finish()
    }
}

impl From<openid::Client> for Authenticator {
    fn from(client: Client) -> Self {
        Self::from_client(client)
    }
}

impl Authenticator {
    /// Create a new authenticator by evaluating endpoints and SSO configuration.
    pub async fn new() -> anyhow::Result<Self> {
        Self::from_endpoints(eval_endpoints().await?).await
    }

    pub async fn from_endpoints(endpoints: Endpoints) -> anyhow::Result<Self> {
        let config = AuthenticatorConfig::init_from_env()?;
        Self::from_config(&config, endpoints).await
    }

    pub fn from_client(client: openid::Client) -> Self {
        Authenticator { client }
    }

    pub async fn from_config(
        config: &AuthenticatorConfig,
        endpoints: Endpoints,
    ) -> anyhow::Result<Self> {
        Ok(Self::from_client(create_client(config, endpoints).await?))
    }

    pub async fn validate_token<S: AsRef<str>>(
        &self,
        token: S,
    ) -> Result<Compact<StandardClaims, Empty>, AuthenticatorError> {
        let mut token = Jws::new_encoded(token.as_ref());

        self.client.decode_token(&mut token).map_err(|err| {
            log::debug!("Failed to decode token: {}", err);
            AuthenticatorError::Failed
        })?;

        log::debug!("Token: {:#?}", token);

        self.client
            .validate_token(&token, None, None)
            .map_err(|err| {
                log::info!("Validation failed: {}", err);
                AuthenticatorError::Failed
            })?;

        Ok(token)
    }
}

impl ClientConfig for AuthenticatorConfig {
    fn client_id(&self) -> String {
        self.client_id.clone()
    }

    fn client_secret(&self) -> String {
        self.client_secret.clone()
    }
}

pub trait ClientConfig {
    fn client_id(&self) -> String;
    fn client_secret(&self) -> String;
}

pub async fn create_client(
    config: &dyn ClientConfig,
    endpoints: Endpoints,
) -> anyhow::Result<openid::Client> {
    let mut client = reqwest::ClientBuilder::new();

    client = add_service_cert(client)?;

    let Endpoints {
        redirect_url,
        issuer_url,
        ..
    } = endpoints;

    let issuer_url = issuer_url.ok_or_else(|| {
        anyhow::anyhow!(
            "Failed to detect 'issuer URL'. Consider using an env-var based configuration."
        )
    })?;

    let client = openid::DiscoveredClient::discover_with_client(
        client.build()?,
        config.client_id(),
        config.client_secret(),
        redirect_url,
        Url::parse(&issuer_url)
            .with_context(|| format!("Failed to parse issuer URL: {}", issuer_url))?,
    )
    .await
    .map_err(|err| anyhow::Error::from(err.compat()))?;

    log::info!("Discovered OpenID: {:#?}", client.config());

    Ok(client)
}

fn add_service_cert(mut client: reqwest::ClientBuilder) -> anyhow::Result<reqwest::ClientBuilder> {
    let cert = Path::new(SERVICE_CA_CERT);
    if cert.exists() {
        log::info!("Adding root certificate: {}", SERVICE_CA_CERT);
        let mut file = File::open(cert)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let pems = pem::parse_many(buf);
        let pems = pems
            .into_iter()
            .map(|pem| {
                Certificate::from_pem(&pem::encode(&pem).into_bytes()).map_err(|err| err.into())
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        log::info!("Found {} certificates", pems.len());

        for pem in pems {
            log::info!("Adding root certificate: {:?}", pem);
            client = client.add_root_certificate(pem);
        }
    } else {
        log::info!(
            "Service CA certificate does not exist, skipping! ({})",
            SERVICE_CA_CERT
        );
    }

    Ok(client)
}
