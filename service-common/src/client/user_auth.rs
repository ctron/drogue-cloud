use crate::{defaults, openid::TokenConfig};
use drogue_client::{
    error::ClientError,
    openid::{OpenIdTokenProvider, TokenInjector},
    Context,
};
use drogue_cloud_service_api::auth::user::{
    authn::{AuthenticationRequest, AuthenticationResponse},
    authz::{AuthorizationRequest, AuthorizationResponse},
};
use reqwest::{Response, StatusCode};
use serde::{Deserialize, Serialize};
use url::Url;

/// A client for authorizing user requests.
#[derive(Clone, Debug)]
pub struct UserAuthClient {
    client: reqwest::Client,
    authn_url: Url,
    authz_url: Url,
    token_provider: Option<OpenIdTokenProvider>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserAuthClientConfig {
    #[serde(default = "defaults::user_auth_url")]
    pub url: Url,
}

impl Default for UserAuthClientConfig {
    fn default() -> Self {
        Self {
            url: defaults::user_auth_url(),
        }
    }
}

impl UserAuthClient {
    /// Create a new client instance.
    pub fn new(
        client: reqwest::Client,
        url: Url,
        token_provider: Option<OpenIdTokenProvider>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            authn_url: url.join("/api/user/v1alpha1/authn")?,
            authz_url: url.join("/api/v1/user/authz")?,
            client,
            token_provider,
        })
    }

    pub async fn from_config(
        client: reqwest::Client,
        config: UserAuthClientConfig,
        provider_config: TokenConfig,
    ) -> anyhow::Result<Self> {
        Self::new(
            client.clone(),
            config.url,
            Some(provider_config.discover_from(client).await?),
        )
    }

    pub async fn authenticate_api_key(
        &self,
        request: AuthenticationRequest,
        context: Context,
    ) -> Result<AuthenticationResponse, ClientError<reqwest::Error>> {
        let req = self
            .client
            .post(self.authn_url.clone())
            .inject_token(&self.token_provider, context)
            .await?;

        let response: Response = req.json(&request).send().await.map_err(|err| {
            log::warn!("Error while authenticating {:?}: {}", request, err);
            Box::new(err)
        })?;

        match response.status() {
            StatusCode::OK => match response.json::<AuthenticationResponse>().await {
                Ok(result) => {
                    log::debug!("Outcome for {:?} is {:?}", request, result);
                    Ok(result)
                }
                Err(err) => {
                    log::debug!("Authentication failed for {:?}. Result: {:?}", request, err);

                    Err(ClientError::Request(format!(
                        "Failed to decode service response: {}",
                        err
                    )))
                }
            },
            code => super::default_error(code, response).await,
        }
    }

    pub async fn authorize(
        &self,
        request: AuthorizationRequest,
        context: Context,
    ) -> Result<AuthorizationResponse, ClientError<reqwest::Error>> {
        let req = self
            .client
            .post(self.authz_url.clone())
            .inject_token(&self.token_provider, context)
            .await?;

        let response: Response = req.json(&request).send().await.map_err(|err| {
            log::warn!("Error while authorizing {:?}: {}", request, err);
            Box::new(err)
        })?;

        match response.status() {
            StatusCode::OK => match response.json::<AuthorizationResponse>().await {
                Ok(result) => {
                    log::debug!("Outcome for {:?} is {:?}", request, result);
                    Ok(result)
                }
                Err(err) => {
                    log::debug!("Authorization failed for {:?}. Result: {:?}", request, err);

                    Err(ClientError::Request(format!(
                        "Failed to decode service response: {}",
                        err
                    )))
                }
            },
            code => super::default_error(code, response).await,
        }
    }
}
