use crate::{
    client::Context,
    defaults,
    openid::{OpenIdTokenProvider, TokenConfig},
};
use drogue_cloud_service_api::auth::{
    authz::{AuthorizationRequest, AuthorizationResponse},
    ClientError, ErrorInformation,
};
use reqwest::{Response, StatusCode};
use serde::{Deserialize, Serialize};
use url::Url;

/// A client for authorizing user requests.
#[derive(Clone, Debug)]
pub struct UserAuthClient {
    client: reqwest::Client,
    auth_url: Url,
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
            auth_url: url.join("/api/v1/user/authz")?,
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
            Some(OpenIdTokenProvider::discover_from(client, provider_config).await?),
        )
    }

    pub async fn authorize(
        &self,
        request: AuthorizationRequest,
        context: Context,
    ) -> Result<AuthorizationResponse, ClientError<reqwest::Error>> {
        let req = self.client.post(self.auth_url.clone());
        let req = super::inject_token(self.token_provider.clone(), req, context).await?;

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
                    log::debug!("Authentication failed for {:?}. Result: {:?}", request, err);

                    Err(ClientError::Request(format!(
                        "Failed to decode service response: {}",
                        err
                    )))
                }
            },
            code => match response.json::<ErrorInformation>().await {
                Ok(result) => {
                    log::debug!("Service reported error ({}): {}", code, result);
                    Err(ClientError::Service(result))
                }
                Err(err) => {
                    log::debug!(
                        "Service call failed ({}) for {:?}. Result couldn't be decoded: {:?}",
                        code,
                        request,
                        err
                    );
                    Err(ClientError::Request(format!(
                        "Failed to decode service error response: {}",
                        err
                    )))
                }
            },
        }
    }
}
