use super::management;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthenticationRequest {
    pub application: String,
    pub device: String,
    pub credential: Credential,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Credential {
    #[serde(rename = "user")]
    UsernamePassword { username: String, password: String },
    #[serde(rename = "pass")]
    Password(String),
    #[serde(rename = "cert")]
    Certificate(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// The authentication request passed. The outcome also contains application and device
    /// details for further processing.
    Pass {
        application: management::Application,
        device: management::Device,
    },
    /// The authentication request failed. The device is not authenticated, and the device's
    /// request must be rejected.
    Fail,
}

#[derive(thiserror::Error, Debug)]
pub enum AuthenticationClientError<E: 'static>
where
    E: std::error::Error,
{
    /// An error from the underlying API client (e.g. reqwest).
    #[error("client error: {0}")]
    Client(#[from] Box<E>),
    /// A local error, performing the request.
    #[error("request error: {0}")]
    Request(String),
    /// A remote error, performing the request.
    #[error("service error: {0}")]
    Service(ErrorInformation),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ErrorInformation {
    pub error: String,
    #[serde(default)]
    pub message: String,
}

impl fmt::Display for ErrorInformation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.error, self.message)
    }
}

/// A client, authenticating devices.
#[async_trait]
pub trait AuthenticationClient {
    type Error: std::error::Error;

    /// Authenticate a device.
    ///
    /// Any kind of error should always be treated as an authentication failure. A successful
    /// call still doesn't mean that the authentication service authenticated the device. The
    /// caller needs to inspect the outcome in the [AuthenticationResponse](drogue_cloud_service_api::auth::AuthenticationResponse).
    async fn authenticate(
        &self,
        request: AuthenticationRequest,
    ) -> Result<AuthenticationResponse, AuthenticationClientError<Self::Error>>;
}

/// The result of an authentication request.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AuthenticationResponse {
    /// The outcome, if the request.
    pub outcome: Outcome,
}

impl AuthenticationResponse {
    pub fn failed() -> Self {
        Self {
            outcome: Outcome::Fail,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    #[test]
    fn ser_credentials() {
        let ser = serde_json::to_value(vec![
            Credential::Password("foo".into()),
            Credential::UsernamePassword {
                username: "foo".into(),
                password: "bar".into(),
            },
        ]);
        assert!(ser.is_ok());
        assert_eq!(
            ser.unwrap(),
            json! {[
                {"pass": "foo"},
                {"user": {"username": "foo", "password": "bar"}}
            ]}
        )
    }

    #[test]
    fn test_encode_fail() {
        let str = serde_json::to_string(&AuthenticationResponse {
            outcome: Outcome::Fail,
        });
        assert!(str.is_ok());
        assert_eq!(String::from(r#"{"outcome":"fail"}"#), str.unwrap());
    }

    #[test]
    fn test_encode_pass() {
        let str = serde_json::to_string(&AuthenticationResponse {
            outcome: Outcome::Pass {
                application: management::Application {
                    metadata: management::ApplicationMetadata {
                        name: "a1".to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                device: management::Device {
                    metadata: management::DeviceMetadata {
                        application: "a1".to_string(),
                        name: "d1".to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
        });

        assert!(str.is_ok());
        assert_eq!(
            String::from(
                r#"{"outcome":{"pass":{"application":{"metadata":{"name":"a1"}},"device":{"metadata":{"application":"a1","name":"d1"}}}}}"#
            ),
            str.unwrap()
        );
    }
}
