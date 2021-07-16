//! Clients for services.

mod device_auth;
mod user_auth;

pub use device_auth::*;
pub use user_auth::*;

use drogue_client::error::{ClientError, ErrorInformation};
use http::StatusCode;
use reqwest::Response;

pub(crate) async fn default_error<T>(
    code: StatusCode,
    response: Response,
) -> Result<T, ClientError<reqwest::Error>> {
    match response.json::<ErrorInformation>().await {
        Ok(result) => {
            log::debug!("Service reported error ({}): {}", code, result);
            Err(ClientError::Service(result))
        }
        Err(err) => {
            log::debug!(
                "Service call failed ({}). Result couldn't be decoded: {:?}",
                code,
                err
            );
            Err(ClientError::Request(format!(
                "Failed to decode service error response: {}",
                err
            )))
        }
    }
}
