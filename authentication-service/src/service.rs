use actix_web::ResponseError;
use async_trait::async_trait;
use deadpool_postgres::Pool;
use drogue_cloud_database_common::{
    error::ServiceError,
    models::{app::*, device::*},
};
use drogue_cloud_service_api::management::{DeviceSpecCore, DeviceSpecCredentials};
use drogue_cloud_service_api::{
    auth::{self, AuthenticationRequest, Outcome},
    management::{self, Application, Device},
    Translator,
};
use serde::Deserialize;
use tokio_postgres::NoTls;

#[async_trait]
pub trait AuthenticationService: Clone {
    type Error: ResponseError;

    async fn authenticate(&self, request: AuthenticationRequest) -> Result<Outcome, Self::Error>;
    async fn is_ready(&self) -> Result<(), Self::Error>;
}

#[derive(Clone, Debug, Deserialize)]
pub struct AuthenticationServiceConfig {
    pub pg: deadpool_postgres::Config,
}

impl AuthenticationServiceConfig {
    pub fn from_env() -> Result<Self, config::ConfigError> {
        let mut cfg = config::Config::new();
        cfg.merge(config::Environment::new().separator("__"))?;
        cfg.try_into()
    }
}

#[derive(Clone)]
pub struct PostgresAuthenticationService {
    pool: Pool,
}

impl PostgresAuthenticationService {
    pub fn new(config: AuthenticationServiceConfig) -> anyhow::Result<Self> {
        Ok(Self {
            pool: config.pg.create_pool(NoTls)?,
        })
    }
}

#[async_trait]
impl AuthenticationService for PostgresAuthenticationService {
    type Error = ServiceError;

    async fn authenticate(&self, request: AuthenticationRequest) -> Result<Outcome, Self::Error> {
        let c = self.pool.get().await?;

        // lookup the tenant

        let application = PostgresApplicationAccessor::new(&c);
        let application = match application.lookup(&request.application).await? {
            Some(application) => application.into(),
            None => {
                return Ok(Outcome::Fail);
            }
        };

        // validate tenant

        if !validate_app(&application) {
            return Ok(Outcome::Fail);
        }

        // lookup the device

        let device = PostgresDeviceAccessor::new(&c);
        let device = match device
            .lookup(&application.metadata.name, &request.device)
            .await?
        {
            Some(device) => device.into(),
            None => {
                return Ok(Outcome::Fail);
            }
        };

        // validate credential

        Ok(
            match validate_credential(&application, &device, &request.credential) {
                true => Outcome::Pass {
                    application,
                    device: strip_credentials(device),
                },
                false => Outcome::Fail,
            },
        )
    }

    async fn is_ready(&self) -> Result<(), Self::Error> {
        self.pool.get().await?.simple_query("SELECT 1").await?;
        Ok(())
    }
}

/// Strip the credentials from the device information, so that we do not leak them.
fn strip_credentials(mut device: management::Device) -> Device {
    // FIXME: we need to do a better job here, maybe add a "secrets" section instead
    device.spec.remove("credentials");
    device
}

/// Validate if an application is "ok" to be used for authentication.
fn validate_app(app: &management::Application) -> bool {
    match app.spec_as::<DeviceSpecCore, _>("core") {
        // found "core", decoded successfully -> check
        Some(Ok(core)) => {
            if core.disabled {
                return false;
            }
        }
        // found "core", but could not decode -> fail
        Some(Err(_)) => {
            return false;
        }
        // no "core" section
        _ => {}
    };

    // done
    true
}

fn validate_credential(_: &Application, device: &Device, cred: &auth::Credential) -> bool {
    let credentials = match device.spec_as::<DeviceSpecCredentials, _>("credentials") {
        Some(Ok(credentials)) => credentials.credentials,
        _ => return false,
    };

    match cred {
        auth::Credential::Password(provided_password) => {
            credentials.iter().any(|c| match c {
                // match passwords
                management::Credential::Password(stored_password) => {
                    stored_password == provided_password
                }
                // match passwords if the stored username is equal to the device id
                management::Credential::UsernamePassword {
                    username: stored_username,
                    password: stored_password,
                    ..
                } if stored_username == &device.metadata.name => {
                    stored_password == provided_password
                }
                // no match
                _ => false,
            })
        }
        auth::Credential::UsernamePassword {
            username: provided_username,
            password: provided_password,
            ..
        } => credentials.iter().any(|c| match c {
            // match passwords if the provided username is equal to the device id
            management::Credential::Password(stored_password)
                if provided_username == &device.metadata.name =>
            {
                stored_password == provided_password
            }
            // match username/password against username/password
            management::Credential::UsernamePassword {
                username: stored_username,
                password: stored_password,
                ..
            } => stored_username == provided_username && stored_password == provided_password,
            // no match
            _ => false,
        }),
        _ => false,
    }
}
