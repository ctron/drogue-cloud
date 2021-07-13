use crate::{config::ConfigFromEnv, defaults};
use async_trait::async_trait;
use drogue_cloud_service_api::endpoints::*;
use serde::Deserialize;
use std::fmt::Debug;

const DEFAULT_REALM: &str = "drogue";

pub type EndpointSourceType = Box<dyn EndpointSource + Send + Sync>;

#[async_trait]
pub trait EndpointSource: Debug {
    async fn eval_endpoints(&self) -> anyhow::Result<Endpoints>;
}

/// This is the endpoint configuration when using the [`EnvEndpointSource`].
#[derive(Clone, Debug, Deserialize)]
pub struct EndpointConfig {
    #[serde(default)]
    pub api_url: Option<String>,
    #[serde(default)]
    pub console_url: Option<String>,
    #[serde(default)]
    pub issuer_url: Option<String>,
    #[serde(default)]
    pub sso_url: Option<String>,
    #[serde(default)]
    pub redirect_url: Option<String>,
    #[serde(default)]
    pub http_endpoint_url: Option<String>,
    #[serde(default)]
    pub mqtt_endpoint_host: Option<String>,
    #[serde(default = "defaults::mqtts_port")]
    pub mqtt_endpoint_port: u16,
    #[serde(default)]
    pub mqtt_integration_host: Option<String>,
    #[serde(default = "defaults::mqtts_port")]
    pub mqtt_integration_port: u16,
    #[serde(default)]
    pub device_registry_url: Option<String>,
    #[serde(default)]
    pub command_endpoint_url: Option<String>,

    #[serde(default)]
    pub local_certs: bool,

    #[serde(default)]
    pub demos: Option<String>,
}

pub async fn eval_endpoints() -> anyhow::Result<Endpoints> {
    create_endpoint_source()?.eval_endpoints().await
}

pub fn create_endpoint_source() -> anyhow::Result<EndpointSourceType> {
    let source = std::env::var_os("ENDPOINT_SOURCE").unwrap_or_else(|| "env".into());
    match source.to_str() {
        Some("env") => Ok(Box::new(EnvEndpointSource(amend_global_sso(
            EndpointConfig::from_env_prefix("ENDPOINTS")?,
        )))),
        other => Err(anyhow::anyhow!(
            "Unsupported endpoint source: '{:?}'",
            other
        )),
    }
}

/// Fill in the SSO url from the global scope, if we don't have any configuration for it.
fn amend_global_sso(mut endpoints: EndpointConfig) -> EndpointConfig {
    if endpoints.sso_url.is_none() {
        endpoints.sso_url = super::openid::global_sso();
    }
    endpoints
}

/// Split demo entries
///
/// Format: ENV=Label of Demo=target;Next demo=target2
fn split_demos(str: &str) -> Vec<(String, String)> {
    let mut demos = Vec::new();

    for demo in str.split(';') {
        if let [label, target] = demo.splitn(2, '=').collect::<Vec<&str>>().as_slice() {
            demos.push((label.to_string(), target.to_string()))
        }
    }

    demos
}

fn get_demos() -> Vec<(String, String)> {
    match std::env::var("DEMOS") {
        Ok(value) => split_demos(&value),
        _ => vec![],
    }
}

#[derive(Debug)]
pub struct EnvEndpointSource(pub EndpointConfig);

#[async_trait]
impl EndpointSource for EnvEndpointSource {
    async fn eval_endpoints(&self) -> anyhow::Result<Endpoints> {
        let http = self
            .0
            .http_endpoint_url
            .as_ref()
            .cloned()
            .map(|url| HttpEndpoint { url });
        let mqtt = self.0.mqtt_endpoint_host.as_ref().map(|host| MqttEndpoint {
            host: host.clone(),
            port: self.0.mqtt_endpoint_port,
        });
        let mqtt_integration = self
            .0
            .mqtt_integration_host
            .as_ref()
            .map(|host| MqttEndpoint {
                host: host.clone(),
                port: self.0.mqtt_integration_port,
            });
        let registry = self
            .0
            .device_registry_url
            .as_ref()
            .cloned()
            .map(|url| RegistryEndpoint { url });

        let api = self.0.api_url.clone();
        let console = self.0.console_url.clone();

        let sso = self.0.sso_url.clone();
        let issuer_url = self.0.issuer_url.as_ref().cloned().or_else(|| {
            sso.as_ref()
                .map(|sso| crate::utils::sso_to_issuer_url(&sso, DEFAULT_REALM))
        });

        Ok(Endpoints {
            http,
            mqtt,
            mqtt_integration,
            sso,
            api,
            console,
            issuer_url,
            redirect_url: self.0.redirect_url.as_ref().cloned(),
            registry,
            command_url: self.0.command_endpoint_url.as_ref().cloned(),
            demos: get_demos(),
            local_certs: self.0.local_certs,
        })
    }
}
