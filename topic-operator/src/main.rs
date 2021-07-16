mod controller;
mod data;
mod endpoints;

use crate::controller::base::{
    ApplicationController, BaseController, EventSource, FnEventProcessor,
};
use crate::controller::ControllerConfig;
use actix_web::{
    get, middleware,
    web::{self},
    App, HttpResponse, HttpServer, Responder,
};
use anyhow::Context;
use dotenv::dotenv;
use drogue_client::registry;
use drogue_cloud_registry_events::Event;
use drogue_cloud_service_common::{
    config::ConfigFromEnv,
    defaults,
    health::{HealthServer, HealthServerConfig},
    openid::TokenConfig,
};
use futures::TryFutureExt;
use kube::{api::GroupVersionKind, core::DynamicObject, discovery, Api};
use serde::Deserialize;
use serde_json::json;
use url::Url;

#[derive(Clone, Debug, Deserialize)]
struct Config {
    #[serde(default = "defaults::max_json_payload_size")]
    pub max_json_payload_size: usize,

    #[serde(default = "defaults::bind_addr")]
    pub bind_addr: String,

    #[serde(default)]
    pub registry: RegistryConfig,

    #[serde(default)]
    pub health: HealthServerConfig,

    pub controller: ControllerConfig,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RegistryConfig {
    #[serde(default = "defaults::registry_url")]
    pub url: Url,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            url: defaults::registry_url(),
        }
    }
}

pub struct WebData {
    // pub controller: controller::Controller,
    pub processor: BaseController<String, ApplicationController>,
}

#[get("/")]
async fn index() -> impl Responder {
    HttpResponse::Ok().json(json!({"success": true}))
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    dotenv().ok();

    let config = Config::from_env()?;
    let max_json_payload_size = config.max_json_payload_size;

    let kube = kube::client::Client::try_default()
        .await
        .context("Failed to create Kubernetes client")?;

    // TODO: discover version too
    let gvk = GroupVersionKind::gvk("kafka.strimzi.io", "v1beta2", "KafkaTopic");
    let (kafka_topic_resource, _caps) = discovery::pinned_kind(&kube, &gvk).await?;
    let kafka_topics = Api::<DynamicObject>::namespaced_with(
        kube,
        &config.controller.topic_namespace,
        &kafka_topic_resource,
    );

    let client = reqwest::Client::new();

    let registry = registry::v1::Client::new(
        client.clone(),
        config.registry.url,
        Some(
            TokenConfig::from_env_prefix("REGISTRY")?
                .amend_with_env()
                .discover_from(client.clone())
                .await?,
        ),
    );

    let controller = controller::Controller::new(
        config.controller,
        registry.clone(),
        kafka_topic_resource,
        kafka_topics,
    );

    // controller

    let processor = BaseController::new(ApplicationController::new(registry));

    // app data

    let data = web::Data::new(WebData { processor });

    // health server

    let health = HealthServer::new(config.health, vec![]);

    // main

    let main = HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .data(web::JsonConfig::default().limit(max_json_payload_size))
            .app_data(data.clone())
            .service(index)
            .service(endpoints::events)
    })
    .bind(config.bind_addr)?
    .run();

    // run

    futures::try_join!(health.run(), main.err_into())?;

    // exiting

    Ok(())
}
