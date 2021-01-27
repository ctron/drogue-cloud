mod basic_auth;
mod command;
mod telemetry;
mod ttn;

use crate::basic_auth::basic_validator;
use crate::command::command_wait;
use actix_web::middleware::Condition;
use actix_web::web::Data;
use actix_web::{
    get, http, http::header, middleware, post, web, App, HttpResponse, HttpServer, Responder,
};
use actix_web_httpauth::middleware::HttpAuthentication;
use cloudevents_sdk_actix_web::HttpRequestExt;
use dotenv::dotenv;
use drogue_cloud_endpoint_common::{
    auth::{AuthConfig, DeviceAuthenticator},
    command_router::CommandRouter,
    downstream::{DownstreamSender, Outcome, Publish, PublishResponse},
    error::HttpEndpointError,
};
use envconfig::Envconfig;
use serde::Deserialize;
use serde_json::json;
use std::convert::TryInto;

#[derive(Envconfig, Clone, Debug)]
struct Config {
    #[envconfig(from = "MAX_JSON_PAYLOAD_SIZE", default = "65536")]
    pub max_json_payload_size: usize,
    #[envconfig(from = "MAX_PAYLOAD_SIZE", default = "65536")]
    pub max_payload_size: usize,
    #[envconfig(from = "BIND_ADDR", default = "127.0.0.1:8080")]
    pub bind_addr: String,
    #[envconfig(from = "HEALTH_BIND_ADDR", default = "127.0.0.1:8081")]
    pub health_bind_addr: String,
    #[envconfig(from = "ENABLE_AUTH", default = "false")]
    pub enable_auth: bool,
    #[envconfig(from = "AUTH_SERVICE_URL")]
    pub auth_service_url: Option<String>,
}

#[get("/")]
async fn index() -> impl Responder {
    HttpResponse::Ok().json(json!({"success": true}))
}

#[get("/health")]
async fn health() -> impl Responder {
    HttpResponse::Ok().finish()
}

#[derive(Deserialize)]
pub struct PublishOptions {
    model_id: Option<String>,
    ttd: Option<u64>,
}

async fn publish(
    endpoint: web::Data<DownstreamSender>,
    web::Path((device_id, channel)): web::Path<(String, String)>,
    web::Query(opts): web::Query<PublishOptions>,
    req: web::HttpRequest,
    body: web::Bytes,
) -> Result<HttpResponse, HttpEndpointError> {
    log::debug!("Published to '{}'", channel);

    match endpoint
        .publish(
            Publish {
                channel,
                tenant_id: "default".into(),
                device_id: device_id.to_owned(),
                model_id: opts.model_id,
                content_type: req
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string()),
            },
            body,
        )
        .await
    {
        // ok, and accepted
        Ok(PublishResponse {
            outcome: Outcome::Accepted,
        }) => command_wait("default", device_id, opts.ttd, http::StatusCode::ACCEPTED).await,

        // ok, but rejected
        Ok(PublishResponse {
            outcome: Outcome::Rejected,
        }) => {
            command_wait(
                "default",
                device_id,
                opts.ttd,
                http::StatusCode::NOT_ACCEPTABLE,
            )
            .await
        }

        // internal error
        Err(err) => Ok(HttpResponse::InternalServerError()
            .content_type("text/plain")
            .body(err.to_string())),
    }
}

async fn telemetry(
    endpoint: web::Data<DownstreamSender>,
    web::Path((tenant, device)): web::Path<(String, String)>,
    req: web::HttpRequest,
    body: web::Bytes,
) -> Result<HttpResponse, HttpEndpointError> {
    log::debug!(
        "Sending telemetry for device '{}' belonging to tenant '{}'",
        device,
        tenant
    );
    endpoint
        .publish_http(
            Publish {
                channel: "telemetry".into(),
                tenant_id: tenant,
                device_id: device,
                model_id: None,
                content_type: req
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string()),
            },
            body,
        )
        .await
}

#[post("/command-service")]
async fn command_service(
    body: web::Bytes,
    req: web::HttpRequest,
    payload: web::Payload,
) -> Result<HttpResponse, actix_web::Error> {
    log::debug!("Req: {:?}", req);

    let mut request_event = req.to_event(payload).await?;
    request_event.set_data(
        "application/json",
        String::from_utf8(body.as_ref().to_vec()).unwrap(),
    );

    if let Err(e) = CommandRouter::send(request_event).await {
        log::error!("Failed to route command: {}", e);
        HttpResponse::BadRequest().await
    } else {
        HttpResponse::Ok().await
    }
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    dotenv().ok();

    log::info!("Starting HTTP service endpoint");

    let sender = DownstreamSender::new()?;

    let config = Config::init_from_env()?;
    let enable_auth = config.enable_auth;
    let max_payload_size = config.max_payload_size;
    let max_json_payload_size = config.max_json_payload_size;

    // create authenticator, fails if authentication is enabled, but configuration is missing
    let authenticator = match enable_auth {
        true => {
            log::info!("Enabling authentication");
            let authenticator: DeviceAuthenticator = AuthConfig::init_from_env()?.try_into()?;
            Some(authenticator)
        }
        false => {
            log::warn!("Device authentication is disabled!");
            None
        }
    };

    HttpServer::new(move || {
        let app = App::new()
            .wrap(middleware::Logger::default())
            .app_data(web::PayloadConfig::new(max_payload_size))
            .data(web::JsonConfig::default().limit(max_json_payload_size))
            .data(sender.clone());

        // add authenticator, if we have one
        let app = if let Some(authenticator) = &authenticator {
            app.app_data(Data::new(authenticator.clone()))
        } else {
            app
        };

        app.service(index)
            .service(
                web::scope("/v1")
                    .service(telemetry::publish_plain)
                    .service(telemetry::publish_tail),
            )
            .service(
                web::resource("/publish/{device_id}/{channel}")
                    .wrap(Condition::new(
                        enable_auth,
                        HttpAuthentication::basic(basic_validator),
                    ))
                    .route(web::post().to(publish)),
            )
            .service(
                web::resource("/telemetry/{tenant}/{device}")
                    .wrap(Condition::new(
                        enable_auth,
                        HttpAuthentication::basic(basic_validator),
                    ))
                    .route(web::put().to(telemetry)),
            )
            .service(
                web::scope("/ttn")
                    .wrap(Condition::new(
                        enable_auth,
                        HttpAuthentication::basic(basic_validator),
                    ))
                    .service(ttn::publish),
            )
            .service(command_service)
            //fixme : bind to a different port
            .service(health)
    })
    .bind(config.bind_addr)?
    .run()
    .await?;

    // fixme
    //
    // let health_server = HttpServer::new(move || App::new().service(health))
    //     .bind(config.health_bind_addr)?
    //     .run();
    //
    // future::try_join(app_server, health_server).await?;

    Ok(())
}
