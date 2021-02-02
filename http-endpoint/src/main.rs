mod command;
mod downstream;
mod telemetry;
mod ttn;
mod x509;

use crate::x509::ClientCertificateChain;
use actix_web::{
    get, middleware, post,
    web::{self, Data},
    App, HttpResponse, HttpServer, Responder,
};
use cloudevents_sdk_actix_web::HttpRequestExt;
use dotenv::dotenv;
use drogue_cloud_endpoint_common::{
    auth::{AuthConfig, DeviceAuthenticator},
    command_router::CommandRouter,
    downstream::DownstreamSender,
};
use envconfig::Envconfig;
use serde_json::json;
use std::{any::Any, convert::TryInto};

#[derive(Envconfig, Clone, Debug)]
struct Config {
    #[envconfig(from = "MAX_JSON_PAYLOAD_SIZE", default = "65536")]
    pub max_json_payload_size: usize,
    #[envconfig(from = "MAX_PAYLOAD_SIZE", default = "65536")]
    pub max_payload_size: usize,
    #[envconfig(from = "BIND_ADDR", default = "127.0.0.1:8443")]
    pub bind_addr: String,
    #[envconfig(from = "HEALTH_BIND_ADDR", default = "127.0.0.1:8081")]
    pub health_bind_addr: String,
    #[envconfig(from = "AUTH_SERVICE_URL")]
    pub auth_service_url: Option<String>,
    #[envconfig(from = "DISABLE_TLS", default = "false")]
    pub disable_tls: bool,
    #[envconfig(from = "CERT_BUNDLE_FILE")]
    pub cert_file: Option<String>,
    #[envconfig(from = "KEY_FILE")]
    pub key_file: Option<String>,
}

#[get("/")]
async fn index() -> impl Responder {
    HttpResponse::Ok().json(json!({"success": true}))
}

#[get("/health")]
async fn health() -> impl Responder {
    HttpResponse::Ok().finish()
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
    let max_payload_size = config.max_payload_size;
    let max_json_payload_size = config.max_json_payload_size;

    let authenticator: DeviceAuthenticator = AuthConfig::init_from_env()?.try_into()?;

    let http = HttpServer::new(move || {
        let app = App::new()
            .wrap(middleware::Logger::default())
            .app_data(web::PayloadConfig::new(max_payload_size))
            .data(web::JsonConfig::default().limit(max_json_payload_size))
            .data(sender.clone());

        let app = app.app_data(Data::new(authenticator.clone()));

        app.service(index)
            // the standard endpoint
            .service(
                web::scope("/v1")
                    .service(telemetry::publish_plain)
                    .service(telemetry::publish_tail),
            )
            // The Things Network variant
            .service(web::scope("/ttn").service(ttn::publish))
            .service(command_service)
            //fixme : bind to a different port
            .service(health)
    })
    .on_connect(|con, ext| {
        if let Some(cert) = extract_client_cert(con) {
            if !cert.0.is_empty() {
                ext.insert(cert);
            }
        }
    });

    let http = match (config.disable_tls, config.key_file, config.cert_file) {
        (false, Some(key), Some(cert)) => {
            if cfg!(feature = "openssl") {
                use open_ssl::ssl;
                let method = ssl::SslMethod::tls_server();
                let mut builder = ssl::SslAcceptor::mozilla_intermediate(method)?;
                builder.set_private_key_file(key, ssl::SslFiletype::PEM)?;
                builder.set_certificate_chain_file(cert)?;
                // we ask for client certificates, but don't enforce them
                builder.set_verify_callback(ssl::SslVerifyMode::PEER, |_, ctx| {
                    log::debug!(
                        "Accepting client certificates: {:?}",
                        ctx.current_cert()
                            .map(|cert| format!("{:?}", cert.subject_name()))
                            .unwrap_or_else(|| "<unknown>".into())
                    );
                    true
                });

                http.bind_openssl(config.bind_addr, builder)?
            } else {
                panic!("TLS is required, but no TLS implementation enabled")
            }
        }
        (true, None, None) => http.bind(config.bind_addr)?,
        (false, _, _) => panic!("Wrong TLS configuration: TLS enabled, but key or cert is missing"),
        (true, Some(_), _) | (true, _, Some(_)) => {
            // the TLS configuration must be consistent, to prevent configuration errors.
            panic!("Wrong TLS configuration: key or cert specified, but TLS is disabled")
        }
    };

    http.run().await?;

    // fixme
    //
    // let health_server = HttpServer::new(move || App::new().service(health))
    //     .bind(config.health_bind_addr)?
    //     .run();
    //
    // future::try_join(app_server, health_server).await?;

    Ok(())
}

fn extract_client_cert(con: &dyn Any) -> Option<ClientCertificateChain> {
    log::debug!("Try extracting client cert");

    #[cfg(feature = "openssl")]
    if let Some(con) =
        con.downcast_ref::<actix_tls::openssl::SslStream<actix_web::rt::net::TcpStream>>()
    {
        log::debug!("Try extracting client cert: using OpenSSL");
        let chain = con.ssl().verified_chain();
        // **NOTE:** This chain (despite the function name) is **NOT** verified.
        // These are the client certificates, which will be passed on to the authentication service.
        let chain = chain
            .map(|chain| {
                log::debug!("Peer cert chain len: {}", chain.len());
                chain
                    .into_iter()
                    .map(|cert| cert.to_der())
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()
            .unwrap_or_else(|err| {
                log::info!("Failed to retrieve client certificate: {}", err);
                None
            });
        log::debug!("Client certificates: {:?}", chain);
        return chain.map(ClientCertificateChain);
    }
    #[cfg(feature = "rustls")]
    if let Some(con) =
        con.downcast_ref::<actix_tls::rustls::TlsStream<actix_web::rt::net::TcpStream>>()
    {
        log::debug!("Try extracting client cert: using rustls");
        use actix_tls::rustls::Session;
        return con
            .get_ref()
            .1
            .get_peer_certificates()
            .map(|certs| certs.iter().map(|cert| cert.0.clone()).collect())
            .map(ClientCertificateChain);
    }

    None
}
