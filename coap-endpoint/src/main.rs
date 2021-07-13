mod auth;
mod downstream;
mod response;
mod telemetry;
mod error;

//mod command;
//mod server;

use crate::auth::DeviceAuthenticator;
use crate::error::CoapEndpointError;
use crate::response::Responder;
use dotenv::dotenv;
use drogue_cloud_endpoint_common::{
    downstream::{DownstreamSender, DownstreamSink, KafkaSink},
    error::EndpointError,
};
use drogue_cloud_service_common::{config::ConfigFromEnv, defaults, health::HealthServerConfig};
use futures;
use std::collections::LinkedList;
use telemetry::PublishOptions;
//use http::HeaderValue;
//use log;
use std::net::SocketAddr;

//use bytes::Bytes;
//use bytestring::ByteString;
use coap::Server;
use coap_lite::{CoapOption, CoapRequest, CoapResponse};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub disable_tls: bool,
    #[serde(default)]
    pub cert_bundle_file: Option<String>,
    #[serde(default)]
    pub key_file: Option<String>,
    #[serde(default)]
    pub bind_addr_coap: Option<String>,
    #[serde(default)]
    pub bind_addr_mqtt: Option<String>,
    #[serde(default = "defaults::bind_addr")]
    pub bind_addr_http: String,
    #[serde(default)]
    pub health: HealthServerConfig,
}

#[derive(Clone, Debug)]
pub struct App<S>
where
    S: DownstreamSink + Send,
    <S as DownstreamSink>::Error: Send,
{
    pub downstream: DownstreamSender<S>,
    pub authenticator: DeviceAuthenticator,
    // pub commands: Commands,
}

fn uri_parser(ll: &LinkedList<Vec<u8>>) -> Result<Vec<String>, EndpointError> {
    let mut linked_list = ll.iter();
    let mut option_values = Vec::new();
    linked_list
        .next()
        .map(|x| String::from_utf8(x.clone()).ok())
        .flatten()
        .filter(|x| x.eq("v1"))
        .ok_or_else(|| EndpointError::InvalidRequest {
            details: "incorrect version number".to_string(),
        })?;
    let channel = linked_list
        .next()
        .map(|x| String::from_utf8(x.clone()).ok())
        .flatten()
        .ok_or_else(|| EndpointError::InvalidRequest {
            details: "error parsing channel".to_string(),
        })?;

    let mut subject = String::new();
    for i in linked_list {
        subject.push_str(std::str::from_utf8(i).map_err(|err| {
            return EndpointError::InvalidRequest {
                details: format!("error parsing channel: {:?}", err).to_string(),
            };
        })?);
        subject.push('/');
    }

    option_values.push(channel);
    if subject.len() != 0 {
        subject.pop();
        option_values.push(subject);
    }
    Ok(option_values)
}

fn params(
    request: &CoapRequest<SocketAddr>,
) -> Result<(Vec<String>, Option<&Vec<u8>>, &Vec<u8>), anyhow::Error> {
    let path_segments = request
        .message
        .get_option(CoapOption::UriPath)
        .map(|uri| uri_parser(uri).ok())
        .flatten()
        .ok_or_else(|| anyhow::Error::msg("Error parsing path segments"))?; // TODO: see how this behaves, should you put in separate function?
    let queries = request
        .message
        .get_option(CoapOption::UriQuery)
        .map(|x| (x.front()))
        .flatten();
    let auth = request
        .message
        .get_option(CoapOption::Unknown(4209))
        .map(|x| x.front())
        .flatten()
        .ok_or_else(|| anyhow::Error::msg("Error parsing path segments"))?;
    Ok((path_segments, queries, auth))
}

async fn publish_handler<S>(
    mut request: CoapRequest<SocketAddr>,
    app: App<S>,
) -> Option<CoapResponse>
where
    S: DownstreamSink + Send,
    <S as DownstreamSink>::Error: Send,
{
    let path_segments: Vec<String>;
    let queries: Option<&Vec<u8>>;
    let auth: &Vec<u8>;

    if let Ok((p, q, a)) = params(&request) {
        path_segments = p;
        queries = q;
        auth = a;
    } else {
        let ret = Err(CoapEndpointError(EndpointError::InvalidRequest {
            details: "Invalid Path".to_string(),
        }))
        .respond_to(&mut request);
        return ret;
    }

    let options = queries
        .map(|x| serde_urlencoded::from_bytes::<PublishOptions>(x).ok())
        .flatten()
        .unwrap_or_default();

    match path_segments.len() {
        1 => telemetry::publish_plain(
            app.downstream,
            app.authenticator,
            path_segments[0].clone(),
            options,
            request.clone(),
            auth,
        )
        .await
        .respond_to(&mut request),

        2 => telemetry::publish_tail(
            app.downstream,
            app.authenticator,
            (path_segments[0].clone(), path_segments[1].clone()),
            queries
                .map(|x| serde_urlencoded::from_bytes::<PublishOptions>(x))?
                .map_err(anyhow::Error::from)
                .ok()?,
            request.clone(),
            auth,
        )
        .await
        .respond_to(&mut request),

        _ => Err(CoapEndpointError(EndpointError::InvalidRequest {
            details: "Invalid Path".to_string(),
        }))
        .respond_to(&mut request),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    dotenv().ok();

    let config = Config::from_env()?;

    let addr = config
        .bind_addr_coap
        .unwrap_or("0.0.0.0:5683".to_string());

    let app = App {
        downstream: DownstreamSender::new(KafkaSink::new("DOWNSTREAM_KAFKA_SINK")?)?,
        authenticator: DeviceAuthenticator(
            drogue_cloud_endpoint_common::auth::DeviceAuthenticator::new().await?,
        ),
    };

    println!("Server up on {}", addr);
    let mut server = Server::new(addr).unwrap();

    let device_to_endpoint = server.run(move |request| publish_handler(request, app.clone()));

    //let health = HealthServer::new(config.health, vec![]);

    futures::try_join!(/*health.run_ntex(),*/ device_to_endpoint)?;
    Ok(())
}
