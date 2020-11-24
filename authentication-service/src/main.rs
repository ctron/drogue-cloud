mod models;
mod schema;
mod auth;
mod database;

use actix_web::{get, web, App, HttpResponse, HttpServer};
use actix_web_httpauth::extractors::basic::BasicAuth;
use actix_web::http::header::ContentType;

use serde::Deserialize;

use dotenv::dotenv;
use envconfig::Envconfig;

use std::borrow::Cow;

#[derive(Clone, Debug, Deserialize)]
struct Secret {
    hash: String,
    salt: String,
}

#[derive(Debug)]
enum AuthenticationResult {
    Success,
    Failed,
    Error,
}

#[get("/auth")]
async fn password_authentication(
    auth: BasicAuth,
    data: web::Data<WebData>,
) -> Result<HttpResponse, actix_web::Error> {

    let connection = database::pg_pool_handler(&data.connection_pool)?;
    let auth_result;
    let cred = database::get_credential(&auth.user_id(), &connection)?;
    let props = database::serialise_props(cred.properties);

    auth_result = auth::verify_password(
        &auth.password().unwrap_or(&Cow::from("")),
        cred.secret
    );

    match auth_result {
        AuthenticationResult::Success =>
            Ok(HttpResponse::Ok()
                .set(ContentType::json())
                .body(props)),
        AuthenticationResult::Failed => Ok(HttpResponse::Unauthorized().finish()),
        AuthenticationResult::Error => Ok(HttpResponse::BadRequest().finish()),
    }
}


#[get("/jwt")]
async fn token_authentication(
    auth: BasicAuth,
    data: web::Data<WebData>,
) -> Result<HttpResponse, actix_web::Error> {
    log::info!(
        "Received Authentication request for device: {}",
        auth.user_id()
    );

    let connection = database::pg_pool_handler(&data.connection_pool)?;
    let auth_result;
    let cred = database::get_credential(&auth.user_id(), &connection)?;

    auth_result = auth::verify_password(
        &auth.password().unwrap_or(&Cow::from("")),
        cred.secret
    );
    let props = database::serialise_props(cred.properties);

    //issue token if auth is successful
    match auth_result {
        AuthenticationResult::Success => {
            let token = auth::get_jwt_token(
                &auth.user_id(),
                &data.token_signing_private_key,
                data.token_expiration_seconds,
            );
            match token {
                Ok(token) => {
                    log::debug!(
                        "Issued JWT for device {}. Token: {}",
                        auth.user_id(),
                        token
                    );
                    Ok(HttpResponse::Ok()
                        .set(ContentType::json())
                        .header("Authorization", token)
                        .body(props))
                }
                Err(e) => {
                    log::error!("Could not issue JWT token: {}", e);
                    Ok(HttpResponse::InternalServerError()
                        .content_type("text/plain")
                        .body("error encoding the JWT"))
                }
            }
        }
        AuthenticationResult::Failed => Ok(HttpResponse::Unauthorized().finish()),
        AuthenticationResult::Error => Ok(HttpResponse::BadRequest().finish()),
    }
}



#[derive(Clone)]
struct WebData {
    connection_pool: database::PgPool,
    token_expiration_seconds: u64,
    token_signing_private_key: Vec<u8>,
}

#[derive(Envconfig)]
struct Config {
    #[envconfig(from = "DATABASE_URL")]
    pub db_url: String,
    #[envconfig(from = "BIND_ADDR", default = "127.0.0.1:8080")]
    pub bind_addr: String,

    #[envconfig(from = "TOKEN_EXPIRATION", default = "300")]
    pub jwt_expiration: u64,
    #[envconfig(from = "JWT_ECDSA_SIGNING_KEY")]
    pub jwt_signing_key: Option<String>,
    #[envconfig(from = "ENABLE_JWT", default = "false")]
    pub enable_jwt: bool,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    dotenv().ok();

    // Initialize config from environment variables
    let config = Config::init_from_env().unwrap();
    let data : WebData;
    let app = App::new();

    let pool = database::establish_connection(config.db_url);
    if config.enable_jwt {
        data = WebData{
            connection_pool: pool,
            token_expiration_seconds: config.jwt_expiration,
            token_signing_private_key: std::fs::read(config.jwt_signing_key
                .expect("JWT_ECDSA_SIGNING_KEY must be set")).unwrap(),
        };
        // add the JWT service to the web server.
        app.service(token_authentication).data(data.clone());
    } else {
        data = WebData{
            connection_pool: pool,
            token_expiration_seconds: 0,
            token_signing_private_key: Vec::new(),
        };
    }

    //todo use a separate config function
    if config.enable_jwt {
        HttpServer::new(move || App::new()
            .service(token_authentication).data(data.clone())
            .service(password_authentication).data(data.clone()))
            .bind(config.bind_addr)?
            .run()
            .await
    } else {
        HttpServer::new(move || App::new()
            .service(password_authentication).data(data.clone()))
            .bind(config.bind_addr)?
            .run()
            .await
    }
}
