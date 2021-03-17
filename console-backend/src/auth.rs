use actix_web::{get, http, web, HttpRequest, HttpResponse, Responder};
use drogue_cloud_console_common::UserInfo;
use drogue_cloud_service_common::error::ErrorResponse;
use openid::{biscuit::jws::Compact, Bearer, Configurable};
use serde::Deserialize;
use serde_json::json;
use std::fmt::Debug;

pub struct OpenIdClient {
    pub client: openid::Client,
    pub scopes: String,
}

#[get("/ui/login")]
pub async fn login(req: HttpRequest) -> impl Responder {
    let login_handler: Option<&web::Data<OpenIdClient>> = req.app_data();

    if let Some(client) = login_handler {
        let auth_url = client.client.auth_uri(Some(&client.scopes), None);

        HttpResponse::Found()
            .append_header((http::header::LOCATION, auth_url.to_string()))
            .finish()
    } else {
        // if we are missing the authenticator, we hide ourselves
        HttpResponse::NotFound().finish()
    }
}

/// An endpoint that will redirect to the SSO "end session" endpoint
#[get("/ui/logout")]
pub async fn logout(req: HttpRequest) -> impl Responder {
    let login_handler: Option<&web::Data<OpenIdClient>> = req.app_data();

    if let Some(client) = login_handler {
        if let Some(url) = &client.client.provider.config().end_session_endpoint {
            let mut url = url.clone();

            if let Some(redirect) = &client.client.redirect_uri {
                url.query_pairs_mut().append_pair("redirect_uri", redirect);
            }

            return HttpResponse::Found()
                .append_header((http::header::LOCATION, url.to_string()))
                .finish();
        } else {
            log::info!("Missing logout URL");
        }
    }

    // if we are missing the authenticator, we hide ourselves
    HttpResponse::NotFound().finish()
}

#[derive(Deserialize, Debug)]
pub struct LoginQuery {
    code: String,
    nonce: Option<String>,
}

#[get("/ui/token")]
pub async fn code(req: HttpRequest, query: web::Query<LoginQuery>) -> impl Responder {
    let login_handler: Option<&web::Data<OpenIdClient>> = req.app_data();

    if let Some(client) = login_handler {
        let response = client
            .client
            .authenticate(&query.code, query.nonce.as_deref(), None)
            .await;

        log::info!(
            "Response: {:?}",
            response.as_ref().map(|r| r.bearer.clone())
        );

        match response {
            Ok(token) => {
                let userinfo = token.id_token.and_then(|t| match t {
                    Compact::Decoded { payload, .. } => Some(UserInfo {
                        email_verified: payload.userinfo.email_verified,
                        email: payload.userinfo.email,
                    }),
                    Compact::Encoded(_) => None,
                });

                HttpResponse::Ok()
                    .json(json!({ "bearer": token.bearer, "expires": token.bearer.expires, "userinfo": userinfo}))
            }
            Err(err) => HttpResponse::Unauthorized().json(ErrorResponse {
                error: "Unauthorized".to_string(),
                message: format!("Code invalid: {:?}", err),
            }),
        }
    } else {
        // if we are missing the authenticator, we hide ourselves
        HttpResponse::NotFound().finish()
    }
}

#[derive(Deserialize, Debug)]
pub struct RefreshQuery {
    refresh_token: String,
}

#[get("/ui/refresh")]
pub async fn refresh(req: HttpRequest, query: web::Query<RefreshQuery>) -> impl Responder {
    let login_handler: Option<&web::Data<OpenIdClient>> = req.app_data();
    if let Some(client) = login_handler {
        let response = client
            .client
            .refresh_token(
                Bearer {
                    refresh_token: Some(query.0.refresh_token),
                    access_token: String::new(),
                    expires: None,
                    id_token: None,
                    scope: None,
                },
                None,
            )
            .await;

        log::info!("Response: {:?}", response.as_ref());

        match response {
            Ok(bearer) => {
                HttpResponse::Ok().json(json!({ "bearer": bearer, "expires": bearer.expires, }))
            }
            Err(err) => HttpResponse::Unauthorized().json(ErrorResponse {
                error: "Unauthorized".to_string(),
                message: format!("Refresh token invalid: {:?}", err),
            }),
        }
    } else {
        // if we are missing the authenticator, we hide ourselves
        HttpResponse::NotFound().finish()
    }
}
