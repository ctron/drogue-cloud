use crate::WebData;
use actix_web::{post, web, HttpResponse};
use drogue_cloud_registry_events::Event;
use serde_json::json;
use std::convert::TryInto;

#[post("/")]
pub async fn events(
    event: cloudevents::Event,
    data: web::Data<WebData>,
) -> Result<HttpResponse, actix_web::error::Error> {
    log::debug!("Received event: {:?}", event);

    let event = match event.try_into() {
        Ok(event) => event,
        Err(err) => {
            return Ok(HttpResponse::BadRequest().json(json!({ "details": format!("{}", err) })))
        }
    };

    log::debug!("Registry event: {:?}", event);

    Ok(match is_relevant(event) {
        Some((app, None)) => match data.controller.handle_app_event(app).await {
            Ok(_) => HttpResponse::Ok().finish(),
            Err(err) => HttpResponse::InternalServerError().json(json!({
                "details": err.to_string(),
            })),
        },
        _ => {
            // not relevant, consider contacting admin ;-)
            HttpResponse::Ok().finish()
        }
    })
}

fn is_relevant(event: Event) -> Option<(String, Option<String>)> {
    match event {
        Event::Application {
            path, application, ..
        } if path == "." => Some((application, None)),

        _ => None,
    }
}
