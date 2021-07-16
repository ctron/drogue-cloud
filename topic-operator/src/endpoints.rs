use crate::controller::base::{EventSource, FnEventProcessor};
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

    data.into_inner().processor;

    let mut controller = EventSource::new(FnEventProcessor::new(
        &mut data.get_ref().processor,
        is_relevant,
    ));

    Ok(match controller.handle(event).await {
        Ok(_) => HttpResponse::Ok().finish(),
        Err(_) => HttpResponse::InternalServerError().finish(),
    })
    /*
       Ok(match is_relevant(&event) {
           Some((app, None)) => match data.controller.handle_app_event(app).await {
               Ok(retry) => {
                   if let Some(retry) = retry {
                       data.outbox
                           .reschedule(event, retry, Default::default())
                           .await
                           .map_err(|err| {
                               HttpResponse::InternalServerError()
                                   .json(json!({"details": err.to_string()}))
                           })?;
                   }

                   HttpResponse::Ok().finish()
               }
               Err(err) => HttpResponse::InternalServerError().json(json!({
                   "details": err.to_string(),
               })),
           },
           _ => {
               // not relevant, consider contacting admin ;-)
               HttpResponse::Ok().finish()
           }
       })
    */
}

fn is_relevant(event: &Event) -> Option<String> {
    match event {
        Event::Application {
            path, application, ..
        } if
        // watch the creation of a new application
        path == "." ||
            // watch the finalizer addition
            path == ".metadata" => Some(application.clone()),

        _ => None,
    }
}
