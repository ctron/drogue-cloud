//! Http context command handlers
//!
//! Contains actors that handles commands for HTTP endpoint

use actix::prelude::*;
use actix_web::{http, post, web, web::Bytes, HttpResponse};
use actix_web_actors::HttpContext;
use cloudevents_sdk_actix_web::HttpRequestExt;
use drogue_cloud_endpoint_common::{
    command_router::{CommandMessage, CommandRouter, CommandSubscribe, CommandUnsubscribe},
    error::HttpEndpointError,
    Id,
};
use std::time;

#[post("/command-service")]
pub async fn command_service(
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

/// Actor for receiving commands
pub struct CommandHandler {
    pub device_id: Id,
    pub ttd: u64,
}

impl Actor for CommandHandler {
    type Context = HttpContext<Self>;

    /// Subscribes the actor with the command handler
    /// and waits for the command for `ttd` seconds
    fn started(&mut self, ctx: &mut HttpContext<Self>) {
        let sub = CommandSubscribe(self.device_id.clone(), ctx.address().recipient());
        CommandRouter::from_registry()
            .send(sub)
            .into_actor(self)
            .then(|result, _actor, _ctx| {
                match result {
                    Ok(_v) => {
                        log::debug!("Sent command subscribe request");
                    }
                    Err(e) => {
                        log::error!("Subscribe request failed: {}", e);
                    }
                }
                fut::ready(())
            })
            .wait(ctx);

        // Wait for ttd seconds for a command
        ctx.run_later(time::Duration::from_secs(self.ttd), |_slf, ctx| {
            ctx.write_eof()
        });
    }

    /// Unsubscribes the actor from receiving the commands
    fn stopped(&mut self, ctx: &mut HttpContext<Self>) {
        CommandRouter::from_registry()
            .send(CommandUnsubscribe(self.device_id.clone()))
            .into_actor(self)
            .then(|result, _actor, _ctx| {
                match result {
                    Ok(_v) => {
                        log::debug!("Sent command unsubscribe request");
                    }
                    Err(e) => {
                        log::error!("Unsubscribe request failed: {}", e);
                    }
                }
                fut::ready(())
            })
            .wait(ctx);
    }
}

impl Handler<CommandMessage> for CommandHandler {
    type Result = ();

    /// Handles q command message by writing it into the http context
    fn handle(&mut self, msg: CommandMessage, ctx: &mut HttpContext<Self>) {
        ctx.write(Bytes::from(msg.command));
        ctx.write_eof()
    }
}

/// Settings for waiting on commands.
///
/// The default is to not wait for commands.
#[derive(Clone, Debug, Default)]
pub struct CommandWait {
    /// The duration to wait for an incoming command.
    ///
    /// If the duration is `None` or considered "zero", then the operation will not wait for a
    /// command. **Note:** If the duration is expected to be seconds based, but the provided
    /// duration shorter than a second, that may be treated as zero.
    pub duration: Option<time::Duration>,
}

impl CommandWait {
    /// Conveniently map a number of seconds value into a command wait operation.
    pub fn from_secs(secs: Option<u64>) -> Self {
        Self {
            duration: secs.map(time::Duration::from_secs),
        }
    }
}

/// Waits for a command for a `command.duration` seconds by creating a command handler actor
pub async fn command_wait<A: ToString, D: ToString>(
    app_id: A,
    device_id: D,
    command: CommandWait,
    status: http::StatusCode,
) -> Result<HttpResponse, HttpEndpointError> {
    match command.duration.map(|d| d.as_secs()) {
        Some(ttd) if ttd > 0 => {
            let handler = CommandHandler {
                device_id: Id::new(app_id, device_id),
                ttd,
            };
            let context = HttpContext::create(handler);
            Ok(HttpResponse::build(status).streaming(context))
        }
        _ => Ok(HttpResponse::build(status).finish()),
    }
}
