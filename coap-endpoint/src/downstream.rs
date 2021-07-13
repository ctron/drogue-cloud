use std::net::SocketAddr;

//use crate::command::wait_for_command;
use async_trait::async_trait;
use coap_lite::{CoapRequest, CoapResponse, ResponseType};
use drogue_cloud_endpoint_common::downstream::PublishOutcome;
use drogue_cloud_endpoint_common::{
    //commands::Commands,
    downstream::{DownstreamSender, DownstreamSink, Publish},
    error::{CoapEndpointError, EndpointError},
};
//use drogue_cloud_service_common::Id;

#[async_trait]
pub trait CoapCommandSender {
    async fn publish_and_await<B>(
        &self,
        publish: Publish,
        //commands: Commands,
        _ttd: Option<u64>,
        //command: CommandWait,
        body: B,
        req: CoapRequest<SocketAddr>,
    ) -> Result<Option<CoapResponse>, CoapEndpointError>
    where
        B: AsRef<[u8]> + Send;
}

#[async_trait]
impl<S> CoapCommandSender for DownstreamSender<S>
where
    S: DownstreamSink + Send + Sync,
    <S as DownstreamSink>::Error: Send,
{
    async fn publish_and_await<B>(
        &self,
        publish: Publish,
        //commands: Commands,
        _ttd: Option<u64>,
        body: B,
        req: CoapRequest<SocketAddr>,
    ) -> Result<Option<CoapResponse>, CoapEndpointError>
    where
        B: AsRef<[u8]> + Send,
    {
        //let id = Id::new(&publish.app_id, &publish.device_id);
        match self.publish(publish, body).await {
            // TODO finish after command
            // ok, and accepted
            Ok(PublishOutcome::Accepted) => Ok(req.response.and_then(|mut v| {
                v.set_status(ResponseType::Changed);
                Some(v)
            })), //wait_for_command(commands, id, ttd).await

            // ok, but rejected
            Ok(PublishOutcome::Rejected) => Ok(req.response.and_then(|mut v| {
                v.set_status(ResponseType::NotAcceptable);
                Some(v)
            })),

            // ok, but queue full
            Ok(PublishOutcome::QueueFull) => Ok(req.response.and_then(|mut v| {
                v.set_status(ResponseType::ServiceUnavailable);
                Some(v)
            })),

            // internal error
            Err(err) => Err(CoapEndpointError(EndpointError::ConfigurationError {
                details: err.to_string(),
            })),
        }
    }
}
