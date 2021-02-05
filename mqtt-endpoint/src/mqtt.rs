use crate::x509::ClientCertificateRetriever;
use crate::{error::ServerError, server::Session, App};
use bytes::Bytes;
use bytestring::ByteString;
use drogue_cloud_endpoint_common::downstream::{Outcome, Publish, PublishResponse};
use drogue_cloud_service_api::auth::Outcome as AuthOutcome;
use ntex_mqtt::{
    types::QoS,
    v3,
    v5::{
        self,
        codec::{Auth, ConnectAckReason, DisconnectReasonCode, PublishAckReason},
    },
};
use std::fmt::Debug;
use tokio::sync::mpsc;

macro_rules! connect {
    ($connect:expr, $app:expr, $certs:expr) => {{
        log::info!("new connection: {:?}", $connect);
        match $app
            .authenticate(
                &$connect.packet().username,
                &$connect.packet().password,
                &$connect.packet().client_id,
                $certs,
            )
            .await?
        {
            AuthOutcome::Pass {
                application,
                device,
            } => {
                let (tx, mut rx) = mpsc::channel(32);

                let app_id = application.metadata.name.clone();
                let device_id = device.metadata.name.clone();

                let session = Session::new(
                    $app.downstream,
                    app_id.clone(),
                    device_id.clone(),
                    $app.devices.clone(),
                    tx,
                );

                let sink = $connect.sink().clone();
                ntex::rt::spawn(async move {
                    while let Some(cmd) = rx.recv().await {
                        match sink
                            .publish(ByteString::from_static("cmd"), Bytes::from(cmd))
                            .send_at_least_once()
                            .await
                        {
                            Ok(_) => {
                                log::debug!(
                                    "Command sent to device subscription {} / {}",
                                    app_id,
                                    device_id
                                )
                            }
                            Err(e) => log::error!(
                                "Failed to send a command to device subscription {:?}",
                                e
                            ),
                        }
                    }
                });

                Ok(session)
            }
            AuthOutcome::Fail => Err("Failed"),
        }
    }};
}

pub async fn connect_v3<Io>(
    mut connect: v3::Connect<Io>,
    app: App,
) -> Result<v3::ConnectAck<Io, Session>, ServerError>
where
    Io: ClientCertificateRetriever + 'static,
{
    let certs = connect.io().get_ref().client_certs();
    log::info!("Certs: {:?}", certs);

    // handle connect

    match connect!(connect, app, certs) {
        Ok(session) => Ok(connect.ack(session, false)),
        Err(_) => Ok(connect.bad_username_or_pwd()),
    }
}

pub async fn connect_v5<Io>(
    mut connect: v5::Connect<Io>,
    app: App,
) -> Result<v5::ConnectAck<Io, Session>, ServerError>
where
    Io: ClientCertificateRetriever + 'static,
{
    let certs = connect.io().get_ref().client_certs();
    log::info!("Certs: {:?}", certs);

    match connect!(connect, app, certs) {
        Ok(session) => Ok(connect.ack(session).with(|ack| {
            ack.wildcard_subscription_available = Some(false);
        })),
        Err(_) => Ok(connect.failed(ConnectAckReason::BadUserNameOrPassword)),
    }
}

macro_rules! publish {
    ($session: expr, $publish:expr) => {{
        log::info!(
            "incoming publish: {:?} -> {:?} / {:?}",
            $publish.id(),
            $publish.topic(),
            $publish.packet(),
        );
        let channel = $publish.topic().path();

        $session.state().sender.publish(
            Publish {
                channel: channel.into(),
                app_id: $session.tenant_id.clone(),
                device_id: $session.device_id.clone(),
                ..Default::default()
            },
            $publish.payload(),
        )
    }};
}

pub async fn publish_v3(
    session: v3::Session<Session>,
    publish: v3::Publish,
) -> Result<(), ServerError> {
    match publish!(session, publish).await {
        Ok(PublishResponse {
            outcome: Outcome::Accepted,
        }) => Ok(()),

        Ok(PublishResponse {
            outcome: Outcome::Rejected,
        }) => Err(ServerError {
            // with MQTTv3, we can only close the connection
            msg: "Rejected".into(),
        }),

        Err(e) => Err(ServerError { msg: e.to_string() }),
    }
}

pub async fn publish_v5(
    session: v5::Session<Session>,
    publish: v5::Publish,
) -> Result<v5::PublishAck, ServerError> {
    match publish!(session, publish).await {
        Ok(PublishResponse {
            outcome: Outcome::Accepted,
        }) => Ok(publish.ack()),
        Ok(PublishResponse {
            outcome: Outcome::Rejected,
        }) => Ok(publish
            .ack()
            .reason_code(PublishAckReason::UnspecifiedError)),
        Err(e) => Err(ServerError { msg: e.to_string() }),
    }
}

macro_rules! subscribe {
    ($s: expr, $session: expr, $fail: expr) => {{
        $s.iter_mut().for_each(|mut sub| {
            if sub.topic() == "command" {
                let mut devices = $session.state().devices.lock().unwrap();
                devices.insert(
                    $session.state().device_id.clone(),
                    $session.state().tx.clone(),
                );

                sub.subscribe(QoS::AtLeastOnce);

                log::debug!(
                    "Device '{:?}' subscribed to receive commands",
                    $session.state().device_id.clone()
                );
            } else {
                log::info!("Subscribing to topic {:?} not allowed", sub.topic());
                $fail(sub);
            }
        });

        Ok($s.ack())
    }};
}

macro_rules! unsubscribe {
    ($ack: expr, $session: expr, $log: expr) => {{
        let mut devices = $session.state().devices.lock().unwrap();
        devices.remove(&$session.state().device_id.clone());
        log::debug!($log, $session.state().device_id.clone());
        Ok($ack.ack())
    }};
}

pub async fn control_v3(
    session: v3::Session<Session>,
    control: v3::ControlMessage,
) -> Result<v3::ControlResult, ServerError> {
    match control {
        v3::ControlMessage::Ping(p) => Ok(p.ack()),
        v3::ControlMessage::Disconnect(d) => unsubscribe!(d, session, "Disconnecting device {:?}"),
        v3::ControlMessage::Subscribe(mut s) => {
            subscribe!(s, session, |mut sub: v3::control::Subscription| sub.fail())
        }
        v3::ControlMessage::Unsubscribe(u) => unsubscribe!(u, session, "Unsubscribing device {:?}"),
        v3::ControlMessage::Closed(c) => unsubscribe!(c, session, "Closing device connection {:?}"),
    }
}

pub async fn control_v5<E: Debug>(
    session: v5::Session<Session>,
    control: v5::ControlMessage<E>,
) -> Result<v5::ControlResult, ServerError> {
    match control {
        v5::ControlMessage::Auth(a) => Ok(a.ack(Auth::default())),
        v5::ControlMessage::Error(e) => Ok(e.ack(DisconnectReasonCode::UnspecifiedError)),
        v5::ControlMessage::ProtocolError(pe) => Ok(pe.ack()),
        v5::ControlMessage::Ping(p) => Ok(p.ack()),
        v5::ControlMessage::Disconnect(d) => unsubscribe!(d, session, "Disconnecting device {:?}"),
        v5::ControlMessage::Subscribe(mut s) => {
            subscribe!(s, session, |mut sub: v5::control::Subscription| sub
                .fail(v5::codec::SubscribeAckReason::NotAuthorized))
        }
        v5::ControlMessage::Unsubscribe(u) => unsubscribe!(u, session, "Unsubscribing device {:?}"),
        v5::ControlMessage::Closed(c) => unsubscribe!(c, session, "Closing device connection {:?}"),
    }
}
