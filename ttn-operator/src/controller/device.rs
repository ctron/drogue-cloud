use super::Controller;
use crate::{data::*, ttn};
use async_trait::async_trait;
use drogue_client::{
    meta::{self, v1::CommonMetadataMut},
    registry, Dialect, Translator,
};
use drogue_cloud_operator_common::controller::reconciler::{
    ReconcileError, ReconcileState, Reconciler, ReconcilerOutcome,
};
use maplit::{convert_args, hashmap};

const FINALIZER: &str = "ttn";

pub struct ConstructContext {
    pub app: registry::v1::Application,
    pub app_spec: Option<TtnAppSpec>,
    pub app_status: Option<TtnAppStatus>,
    pub device: registry::v1::Device,
    pub device_spec: TtnDeviceSpec,
}

pub struct DeconstructContext {
    pub app_spec: Option<TtnAppSpec>,
    pub app_status: Option<TtnAppStatus>,
    pub device: registry::v1::Device,
}

pub struct DeviceReconciler<'a> {
    pub ttn: &'a ttn::Client,
}

#[async_trait]
impl<'a> Reconciler for DeviceReconciler<'a> {
    type Input = (registry::v1::Application, registry::v1::Device);
    type Output = registry::v1::Device;
    type Construct = ConstructContext;
    type Deconstruct = DeconstructContext;

    async fn eval_state(
        &self,
        input: Self::Input,
    ) -> Result<ReconcileState<Self::Output, Self::Construct, Self::Deconstruct>, ReconcileError>
    {
        let app = input.0;
        let device = input.1;

        let app_spec = app.section::<TtnAppSpec>().transpose()?;
        let app_status = app.section::<TtnAppStatus>().transpose()?;
        let device_spec = device.section::<TtnDeviceSpec>().transpose()?;

        let requested = device_spec.is_some();
        let configured = device.metadata.finalizers.iter().any(|f| f == FINALIZER);
        let deleted = device.metadata.deletion_timestamp.is_some();

        log::debug!(
            "State - requested: {}, configured: {}, deleted: {}",
            requested,
            configured,
            deleted
        );

        Ok(match (requested, configured, deleted) {
            (false, false, _) => {
                // nothing do to
                ReconcileState::Ignore(device)
            }
            (true, _, false) => {
                // we can unwrap here, as we checked before
                ReconcileState::Construct(ConstructContext {
                    app,
                    app_spec,
                    app_status,
                    device,
                    device_spec: device_spec.unwrap(),
                })
            }

            (_, _, true) | (false, true, _) => ReconcileState::Deconstruct(DeconstructContext {
                app_spec,
                app_status,
                device,
            }),
        })
    }

    async fn construct(
        &self,
        mut ctx: Self::Construct,
    ) -> Result<ReconcilerOutcome<Self::Output>, ReconcileError> {
        if ctx.device.metadata.ensure_finalizer(FINALIZER) {
            // early return
            return Ok(ReconcilerOutcome::Retry(ctx.device, None));
        }

        let mut device_status: TtnDeviceStatus = ctx
            .device
            .section()
            .and_then(|s| s.ok())
            .unwrap_or_default();

        // ensure

        // ensure we have a status section, and a stable app id

        let app_spec = ctx.app_spec.ok_or_else(|| {
            ReconcileError::permanent(
                "Missing TTN configuration in application. Unable to process.",
            )
        })?;
        let app_status = ctx.app_status.ok_or_else(|| {
            ReconcileError::temporary("Missing TTN status information in application. Waiting ...")
        })?;

        let app_id = app_status
            .app_id
            .as_ref()
            .unwrap_or(&ctx.app.metadata.name)
            .clone();

        Controller::ensure_stable_app_id(&ctx.app.metadata, &app_spec, &app_id)?;

        // ensure we have the gateway entry set

        if self.ensure_gateway_for_device(&mut ctx.device).await? {
            // device was changed, need to store
            return Ok(ReconcilerOutcome::Retry(ctx.device, None));
        }

        // ensure the device configuration

        self.ensure_device(&ctx.device.metadata, &app_spec, &app_id, &ctx.device_spec)
            .await?;

        device_status.reconcile = TtnReconcileStatus::reconciled(ctx.device.metadata.generation);
        ctx.device.set_section(device_status)?;

        Ok(ReconcilerOutcome::Complete(ctx.device))
    }

    async fn deconstruct(
        &self,
        mut ctx: Self::Deconstruct,
    ) -> Result<ReconcilerOutcome<Self::Output>, ReconcileError> {
        // delete

        // remove the ttn-gateway mapping

        ctx.device
            .spec
            .remove(registry::v1::DeviceSpecGatewaySelector::key());

        // delete the ttn device

        if let Some(app_id) = ctx.app_status.as_ref().and_then(|s| s.app_id.as_ref()) {
            let ttn_ctx = ctx
                .app_spec
                .ok_or_else(|| {
                    ReconcileError::permanent("Missing API configuration in application.")
                })?
                .api
                .to_context()?;

            let device_id = &ctx.device.metadata.name;

            self.ttn
                .delete_device(&app_id, &device_id, &ttn_ctx)
                .await?;
        }

        // remove the status section

        ctx.device.status.remove("ttn");

        // remove the finalizer

        ctx.device.metadata.finalizers.retain(|f| f != FINALIZER);

        // done

        Ok(ReconcilerOutcome::Complete(ctx.device))
    }
}

impl<'a> DeviceReconciler<'a> {
    /// Ensure that the device has the TTN gateway set
    async fn ensure_gateway_for_device(
        &self,
        device: &mut registry::v1::Device,
    ) -> Result<bool, ReconcileError> {
        let mut gw = device
            .section::<registry::v1::DeviceSpecGatewaySelector>()
            .and_then(|s| s.ok())
            .unwrap_or_default();

        let original_gw = gw.clone();
        gw.match_names = vec!["ttn-gateway".into()];

        let changed = gw != original_gw;
        device.set_section(gw)?;

        Ok(changed)
    }

    async fn ensure_device(
        &self,
        metadata: &meta::v1::ScopedMetadata,
        app_spec: &TtnAppSpec,
        ttn_app_id: &str,
        spec: &TtnDeviceSpec,
    ) -> Result<(), ReconcileError> {
        let ctx = app_spec.api.to_context()?;
        let ttn_device_id = &metadata.name;

        let ttn_device = self.ttn.get_device(ttn_app_id, ttn_device_id, &ctx).await?;

        log::debug!("TTN device: {:#?}", ttn_device);

        let server = app_spec
            .api
            .region
            .url()
            .map_err(|_| ReconcileError::permanent("Failed to parse TTN API URL"))?;
        let server = server
            .host_str()
            .ok_or_else(|| ReconcileError::permanent("Missing hostname of TTP API"))?;

        let device = ttn::Device {
            ids: ttn::DeviceIds {
                device_id: ttn_device_id.clone(),
                dev_eui: spec.dev_eui.clone(),
                join_eui: spec.app_eui.clone(),
            },
            end_device: ttn::EndDevice {
                name: ttn_device_id.clone(),
                network_server_address: server.into(),
                application_server_address: server.into(),
                join_server_address: server.into(),
            },
            ns_device: ttn::NsDevice {
                multicast: false,
                supports_join: true,
                lorawan_version: or_default(&spec.lorawan_version, "MAC_V1_0"),
                lorawan_phy_version: or_default(&spec.lorawan_phy_version, "PHY_V1_0"),
                mac_settings: convert_args!(hashmap!("supports_32_bit_f_cnt" => true)),
                supports_class_b: spec.supports_class_b,
                supports_class_c: spec.supports_class_b,
                frequency_plan_id: spec.frequency_plan_id.clone(),
            },
            js_device: ttn::JsDevice {
                network_server_address: server.into(),
                application_server_address: server.into(),
                join_server_address: server.into(),

                network_server_kek_label: "".into(),
                application_server_kek_label: "".into(),
                application_server_id: "".into(),
                net_id: Default::default(),

                root_keys: ttn::RootKeys {
                    app_key: ttn::Key {
                        key: spec.app_key.clone(),
                    },
                },
            },
        };

        match ttn_device {
            None => {
                log::info!("Creating new device");
                self.ttn.create_device(ttn_app_id, device, &ctx).await?;
            }
            Some(_) => {
                log::info!("Updating existing device");
                self.ttn.update_device(ttn_app_id, device, &ctx).await?;
            }
        }

        Ok(())
    }
}

/// Return the string, or the default value if there is no string or the string is empty.
fn or_default(s: &Option<String>, d: &str) -> String {
    s.as_ref()
        .filter(|s| !s.is_empty())
        .map_or_else(|| d.to_string(), |s| s.to_string())
}
