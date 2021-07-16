mod app;
mod device;

use app::*;
use device::*;

use crate::{
    data::{TtnAppSpec, TtnAppStatus, TtnDeviceStatus, TtnReconcileStatus},
    ttn,
};
use drogue_client::{meta, registry, Translator};
use drogue_cloud_operator_common::controller::reconciler::{
    ReconcileError, ReconcileProcessor, ReconcilerOutcome,
};
use url::Url;

pub struct Controller {
    registry: registry::v1::Client,
    ttn: ttn::Client,
    endpoint_url: Url,
}

impl Controller {
    pub fn new(registry: registry::v1::Client, ttn: ttn::Client, endpoint_url: Url) -> Self {
        Self {
            registry,
            ttn,
            endpoint_url,
        }
    }

    pub async fn handle_device_event(
        &self,
        app_id: String,
        device_id: String,
    ) -> Result<(), anyhow::Error> {
        log::info!("Device changed: {} / {}", app_id, device_id);

        let app = self.registry.get_app(&app_id, Default::default()).await?;
        let device = self
            .registry
            .get_device(&app_id, &device_id, Default::default())
            .await?;
        log::debug!("Reconcile device: {:#?}", device);

        if let (Some(app), Some(mut device)) = (app, device) {
            let result = ReconcileProcessor(DeviceReconciler { ttn: &self.ttn })
                .reconcile((app, device.clone()))
                .await
                .or_else::<ReconcileError, _>(|err| {
                    let generation = device.metadata.generation;
                    log::info!("Failed to reconcile: {}", err);
                    device.update_section(|mut status: TtnDeviceStatus| {
                        status.reconcile = TtnReconcileStatus::failed(generation, err);
                        status
                    })?;

                    Ok(ReconcilerOutcome::Complete(device))
                })?;

            log::debug!("Storing: {:#?}", result);

            let (device, retry) = result.split();

            self.registry
                .update_device(&device, Default::default())
                .await?;
        } else {
            // If application and/or device are missing, we have nothing to do. As we have
            // finalizers to guard against this.
            // If any of the resources is gone, we can ignore this.
        }

        Ok(())
    }

    pub async fn handle_app_event(&self, app: String) -> Result<(), anyhow::Error> {
        log::info!("Application changed: {}", app);

        let app = self.registry.get_app(&app, Default::default()).await?;
        log::debug!("Reconcile application: {:#?}", app);

        if let Some(mut app) = app {
            let app = ReconcileProcessor(ApplicationReconciler {
                ttn: &self.ttn,
                registry: &self.registry,
                endpoint_url: &self.endpoint_url,
            })
            .reconcile(app.clone())
            .await
            .or_else::<ReconcileError, _>(|err| {
                log::info!("Failed to reconcile: {}", err);
                let generation = app.metadata.generation;
                app.update_section(|mut status: TtnAppStatus| {
                    status.reconcile = TtnReconcileStatus::failed(generation, err);
                    status
                })?;

                Ok(ReconcilerOutcome::Complete(app))
            })?;

            let (app, retry) = app.split();

            log::debug!("Storing: {:#?}", app);
            self.registry.update_app(&app, Default::default()).await?;
        } else {
            // If the application is just gone, we can ignore this, as we have finalizers
            // to guard against this.
        }

        Ok(())
    }

    /// Ensure that the app ID did not change.
    pub fn ensure_stable_app_id(
        meta: &meta::v1::NonScopedMetadata,
        spec: &TtnAppSpec,
        current_app_id: &str,
    ) -> Result<(), ReconcileError> {
        let defined_id = spec.api.id.as_ref().unwrap_or(&meta.name);
        if defined_id != current_app_id {
            Err(ReconcileError::permanent(format!(
                "Application IDs have changed - requested: {}, current: {}",
                defined_id, current_app_id
            )))
        } else {
            Ok(())
        }
    }
}
