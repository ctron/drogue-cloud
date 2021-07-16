mod app;
pub mod base;

use crate::{controller::app::ApplicationReconciler, data::KafkaAppStatus};
use drogue_client::{core, registry, Translator};
use drogue_cloud_operator_common::controller::reconciler::{
    ReconcileError, ReconcileProcessor, ReconcilerOutcome,
};
use kube::{
    api::{ApiResource, DynamicObject},
    Api,
};
use serde::Deserialize;
use std::time::Duration;

const CONDITION_READY: &str = "KafkaReady";

#[derive(Clone, Debug, Deserialize)]
pub struct ControllerConfig {
    /// The namespace in which the topics get created
    pub topic_namespace: String,
    /// The resource name of the Kafka cluster.
    ///
    /// This will be used as the `strimzi.io/cluster` label value.
    pub cluster_name: String,
}

pub struct Controller {
    config: ControllerConfig,
    registry: registry::v1::Client,
    kafka_topic_resource: ApiResource,
    kafka_topics: Api<DynamicObject>,
}

impl Controller {
    pub fn new(
        config: ControllerConfig,
        registry: registry::v1::Client,
        kafka_topic_resource: ApiResource,
        kafka_topics: Api<DynamicObject>,
    ) -> Self {
        Self {
            config,
            registry,
            kafka_topic_resource,
            kafka_topics,
        }
    }

    pub async fn handle_app_event(
        &self,
        app: String,
    ) -> Result<Option<Option<Duration>>, anyhow::Error> {
        log::info!("Application changed: {}", app);

        let app = self.registry.get_app(&app, Default::default()).await?;
        log::debug!("Reconcile application: {:#?}", app);

        if let Some(mut app) = app {
            let app = ReconcileProcessor(ApplicationReconciler {
                config: &self.config,
                registry: &self.registry,
                kafka_topic_resource: &self.kafka_topic_resource,
                kafka_topics: &self.kafka_topics,
            })
            .reconcile(app.clone())
            .await
            .or_else::<ReconcileError, _>(|err| {
                // this is a fatal error which cannot be recovered

                log::info!("Failed to reconcile: {}", err);
                let generation = app.metadata.generation;
                app.update_section(|mut status: KafkaAppStatus| {
                    status.observed_generation = generation;
                    status.reason = Some(err.to_string());
                    status.state = "Failed".to_string();
                    status
                })?;
                app.update_section(|mut conditions: core::v1::Conditions| {
                    conditions.update(CONDITION_READY, core::v1::ConditionStatus::default());
                    conditions
                })?;

                match err {
                    ReconcileError::Temporary(_) => Ok(ReconcilerOutcome::Retry(app, None)),
                    ReconcileError::Permanent(_) => Ok(ReconcilerOutcome::Complete(app)),
                }
            })?;
            log::debug!("Storing: {:#?}", app);

            let (app, retry) = app.split();

            self.registry.update_app(&app, Default::default()).await?;

            Ok(retry)
        } else {
            // If the application is just gone, we can ignore this, as we have finalizers
            // to guard against this.
            Ok(None)
        }
    }
}
