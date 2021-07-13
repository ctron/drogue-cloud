mod app;

use crate::{controller::app::ApplicationReconciler, data::KafkaAppStatus};
use drogue_client::{core, registry, Translator};
use drogue_cloud_operator_common::controller::reconciler::{ReconcileError, ReconcileProcessor};
use kube::{
    api::{ApiResource, DynamicObject},
    Api,
};
use serde::Deserialize;

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

    pub async fn handle_app_event(&self, app: String) -> Result<(), anyhow::Error> {
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
                log::info!("Failed to reconcile: {}", err);
                let generation = app.metadata.generation;
                app.update_section(|_: KafkaAppStatus| KafkaAppStatus::failed(generation, err))?;
                app.update_section(|mut conditions: core::v1::Conditions| {
                    conditions.update(CONDITION_READY, core::v1::ConditionStatus::default());
                    conditions
                })?;

                Ok(app)
            })?;
            log::debug!("Storing: {:#?}", app);
            self.registry.update_app(app, Default::default()).await?;
        } else {
            // If the application is just gone, we can ignore this, as we have finalizers
            // to guard against this.
        }

        Ok(())
    }
}
