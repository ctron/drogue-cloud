use crate::controller::CONDITION_READY;
use crate::{controller::ControllerConfig, data::*};
use async_trait::async_trait;
use drogue_client::{core, meta::v1::CommonMetadataMut, registry, Translator};
use drogue_cloud_operator_common::controller::reconciler::{
    ReconcileError, ReconcileState, Reconciler,
};
use kube::{
    api::{ApiResource, DynamicObject},
    Api, Resource,
};
use operator_framework::install::Delete;
use operator_framework::process::create_or_update_by;
use serde_json::json;

const FINALIZER: &str = "kafka";
const LABEL_KAFKA_CLUSTER: &str = "strimzi.io/cluster";

pub struct ConstructContext {
    pub app: registry::v1::Application,
    pub status: Option<KafkaAppStatus>,
}

pub struct DeconstructContext {
    pub app: registry::v1::Application,
    pub status: Option<KafkaAppStatus>,
}

pub struct ApplicationReconciler<'a> {
    pub config: &'a ControllerConfig,
    pub registry: &'a registry::v1::Client,
    pub kafka_topic_resource: &'a ApiResource,
    pub kafka_topics: &'a Api<DynamicObject>,
}

#[async_trait]
impl<'a> Reconciler for ApplicationReconciler<'a> {
    type Input = registry::v1::Application;
    type Output = registry::v1::Application;
    type Construct = ConstructContext;
    type Deconstruct = DeconstructContext;

    async fn eval_state(
        &self,
        app: Self::Input,
    ) -> Result<ReconcileState<Self::Output, Self::Construct, Self::Deconstruct>, ReconcileError>
    {
        let status = app.section::<KafkaAppStatus>().and_then(|s| s.ok());

        let configured = app.metadata.finalizers.iter().any(|f| f == FINALIZER);
        let deleted = app.metadata.deletion_timestamp.is_some();

        Ok(match (configured, deleted) {
            (_, false) => ReconcileState::Construct(ConstructContext { app, status }),
            (true, true) => ReconcileState::Deconstruct(DeconstructContext { app, status }),
            (false, true) => ReconcileState::Ignore(app),
        })
    }

    async fn construct(&self, mut ctx: Self::Construct) -> Result<Self::Output, ReconcileError> {
        // ensure we have a finalizer

        if ctx.app.metadata.ensure_finalizer(FINALIZER) {
            // early return
            return Ok(ctx.app);
        }

        let ready = self.ensure_kafka_topic(&mut ctx.app).await?;

        // extract ready status

        let status = core::v1::ConditionStatus {
            status: Some(ready),
            ..Default::default()
        };

        // assign this topic to our status too

        ctx.app
            .update_section(|mut conditions: core::v1::Conditions| {
                conditions.update(CONDITION_READY, status);
                conditions
            })?;

        let status = KafkaAppStatus::reconciled(ctx.app.metadata.generation);
        ctx.app.set_section(status)?;

        // done

        Ok(ctx.app)
    }

    async fn deconstruct(
        &self,
        mut ctx: Self::Deconstruct,
    ) -> Result<Self::Output, ReconcileError> {
        // delete

        self.delete_kafka_topic(&mut ctx.app).await?;

        // remove finalizer

        ctx.app.metadata.finalizers.retain(|f| f != FINALIZER);

        // done

        Ok(ctx.app)
    }
}

impl<'a> ApplicationReconciler<'a> {
    fn make_topic_resource_name(app: &registry::v1::Application) -> String {
        format!("events-{}", app.metadata.name)
    }

    async fn ensure_kafka_topic(
        &self,
        app: &mut registry::v1::Application,
    ) -> Result<bool, ReconcileError> {
        let topic_name = Self::make_topic_resource_name(app);

        let topic = create_or_update_by(
            self.kafka_topics,
            Some(""),
            &topic_name,
            |meta| {
                let mut topic = DynamicObject::new(&topic_name, self.kafka_topic_resource)
                    .within(&self.config.topic_namespace);
                *topic.meta_mut() = meta;
                topic
            },
            |this, that| this.metadata == that.metadata && this.data == that.data,
            |mut topic| {
                // set target cluster
                topic
                    .metadata
                    .labels
                    .insert(LABEL_KAFKA_CLUSTER.into(), self.config.cluster_name.clone());
                // set config
                topic.data["spec"] = json!({
                    "config": {},
                    "partitions": 3,
                    "replicas": 1,
                    "topicName": topic_name,
                });

                Ok::<_, ReconcileError>(topic)
            },
        )
        .await?;

        // done

        Ok(Self::is_topic_ready(&topic).unwrap_or(false))
    }

    async fn delete_kafka_topic(
        &self,
        app: &mut registry::v1::Application,
    ) -> Result<(), ReconcileError> {
        let topic_name = Self::make_topic_resource_name(app);

        // remove topic

        self.kafka_topics
            .delete_optionally(&topic_name, &Default::default())
            .await?;

        // done

        Ok(())
    }

    fn is_topic_ready(topic: &DynamicObject) -> Option<bool> {
        topic.data["status"]["conditions"]
            .as_array()
            .and_then(|conditions| {
                conditions
                    .iter()
                    .filter_map(|cond| cond.as_object())
                    .filter_map(|cond| {
                        if cond["type"] == "Ready" {
                            match cond["status"].as_str() {
                                Some("True") => Some(true),
                                Some("False") => Some(false),
                                _ => None,
                            }
                        } else {
                            None
                        }
                    })
                    .next()
            })
    }
}
