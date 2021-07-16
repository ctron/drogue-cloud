use crate::{
    controller::{ControllerConfig, CONDITION_READY},
    data::*,
};
use async_trait::async_trait;
use drogue_client::{core, meta::v1::CommonMetadataMut, registry, Translator};
use drogue_cloud_operator_common::controller::reconciler::{
    construct::{ConstructOperation, Construction, Constructor, Outcome},
    ReconcileError, ReconcileState, Reconciler, ReconcilerOutcome,
};
use kube::{
    api::{ApiResource, DynamicObject},
    Api, Resource,
};
use lazy_static::lazy_static;
use operator_framework::{install::Delete, process::create_or_update_by};
use regex::Regex;
use serde_json::json;
use std::time::Duration;

const FINALIZER: &str = "kafka";
const LABEL_KAFKA_CLUSTER: &str = "strimzi.io/cluster";
const ANNOTATION_APP_NAME: &str = "drogue.io/application-name";

const REGEXP: &str = r#"^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"#;
lazy_static! {
    static ref TOPIC_PATTERN: Regex = Regex::new(REGEXP).expect("Regexp must compile");
}

pub struct ConstructContext {
    pub app: registry::v1::Application,
    pub status: Option<KafkaAppStatus>,
    pub topic: Option<DynamicObject>,
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
            (_, false) => ReconcileState::Construct(ConstructContext {
                app,
                status,
                topic: None,
            }),
            (true, true) => ReconcileState::Deconstruct(DeconstructContext { app, status }),
            (false, true) => ReconcileState::Ignore(app),
        })
    }

    async fn construct(
        &self,
        ctx: Self::Construct,
    ) -> Result<ReconcilerOutcome<Self::Output>, ReconcileError> {
        let constructor = Constructor::<Self::Construct>::new(vec![
            Box::new(("HasFinalizer", |mut ctx: Self::Construct| async {
                // ensure we have a finalizer
                if ctx.app.metadata.ensure_finalizer(FINALIZER) {
                    // early return
                    Ok(Outcome::Retry(ctx, None))
                } else {
                    Ok(Outcome::Continue(ctx))
                }
            })),
            Box::new(CreateTopic {
                api: self.kafka_topics.clone(),
                resource: self.kafka_topic_resource.clone(),
                config: self.config.clone(),
            }),
            Box::new(("TopicReady", |ctx: Self::Construct| async {
                let ready = ctx
                    .topic
                    .as_ref()
                    .and_then(|topic| Self::is_topic_ready(topic))
                    .unwrap_or_default();

                match ready {
                    true => Ok(Outcome::Continue(ctx)),
                    false => Ok(Outcome::Retry(ctx, Some(Duration::from_secs(15)))),
                }
            })),
        ]);

        let original_app = ctx.app.clone();

        let mut status = ctx.status.as_ref().cloned().unwrap_or_default();
        status.observed_generation = ctx.app.metadata.generation;
        let conditions = status.conditions.clone();

        let result = match constructor.run(conditions, ctx).await {
            Construction::Complete(context, conditions) => {
                status.conditions = conditions;
                status.state = "Ready".to_string();
                status.reason = None;
                let mut app = Self::aggregate_ready(context.app, &status)?;
                app.set_section(status)?;
                ReconcilerOutcome::Complete(app)
            }
            Construction::Retry(context, when, conditions) => {
                status.conditions = conditions;
                status.state = "Processing".to_string();
                status.reason = None;
                let mut app = Self::aggregate_ready(context.app, &status)?;
                app.set_section(status)?;
                ReconcilerOutcome::Retry(app, when)
            }
            Construction::Failed(err, conditions) => {
                status.conditions = conditions;
                status.state = "Failed".to_string();
                status.reason = Some(err.to_string());
                let mut app = Self::aggregate_ready(original_app, &status)?;
                app.set_section(status)?;
                match err {
                    ReconcileError::Permanent(_) => ReconcilerOutcome::Complete(app),
                    ReconcileError::Temporary(_) => ReconcilerOutcome::Retry(app, None),
                }
            }
        };

        // done

        Ok(result)
    }

    async fn deconstruct(
        &self,
        mut ctx: Self::Deconstruct,
    ) -> Result<ReconcilerOutcome<Self::Output>, ReconcileError> {
        // delete

        self.delete_kafka_topic(&mut ctx.app).await?;

        // remove finalizer

        ctx.app.metadata.finalizers.retain(|f| f != FINALIZER);

        // done

        Ok(ReconcilerOutcome::Complete(ctx.app))
    }
}

struct CreateTopic {
    pub api: Api<DynamicObject>,
    pub resource: ApiResource,
    pub config: ControllerConfig,
}

#[async_trait]
impl ConstructOperation<ConstructContext> for CreateTopic {
    fn type_name(&self) -> String {
        "CreateTopic".into()
    }

    async fn run(
        &self,
        mut ctx: ConstructContext,
    ) -> drogue_cloud_operator_common::controller::reconciler::construct::Result<ConstructContext>
    {
        let topic = ApplicationReconciler::ensure_kafka_topic(
            &self.api,
            &self.resource,
            &self.config,
            &mut ctx.app,
        )
        .await?;

        ctx.topic = Some(topic);

        Ok(Outcome::Continue(ctx))
    }
}

const MAX_TOPIC_LEN: usize = 63;

impl<'a> ApplicationReconciler<'a> {
    pub(crate) fn make_topic_resource_name(app: &registry::v1::Application) -> String {
        let name = format!("events-{}", app.metadata.name);

        // try the simple route, if that works ...
        if name.len() < MAX_TOPIC_LEN && TOPIC_PATTERN.is_match(&name) {
            // ... simply return
            return name;
        }

        // otherwise we need to clean up the name, and ensure we don't generate duplicates
        let hash = md5::compute(&app.metadata.name);
        // use a different prefix to prevent clashes with the simple names
        let name = format!("evt-{:x}-{}", hash, &app.metadata.name);

        let name: String = name
            .to_lowercase()
            .chars()
            .map(|c| match c {
                '-' | 'a'..='z' | '0'..='9' => c,
                _ => '-',
            })
            .take(MAX_TOPIC_LEN)
            .collect();

        let name = name.trim_end_matches('-').to_string();

        name
    }

    async fn ensure_kafka_topic(
        kafka_topics: &Api<DynamicObject>,
        kafka_topic_resource: &ApiResource,
        config: &ControllerConfig,
        app: &mut registry::v1::Application,
    ) -> Result<DynamicObject, ReconcileError> {
        let topic_name = Self::make_topic_resource_name(app);

        let topic = create_or_update_by(
            &kafka_topics,
            Some(""),
            &topic_name,
            |meta| {
                let mut topic = DynamicObject::new(&topic_name, &kafka_topic_resource)
                    .within(&config.topic_namespace);
                *topic.meta_mut() = meta;
                topic
            },
            |this, that| this.metadata == that.metadata && this.data == that.data,
            |mut topic| {
                // set target cluster
                topic
                    .metadata
                    .labels
                    .insert(LABEL_KAFKA_CLUSTER.into(), config.cluster_name.clone());
                topic
                    .metadata
                    .annotations
                    .insert(ANNOTATION_APP_NAME.into(), app.metadata.name.clone());
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
        .await?
        .resource();

        // done

        Ok(topic)
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

    fn aggregate_ready(
        mut app: registry::v1::Application,
        status: &KafkaAppStatus,
    ) -> Result<registry::v1::Application, ReconcileError> {
        let mut ready = Some(true);
        for condition in &status.conditions.0 {
            match condition.status.as_str() {
                "True" => {}
                "False" => {
                    ready = Some(false);
                    break;
                }
                _ => {
                    ready = None;
                    break;
                }
            }
        }

        let ready_state = core::v1::ConditionStatus {
            status: ready,
            ..Default::default()
        };

        app.update_section(|mut conditions: core::v1::Conditions| {
            conditions.update(CONDITION_READY, ready_state);
            conditions
        })?;

        Ok(app)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::Utc;
    use drogue_client::meta;

    #[test]
    fn topic_names() {
        for i in [
            ("foo", "events-foo"),
            ("00foo", "events-00foo"),
            (
                "0123456789012345678901234567890123456789012345678901234567890123456789",
                "evt-109eb12c10c45d94ddac8eca7b818bed-01234567890123456789012345",
            ),
            ("FOO", "evt-901890a8e9c8cf6d5a1a542b229febff-foo"),
            ("foo-", "evt-03f19ca8da08c40c2d036c8915d383e2-foo"),
        ] {
            assert_eq!(
                i.1,
                ApplicationReconciler::make_topic_resource_name(&registry::v1::Application {
                    metadata: meta::v1::NonScopedMetadata {
                        name: i.0.to_string(),
                        uid: "".to_string(),
                        creation_timestamp: Utc::now(),
                        generation: 0,
                        resource_version: "".to_string(),
                        deletion_timestamp: None,
                        finalizers: vec![],
                        labels: Default::default(),
                        annotations: Default::default()
                    },
                    spec: Default::default(),
                    status: Default::default()
                })
            )
        }
    }
}
