use drogue_client::{dialect, Dialect, Section};
use drogue_cloud_operator_common::controller::reconciler::ReconcileError;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaAppStatus {
    pub observed_generation: u64,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl KafkaAppStatus {
    pub fn failed(generation: u64, err: ReconcileError) -> Self {
        Self {
            observed_generation: generation,
            state: "Failed".into(),
            reason: Some(err.to_string()),
        }
    }

    pub fn reconciled(generation: u64) -> Self {
        Self {
            observed_generation: generation,
            state: "Reconciled".into(),
            reason: None,
        }
    }
}

dialect!(KafkaAppStatus[Section::Status => "kafka"]);
