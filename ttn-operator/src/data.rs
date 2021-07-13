use crate::ttn::{self, Owner};
use drogue_client::{dialect, Dialect, Section};
use drogue_cloud_operator_common::controller::reconciler::ReconcileError;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum RegionOrUrl {
    Url(Url),
    Region(String),
}

impl RegionOrUrl {
    pub fn url(&self) -> Result<Url, url::ParseError> {
        match self {
            Self::Url(url) => Ok(url.clone()),
            Self::Region(region) => {
                Url::parse(&format!("https://{}.cloud.thethings.network", region))
            }
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TtnReconcileStatus {
    pub observed_generation: u64,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl TtnReconcileStatus {
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TtnAppSpec {
    pub api: TtnAppApi,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TtnAppApi {
    pub api_key: String,
    pub region: RegionOrUrl,
    pub owner: Owner,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

impl TtnAppApi {
    pub fn to_context(&self) -> Result<ttn::Context, ReconcileError> {
        Ok(ttn::Context {
            api_key: self.api_key.clone(),
            url: self.region.url().map_err(|err| {
                ReconcileError::Permanent(format!("Failed to parse URL: {}", err))
            })?,
        })
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TtnAppStatus {
    #[serde(flatten)]
    pub reconcile: TtnReconcileStatus,
    pub app_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TtnDeviceSpec {
    pub dev_eui: String,
    pub app_eui: String,
    pub app_key: String,

    pub lorawan_version: Option<String>,
    pub lorawan_phy_version: Option<String>,
    #[serde(default)]
    pub supports_class_b: bool,
    #[serde(default)]
    pub supports_class_c: bool,
    pub frequency_plan_id: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TtnDeviceStatus {
    pub reconcile: TtnReconcileStatus,
}

dialect!(TtnDeviceSpec[Section::Spec => "ttn"]);
dialect!(TtnDeviceStatus[Section::Status => "ttn"]);

dialect!(TtnAppSpec[Section::Spec => "ttn"]);
dialect!(TtnAppStatus[Section::Status => "ttn"]);

#[cfg(test)]
mod test {

    use super::*;
    use crate::ttn::Owner;
    use serde_json::json;

    #[test]
    fn test_region_str() {
        let api: TtnAppApi =
            serde_json::from_value(json!({"apiKey": "foo", "region": "bar", "owner": "me"}))
                .unwrap();

        assert_eq!(api.api_key, "foo");
        assert_eq!(api.region, RegionOrUrl::Region("bar".into()));
    }

    #[test]
    fn test_owner_user() {
        let api: TtnAppApi = serde_json::from_value(json!({
            "apiKey": "foo",
            "region": "bar",
            "owner": {
                "user": "my-user",
            }
        }))
        .unwrap();

        assert_eq!(api.owner, Owner::User("my-user".to_string()));
    }

    #[test]
    fn test_owner_plain_user() {
        let api: TtnAppApi = serde_json::from_value(json!({
            "apiKey": "foo",
            "region": "bar",
            "owner": "me",
        }))
        .unwrap();

        assert_eq!(api.owner, Owner::User("me".to_string()));
    }

    #[test]
    fn test_owner_org() {
        let api: TtnAppApi = serde_json::from_value(json!({
            "apiKey": "foo",
            "region": "bar",
            "owner": {
                "org": "my-org",
            }
        }))
        .unwrap();

        assert_eq!(api.owner, Owner::Organization("my-org".to_string()));
    }
}
