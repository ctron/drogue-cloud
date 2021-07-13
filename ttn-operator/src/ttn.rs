use drogue_cloud_operator_common::controller::reconciler::ReconcileError;
use lazy_static::lazy_static;
use reqwest::{RequestBuilder, Response, StatusCode};
use serde::de::MapAccess;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fmt::Formatter;
use url::{PathSegmentsMut, Url};

// NOTE: We need to amend this with every field we add
const FIELD_MASK_WEBHOOK: &[&str] = &[
    "base_url",
    "downlink_ack",
    "downlink_api_key",
    "downlink_failed",
    "downlink_nack",
    "downlink_queue_invalidated",
    "downlink_queued",
    "downlink_sent",
    "format",
    "headers",
    "ids",
    "ids.application_ids",
    "ids.application_ids.application_id",
    "ids.webhook_id",
    "join_accept",
    "location_solved",
    "service_data",
    "uplink_message",
    "uplink_message.path",
];

const FIELD_MASK_DEVICE_CORE: &[&str] = &[
    "ids.dev_eui",
    "ids.join_eui",
    "name",
    "description",
    "attributes",
    "join_server_address",
    "network_server_address",
    "application_server_address",
];
const FIELD_MASK_DEVICE_JS: &[&str] = &[
    "network_server_address",
    "application_server_address",
    "ids.device_id",
    "ids.dev_eui",
    "ids.join_eui",
    "network_server_kek_label",
    "application_server_kek_label",
    "application_server_id",
    "net_id",
    "root_keys.app_key.key",
];
const FIELD_MASK_DEVICE_AS: &[&str] = &["ids.device_id", "ids.dev_eui", "ids.join_eui"];
const FIELD_MASK_DEVICE_NS: &[&str] = &[
    "multicast",
    "supports_join",
    "lorawan_version",
    "ids.device_id",
    "ids.dev_eui",
    "ids.join_eui",
    "mac_settings.supports_32_bit_f_cnt",
    "supports_class_c",
    "supports_class_b",
    "lorawan_phy_version",
    "frequency_plan_id",
];

lazy_static! {
    static ref FIELD_MASK_WEBHOOK_STRING: String = FIELD_MASK_WEBHOOK.join(",");
    static ref FIELD_MASK_WEBHOOK_STR: &'static str = &FIELD_MASK_WEBHOOK_STRING;
}

const FIELD_MASK_APP_STR: &str = "name,attributes";

#[derive(Clone, Debug)]
pub struct Device {
    pub ids: DeviceIds,
    pub end_device: EndDevice,
    pub ns_device: NsDevice,
    pub js_device: JsDevice,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeviceIds {
    pub device_id: String,
    pub dev_eui: String,
    pub join_eui: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EndDevice {
    pub name: String,
    pub network_server_address: String,
    pub application_server_address: String,
    pub join_server_address: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NsDevice {
    pub multicast: bool,
    pub supports_join: bool,
    pub lorawan_version: String,
    pub lorawan_phy_version: String,

    pub mac_settings: HashMap<String, Value>,
    pub supports_class_b: bool,
    pub supports_class_c: bool,
    pub frequency_plan_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsDevice {
    pub network_server_address: String,
    pub application_server_address: String,
    pub join_server_address: String,

    pub network_server_kek_label: String,
    pub application_server_kek_label: String,
    pub application_server_id: String,
    pub net_id: Value,
    pub root_keys: RootKeys,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RootKeys {
    pub app_key: Key,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Key {
    pub key: String,
}

pub struct Client {
    client: reqwest::Client,
}

#[derive(Clone, Debug)]
pub struct Context {
    pub api_key: String,
    pub url: Url,
}

impl Context {
    pub fn inject_token(&self, req: RequestBuilder) -> RequestBuilder {
        req.bearer_auth(&self.api_key)
    }
}

trait TokenInjector {
    fn inject_token(self, ctx: &Context) -> Self;
}

impl TokenInjector for RequestBuilder {
    fn inject_token(self, ctx: &Context) -> Self {
        ctx.inject_token(self)
    }
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub enum Owner {
    #[serde(rename = "user")]
    User(String),
    #[serde(rename = "org")]
    Organization(String),
}

impl<'de> Deserialize<'de> for Owner {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(OwnerVisitor)
    }
}

struct OwnerVisitor;

impl<'de> serde::de::Visitor<'de> for OwnerVisitor {
    type Value = Owner;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("An owner, by string or sub-object ('user' or 'org')")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(Owner::User(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(Owner::User(value))
    }

    fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
    where
        V: MapAccess<'de>,
    {
        if let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "user" => Ok(Owner::User(map.next_value()?)),
                "org" => Ok(Owner::Organization(map.next_value()?)),
                key => Err(serde::de::Error::unknown_field(key, &["user", "org"])),
            }
        } else {
            Err(serde::de::Error::invalid_length(
                0,
                &"Expected exactly one field",
            ))
        }
    }
}

impl Owner {
    pub fn extend(&self, path: &mut PathSegmentsMut) {
        match self {
            Self::User(user) => path.extend(&["users", &user]),
            Self::Organization(org) => path.extend(&["organizations", &org]),
        };
    }
}

impl Client {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    fn url<F>(ctx: &Context, f: F) -> Result<Url, ReconcileError>
    where
        F: FnOnce(&mut PathSegmentsMut),
    {
        let mut url = ctx.url.clone();
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|_| ReconcileError::permanent("Failed to modify path"))?;
            f(&mut path);
        }
        Ok(url)
    }

    pub async fn create_app(
        &self,
        app_id: &str,
        ttn_app_id: &str,
        owner: Owner,
        ctx: &Context,
    ) -> Result<(), ReconcileError> {
        let url = Self::url(&ctx, |path| {
            path.extend(&["api", "v3"]);
            owner.extend(path);
            path.extend(&["applications"]);
        })?;

        let create = json!({
            "application": {
                "ids": {
                    "application_id": ttn_app_id,
                },
                "name": app_id,
                "attributes": {
                    "drogue-app": app_id,
                }
            },
        });

        log::debug!("New app: {}", create);

        let res = self
            .client
            .post(url)
            .inject_token(&ctx)
            .json(&create)
            .send()
            .await?;

        if res.status().is_success() {
            let payload = res.text().await?;
            log::debug!("Response: {:#?}", payload);

            Ok(())
        } else {
            match res.status() {
                StatusCode::CONFLICT => Err(ReconcileError::permanent(format!(
                    "An application with the name '{}' already exists in the TTN system. This may be another user claiming that name, or a previously created application which is still pending final deletion. You can still choose to use a different name.",
                    ttn_app_id
                ))),
                _ => Self::default_error(res.status(), res).await,
            }
        }
    }

    pub async fn update_app(
        &self,
        app_id: &str,
        ttn_app_id: &str,
        ctx: &Context,
    ) -> Result<(), ReconcileError> {
        let url = Self::url(&ctx, |path| {
            path.extend(&["api", "v3", "applications", ttn_app_id]);
        })?;

        let update = json!({
            "application": {
                "name": app_id,
                "attributes": {
                    "drogue-app": app_id,
                }
            },
            "field_mask": {
                "paths": ["name", "attributes"]
            }
        });

        log::debug!("New app: {}", update);

        let res = self
            .client
            .put(url)
            .inject_token(&ctx)
            .json(&update)
            .send()
            .await?;

        if res.status().is_success() {
            let payload = res.text().await?;
            log::debug!("Response: {:#?}", payload);

            Ok(())
        } else {
            Self::default_error(res.status(), res).await
        }
    }

    async fn default_error<T>(code: StatusCode, res: Response) -> Result<T, ReconcileError> {
        let info = res.text().await.unwrap_or_default();

        match code {
            StatusCode::NOT_IMPLEMENTED => {
                return Err(ReconcileError::Permanent(format!(
                    "Implementation error: {}: {}",
                    code, info
                )))
            }
            code if code.is_server_error() => Err(ReconcileError::Temporary(format!(
                "Request failed: {}: {}",
                code, info
            ))),
            code => Err(ReconcileError::Permanent(format!(
                "Request failed: {}: {}",
                code, info
            ))),
        }
    }

    pub async fn get_app(
        &self,
        app_id: &str,
        ctx: &Context,
    ) -> Result<Option<Value>, ReconcileError> {
        let mut url = Self::url(&ctx, |path| {
            path.extend(&["api", "v3", "applications", app_id]);
        })?;

        url.query_pairs_mut()
            .append_pair("field_mask", FIELD_MASK_APP_STR);

        let res = self.client.get(url).inject_token(&ctx).send().await?;

        match res.status() {
            StatusCode::OK => Ok(Some(res.json().await?)),
            StatusCode::FORBIDDEN => Ok(None),
            StatusCode::NOT_FOUND => Ok(None),
            code => Self::default_error(code, res).await,
        }
    }

    pub async fn delete_app(&self, app_id: &str, ctx: &Context) -> Result<(), ReconcileError> {
        let url = Self::url(&ctx, |path| {
            path.extend(&["api", "v3", "applications", app_id]);
        })?;

        let res = self.client.delete(url).inject_token(&ctx).send().await?;

        match res.status() {
            StatusCode::OK | StatusCode::NOT_FOUND => Ok(()),
            code => Self::default_error(code, res).await,
        }
    }

    pub async fn get_webhook(
        &self,
        app_id: &str,
        webhook_id: &str,
        ctx: &Context,
    ) -> Result<Option<Value>, ReconcileError> {
        let mut url = Self::url(&ctx, |path| {
            path.extend(&["api", "v3", "as", "webhooks", app_id, webhook_id]);
        })?;

        url.query_pairs_mut()
            .append_pair("field_mask", &FIELD_MASK_WEBHOOK_STR);

        let res = self.client.get(url).inject_token(&ctx).send().await?;

        match res.status() {
            StatusCode::OK => Ok(Some(res.json().await?)),
            StatusCode::NOT_FOUND => Ok(None),
            code => Self::default_error(code, res).await,
        }
    }

    pub async fn create_webhook(
        &self,
        app_id: &str,
        webhook_id: &str,
        endpoint_url: &Url,
        auth: &str,
        ctx: &Context,
    ) -> Result<(), ReconcileError> {
        let create = json!({
            "field_mask": {
                "paths": FIELD_MASK_WEBHOOK,
            },
            "webhook": {
                "ids": {
                    "webhook_id": webhook_id,
                },
                "base_url": endpoint_url,
                "format": "json",
                "uplink_message": {},
                "headers": {
                    "Authorization": auth,
                }
            },
        });

        let url = Self::url(&ctx, |path| {
            path.extend(&["api", "v3", "as", "webhooks", app_id]);
        })?;

        log::debug!("New webhook: {}", create);

        let res = self
            .client
            .post(url)
            .inject_token(&ctx)
            .json(&create)
            .send()
            .await?;

        if res.status().is_success() {
            let payload = res.text().await?;
            log::debug!("Response: {:#?}", payload);
        } else {
            Self::default_error(res.status(), res).await?;
        }

        Ok(())
    }

    pub async fn update_webhook(
        &self,
        app_id: &str,
        webhook_id: &str,
        endpoint_url: &Url,
        auth: &str,
        ctx: &Context,
    ) -> Result<(), ReconcileError> {
        let update = json!({
            "field_mask": {
                "paths": FIELD_MASK_WEBHOOK,
            },
            "webhook": {
                "base_url": endpoint_url,
                "format": "json",
                "uplink_message": {},
                "headers": {
                    "Authorization": auth,
                }
            },
        });

        let url = Self::url(&ctx, |path| {
            path.extend(&["api", "v3", "as", "webhooks", app_id, webhook_id]);
        })?;

        log::debug!("New webhook: {}", update);

        let res = self
            .client
            .put(url)
            .inject_token(&ctx)
            .json(&update)
            .send()
            .await?;

        if res.status().is_success() {
            let payload = res.text().await?;
            log::debug!("Response: {:#?}", payload);
        } else {
            Self::default_error(res.status(), res).await?;
        }

        Ok(())
    }

    pub async fn get_device(
        &self,
        app_id: &str,
        device_id: &str,
        ctx: &Context,
    ) -> Result<Option<Value>, ReconcileError> {
        let url = Self::url(&ctx, |path| {
            path.extend(&["api", "v3", "applications", app_id, "devices", device_id]);
        })?;

        let res = self.client.get(url).inject_token(&ctx).send().await?;

        match res.status() {
            StatusCode::OK => Ok(Some(res.json().await?)),
            StatusCode::NOT_FOUND => Ok(None),
            code => Self::default_error(code, res).await,
        }
    }

    async fn delete_device_json(
        &self,
        prefix: Option<&str>,
        app_id: &str,
        device_id: &str,
        ctx: &Context,
    ) -> Result<(), ReconcileError> {
        let url = Self::url(&ctx, |path| {
            path.extend(&["api", "v3"]);
            if let Some(prefix) = prefix {
                path.push(prefix);
            }
            path.extend(&["applications", app_id, "devices", device_id]);
        })?;

        let res = self.client.delete(url).inject_token(&ctx).send().await?;

        match res.status() {
            StatusCode::OK | StatusCode::NOT_FOUND => Ok(()),
            code => Self::default_error(code, res).await,
        }
    }

    pub async fn delete_device(
        &self,
        app_id: &str,
        device_id: &str,
        ctx: &Context,
    ) -> Result<(), ReconcileError> {
        self.delete_device_json(Some("ns"), app_id, device_id, ctx)
            .await?;
        self.delete_device_json(Some("js"), app_id, device_id, ctx)
            .await?;
        self.delete_device_json(Some("as"), app_id, device_id, ctx)
            .await?;
        self.delete_device_json(None, app_id, device_id, ctx)
            .await?;

        Ok(())
    }

    fn make_device_json<T: Serialize>(
        ids: &DeviceIds,
        v: &T,
        paths: &[&str],
    ) -> Result<Value, ReconcileError> {
        let mut json = json!({
            "end_device": serde_json::to_value(&v)?,
            "field_mask": {
                "paths": paths,
            }
        });

        json["end_device"]["ids"] = serde_json::to_value(&ids)?;

        Ok(json)
    }

    async fn put_device_json<T: Serialize>(
        &self,
        prefix: &str,
        app_id: &str,
        ids: &DeviceIds,
        payload: &T,
        paths: &[&str],
        ctx: &Context,
    ) -> Result<(), ReconcileError> {
        let url = Self::url(&ctx, |path| {
            path.extend(&[
                "api",
                "v3",
                prefix,
                "applications",
                app_id,
                "devices",
                &ids.device_id,
            ]);
        })?;

        let res = self
            .client
            .put(url)
            .inject_token(&ctx)
            .json(&Self::make_device_json(&ids, &payload, paths)?)
            .send()
            .await?;

        match res.status() {
            StatusCode::OK => Ok(()),
            code => Self::default_error(code, res).await?,
        }
    }

    async fn set_ns_js_as(
        &self,
        app_id: &str,
        device: &Device,
        ctx: &Context,
    ) -> Result<(), ReconcileError> {
        // NS

        self.put_device_json(
            "ns",
            app_id,
            &device.ids,
            &device.ns_device,
            FIELD_MASK_DEVICE_NS,
            &ctx,
        )
        .await?;

        // JS

        self.put_device_json(
            "js",
            app_id,
            &device.ids,
            &device.js_device,
            FIELD_MASK_DEVICE_JS,
            &ctx,
        )
        .await?;

        // AS

        self.put_device_json(
            "as",
            app_id,
            &device.ids,
            &json!({}),
            FIELD_MASK_DEVICE_AS,
            &ctx,
        )
        .await?;

        // done

        Ok(())
    }

    pub async fn create_device(
        &self,
        app_id: &str,
        device: Device,
        ctx: &Context,
    ) -> Result<(), ReconcileError> {
        log::debug!("Creating new device in TTN");

        // core

        let url = Self::url(&ctx, |path| {
            path.extend(&["api", "v3", "applications", app_id, "devices"]);
        })?;

        let res = self
            .client
            .post(url)
            .inject_token(&ctx)
            .json(&Self::make_device_json(
                &device.ids,
                &device.end_device,
                FIELD_MASK_DEVICE_CORE,
            )?)
            .send()
            .await?;

        match res.status() {
            StatusCode::OK => {}
            code => Self::default_error(code, res).await?,
        }

        // set NS, JS, AS entries as well

        self.set_ns_js_as(app_id, &device, &ctx).await?;

        // done

        Ok(())
    }

    pub async fn update_device(
        &self,
        app_id: &str,
        device: Device,
        ctx: &Context,
    ) -> Result<(), ReconcileError> {
        log::debug!("Creating new device in TTN");

        // core

        let url = Self::url(&ctx, |path| {
            path.extend(&[
                "api",
                "v3",
                "applications",
                app_id,
                "devices",
                &device.ids.device_id,
            ]);
        })?;

        let res = self
            .client
            .put(url)
            .inject_token(&ctx)
            .json(&Self::make_device_json(
                &device.ids,
                &device.end_device,
                FIELD_MASK_DEVICE_CORE,
            )?)
            .send()
            .await?;

        match res.status() {
            StatusCode::OK => {}
            code => Self::default_error(code, res).await?,
        }

        // set NS, JS, AS entries as well

        self.set_ns_js_as(app_id, &device, &ctx).await?;

        // Done

        Ok(())
    }
}
