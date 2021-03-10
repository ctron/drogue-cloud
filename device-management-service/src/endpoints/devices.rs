use crate::endpoints::params::DeleteParams;
use crate::{
    service::{ManagementService, PostgresManagementService},
    WebData,
};
use actix_web::{http::header, web, web::Json, HttpRequest, HttpResponse};
use drogue_cloud_registry_events::EventSender;
use drogue_cloud_service_api::management::Device;

pub async fn create<S>(
    data: web::Data<WebData<PostgresManagementService<S>>>,
    path: web::Path<String>,
    device: Json<Device>,
    req: HttpRequest,
) -> Result<HttpResponse, actix_web::Error>
where
    S: EventSender + Clone,
{
    let app_id = path.into_inner();
    log::debug!("Creating device: '{}' / '{:?}'", app_id, device);

    if device.metadata.name.is_empty() || device.metadata.application.is_empty() {
        return Ok(HttpResponse::BadRequest().finish());
    }

    let location = req.url_for("device", &[&app_id, &device.metadata.name])?;

    data.service.create_device(device.0).await?;

    let response = HttpResponse::Created()
        .append_header((header::LOCATION, location.into_string()))
        .finish();

    Ok(response)
}

pub async fn update<S>(
    data: web::Data<WebData<PostgresManagementService<S>>>,
    path: web::Path<(String, String)>,
    device: Json<Device>,
) -> Result<HttpResponse, actix_web::Error>
where
    S: EventSender + Clone,
{
    let (app_id, device_id) = path.into_inner();

    log::debug!(
        "Updating device: '{}' / '{}' / '{:?}'",
        app_id,
        device_id,
        device
    );

    if app_id.is_empty() || device_id.is_empty() {
        return Ok(HttpResponse::BadRequest().finish());
    }
    if app_id != device.metadata.application || device_id != device.metadata.name {
        return Ok(HttpResponse::BadRequest().finish());
    }

    data.service.update_device(device.0).await?;

    Ok(HttpResponse::NoContent().finish())
}

pub async fn delete<S>(
    data: web::Data<WebData<PostgresManagementService<S>>>,
    path: web::Path<(String, String)>,
    params: Option<web::Json<DeleteParams>>,
) -> Result<HttpResponse, actix_web::Error>
where
    S: EventSender + Clone,
{
    let (app, device) = path.into_inner();

    log::debug!("Deleting device: '{}' / '{}'", app, device);

    if app.is_empty() || device.is_empty() {
        return Ok(HttpResponse::BadRequest().finish());
    }

    data.service
        .delete_device(&app, &device, params.map(|p| p.0).unwrap_or_default())
        .await?;

    Ok(HttpResponse::NoContent().finish())
}

pub async fn read<S>(
    data: web::Data<WebData<PostgresManagementService<S>>>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse, actix_web::Error>
where
    S: EventSender + Clone,
{
    let (app_id, device_id) = path.into_inner();

    log::debug!("Reading device: '{}' / '{}'", app_id, device_id);

    if app_id.is_empty() || device_id.is_empty() {
        return Ok(HttpResponse::BadRequest().finish());
    }

    let device = data.service.get_device(&app_id, &device_id).await?;

    let result = match device {
        None => HttpResponse::NotFound().finish(),
        Some(device) => HttpResponse::Ok().json(device),
    };

    Ok(result)
}
