use actix_web::HttpResponse;

use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool, PooledConnection};

use serde_json::Value;

use crate::models::Credential;
use crate::schema;

pub type PgPool = Pool<ConnectionManager<PgConnection>>;
pub type PgPooledConnection = PooledConnection<ConnectionManager<PgConnection>>;

pub fn establish_connection(database_url: String) -> PgPool {
    let manager = ConnectionManager::<PgConnection>::new(database_url);
    Pool::builder()
        .build(manager)
        .expect("Failed to create pool.")
}

pub fn pg_pool_handler(pool: &PgPool) -> Result<PgPooledConnection, HttpResponse> {
    pool.get()
        .map_err(|e| HttpResponse::InternalServerError().json(e.to_string()))
}

pub fn get_credential(id: &str, pool: &PgConnection) -> Result<Credential, HttpResponse> {
    use schema::credentials::dsl::*;

    let results = credentials
        .filter(device_id.eq(id))
        .load::<Credential>(pool)
        .expect("Error loading credentials");

    control_credentials(results, id)
}

pub fn serialise_props(props: Option<Value>) -> String {
    match props {
        Some(p) => p.as_str().unwrap_or("{}").to_string(),
        None => "{}".to_string(),
    }
}

pub fn insert_credential(
    data: Credential,
    pool: &PgConnection,
) -> Result<Credential, HttpResponse> {
    use schema::credentials::dsl::*;

    let res = diesel::insert_into(credentials)
        .values(data)
        .get_result(pool);

    res.map_err(|e| HttpResponse::InternalServerError().json(e.to_string()))
}

pub fn delete_credential(id: String, pool: &PgConnection) -> Result<usize, HttpResponse> {
    use schema::credentials::dsl::*;

    let res = diesel::delete(credentials.filter(device_id.eq(id))).execute(pool);

    res.map_err(|e| HttpResponse::InternalServerError().json(e.to_string()))
}

fn control_credentials(creds: Vec<Credential>, id: &str) -> Result<Credential, HttpResponse> {
    if creds.len() > 1 {
        log::info!("More than one credential exist for {}", id);
        Err(HttpResponse::InternalServerError().finish())
    } else if creds.len() == 1 {
        Ok(creds[0].clone())
    } else if creds.is_empty() {
        log::info!("No credentials found for {}", id);
        Err(HttpResponse::NotFound().finish())
    } else {
        Err(HttpResponse::InternalServerError().finish())
    }
}
