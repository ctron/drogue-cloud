pub mod endpoints;
pub mod service;
pub mod utils;

use crate::service::ManagementService;
use drogue_cloud_service_common::openid::Authenticator;
use envconfig::Envconfig;
use url::Url;

#[derive(Debug)]
pub struct WebData<S: ManagementService> {
    pub service: S,
    pub authenticator: Option<Authenticator>,
}

#[derive(Envconfig)]
pub struct Config {
    #[envconfig(from = "BIND_ADDR", default = "127.0.0.1:8080")]
    pub bind_addr: String,
    #[envconfig(from = "HEALTH_BIND_ADDR", default = "127.0.0.1:9090")]
    pub health_bind_addr: String,
    #[envconfig(from = "ENABLE_AUTH", default = "true")]
    pub enable_auth: bool,
    #[envconfig(from = "K_SINK")]
    pub event_url: Url,
}

#[macro_export]
macro_rules! crud {
    ($sender:ty, $scope:ident, $base:literal, $module:path, $name:ident) => {{
        $scope
            .service({
                let resource = concat!($base, stringify!($name), "s");
                log::debug!("{}", resource);
                web::resource(resource).route(web::post().to({
                    use $module as m;
                    m::create::<$sender>
                }))
            })
            .service({
                let resource = concat!($base, stringify!($name), "s/{", stringify!($name), "}");
                log::debug!("{}", resource);

                web::resource(resource)
                    .name(stringify!($name))
                    // "use" is required due to: https://github.com/rust-lang/rust/issues/48067
                    .route(web::get().to({
                        use $module as m;
                        m::read::<$sender>
                    }))
                    .route(web::put().to({
                        use $module as m;
                        m::update::<$sender>
                    }))
                    .route(web::delete().to({
                        use $module as m;
                        m::delete::<$sender>
                    }))
            })
    }};
}

#[macro_export]
macro_rules! app {
    ($sender:ty, $data:expr, $enable_auth:expr, $max_json_payload_size:expr, $auth:expr) => {{
        let app = App::new()
            .wrap(actix_web::middleware::Logger::default())
            .data(web::JsonConfig::default().limit($max_json_payload_size))
            // FIXME: bind to a different port
            .service(
                web::resource("/health").route(web::get().to(endpoints::health::health::<$sender>)),
            )
            .app_data($data.clone());

        let app = {
            let scope = web::scope("/api/v1")
                .wrap(Cors::permissive())
                .wrap(Condition::new($enable_auth, $auth));

            let scope = drogue_cloud_device_management_service::crud!(
                $sender,
                scope,
                "/",
                endpoints::apps,
                app
            );

            let scope = drogue_cloud_device_management_service::crud!(
                $sender,
                scope,
                "/apps/{app}/",
                endpoints::devices,
                device
            );

            app.service(scope)
        };

        app
    }};
}
