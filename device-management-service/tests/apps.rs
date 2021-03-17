mod common;

use crate::common::{assert_events, call_http, init, outbox_retrieve, user};
use actix_cors::Cors;
use actix_web::{http::StatusCode, middleware::Condition, test, web, App};
use drogue_cloud_device_management_service::{
    app, endpoints,
    service::{self},
    WebData,
};
use drogue_cloud_registry_events::{mock::MockEventSender, Event};
use drogue_cloud_test_common::{client, db};
use http::{header, HeaderValue};
use serde_json::json;
use serial_test::serial;

#[actix_rt::test]
#[serial]
async fn test_create_app() -> anyhow::Result<()> {
    test!((app, sender, outbox) => {
        let resp = call_http(&app, user("foo"), test::TestRequest::post().uri("/api/v1/apps").set_json(&json!({
            "metadata": {
                "name": "app1",
            },
        }))).await;

        assert_eq!(resp.status(), StatusCode::CREATED);
        assert_eq!(resp.headers().get(header::LOCATION), Some(&HeaderValue::from_static("http://localhost:8080/api/v1/apps/app1")));

        // an event must have been fired
        assert_events(vec![sender.retrieve()?, outbox_retrieve(&outbox).await?], vec![Event::Application {
            instance: "drogue-instance".into(),
            application: "app1".into(),
            uid: "".into(),
            path: ".".into(),
            generation: 0,
        }]);
    })
}

/// Create, update, and delete an app. Check the current state and the operation outcomes.
#[actix_rt::test]
#[serial]
async fn test_crud_app() -> anyhow::Result<()> {
    test!((app, sender, outbox) => {

        // try read, must not exist
        let resp = test::TestRequest::get().uri("/api/v1/apps/app1").send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // create, must succeed, event sent
        let resp = test::TestRequest::post().uri("/api/v1/apps").set_json(&json!({
            "metadata": {
                "name": "app1",
            },
        })).send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::CREATED);

        assert_events(vec![sender.retrieve()?, outbox_retrieve(&outbox).await?], vec![Event::Application {
            instance: "drogue-instance".into(),
            application: "app1".into(),
            uid: "".into(),
            path: ".".into(),
            generation: 0,
        }]);

        // read, must exist
        let resp = test::TestRequest::get().uri("/api/v1/apps/app1").send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let result: serde_json::Value = test::read_body_json(resp).await;

        let creation_timestamp = result["metadata"]["creationTimestamp"].clone();
        let resource_version = result["metadata"]["resourceVersion"].clone();
        let generation = result["metadata"]["generation"].clone();
        let uid = result["metadata"]["uid"].clone();

        assert_eq!(result, json!({
            "metadata": {
                "name": "app1",
                "uid": uid,
                "creationTimestamp": creation_timestamp,
                "generation": generation,
                "resourceVersion": resource_version,
            }
        }));

        // update, must succeed
        let resp = test::TestRequest::put().uri("/api/v1/apps/app1").set_json(&json!({
            "metadata": {
                "name": "app1"
            },
            "spec": {
                "core": {
                    "disabled": true,
                }
            }
        })).send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // event must have been fired
        assert_events(vec![sender.retrieve()?, outbox_retrieve(&outbox).await?], vec![Event::Application {
            instance: "drogue-instance".into(),
            application: "app1".into(),
            uid: "".into(),
            path: ".spec.core".into(),
            generation: 0,
        }]);

        // read, must exist
        let resp = test::TestRequest::get().uri("/api/v1/apps/app1").send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let result: serde_json::Value = test::read_body_json(resp).await;

        let creation_timestamp = result["metadata"]["creationTimestamp"].clone();
        let new_resource_version = result["metadata"]["resourceVersion"].clone();
        let new_generation = result["metadata"]["generation"].clone();

        assert_ne!(resource_version, new_resource_version);
        assert_ne!(generation, new_generation);

        assert_eq!(result, json!({
            "metadata": {
                "name": "app1",
                "uid": uid,
                "creationTimestamp": creation_timestamp,
                "generation": new_generation,
                "resourceVersion": new_resource_version,
            },
            "spec": {
                "core": {
                    "disabled": true
                },
            },
        }));

        // delete, must succeed
        let resp = test::TestRequest::delete().uri("/api/v1/apps/app1").send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // an event must have been fired
        assert_events(vec![sender.retrieve()?, outbox_retrieve(&outbox).await?], vec![Event::Application {
            instance: "drogue-instance".into(),
            application: "app1".into(),
            uid: "".into(),
            path: ".".into(),
            generation: 0,
        }]);

        // try read, must not exist
        let resp = test::TestRequest::get().uri("/api/v1/apps/app1").send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // second delete, must report "not found"
        let resp = test::TestRequest::delete().uri("/api/v1/apps/app1").send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // no additional event must be fired
        assert_eq!(sender.retrieve().unwrap(), vec![]);
    })
}

/// Create, update, and delete an app. Check the current state and the operation outcomes.
#[actix_rt::test]
#[serial]
async fn test_app_labels() -> anyhow::Result<()> {
    test!((app, sender, outbox) => {

        // try read, must not exist
        let resp = test::TestRequest::get().uri("/api/v1/apps/app1").send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // create, must succeed
        let resp = test::TestRequest::post().uri("/api/v1/apps").set_json(&json!({
            "metadata": {
                "name": "app1",
                "labels": {
                    "foo": "bar",
                    "foo/bar": "baz",
                },
            },
        })).send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::CREATED);

        // an event must have been fired
        assert_events(vec![sender.retrieve()?, outbox_retrieve(&outbox).await?], vec![Event::Application {
            instance: "drogue-instance".into(),
            application: "app1".into(),
            uid: "".into(),
            path: ".".into(),
            generation: 0,
        }]);

        // read, must exist
        let resp = test::TestRequest::get().uri("/api/v1/apps/app1").send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let result: serde_json::Value = test::read_body_json(resp).await;

        let creation_timestamp = result["metadata"]["creationTimestamp"].clone();
        let resource_version = result["metadata"]["resourceVersion"].clone();
        let generation = result["metadata"]["generation"].clone();
        let uid = result["metadata"]["uid"].clone();

        assert_eq!(result, json!({
            "metadata": {
                "name": "app1",
                "uid": uid,
                "creationTimestamp": creation_timestamp,
                "generation": generation,
                "resourceVersion": resource_version,
                "labels": {
                    "foo": "bar",
                    "foo/bar": "baz",
                }
            }
        }));

        // update, must succeed
        let resp = test::TestRequest::put().uri("/api/v1/apps/app1").set_json(&json!({
            "metadata": {
                "name": "app1",
                "labels": {
                    "foo": "bar",
                    "baz/bar": "foo",
                }
            },
            "spec": {
                "core": {
                    "disabled": true,
                }
            }
        })).send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // an event must have been fired
        assert_events(vec![sender.retrieve()?, outbox_retrieve(&outbox).await?], vec![
            Event::Application {
                instance: "drogue-instance".into(),
                application: "app1".into(),
                uid: "".into(),
                path: ".metadata".into(),
                generation: 0,
            },
            Event::Application {
                instance: "drogue-instance".into(),
                application: "app1".into(),
                uid: "".into(),
                path: ".spec.core".into(),
                generation: 0,
            }
        ]);

        // read, must exist
        let resp = test::TestRequest::get().uri("/api/v1/apps/app1").send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let result: serde_json::Value = test::read_body_json(resp).await;

        let creation_timestamp = result["metadata"]["creationTimestamp"].clone();
        let new_resource_version = result["metadata"]["resourceVersion"].clone();
        let new_generation = result["metadata"]["generation"].clone();

        // resource version must change
        assert_ne!(resource_version, new_resource_version);
        // generation must change
        assert_ne!(generation, new_generation);

        assert_eq!(result, json!({
            "metadata": {
                "name": "app1",
                "uid": uid,
                "creationTimestamp": creation_timestamp,
                "generation": new_generation,
                "resourceVersion": new_resource_version,
                "labels": {
                    "foo": "bar",
                    "baz/bar": "foo",
                }
            },
            "spec": {
                "core": {
                    "disabled": true
                },
            },
        }));

    })
}

#[actix_rt::test]
#[serial]
async fn test_create_duplicate_app() -> anyhow::Result<()> {
    test!((app, sender, outbox) => {
        let resp = test::TestRequest::post().uri("/api/v1/apps").set_json(&json!({
            "metadata": {
                "name": "app1",
            },
        })).send_request(&app).await;

        assert_eq!(resp.status(), StatusCode::CREATED);

        // an event must have been fired
        assert_events(vec![sender.retrieve()?, outbox_retrieve(&outbox).await?], vec![Event::Application {
            instance: "drogue-instance".into(),
            application: "app1".into(),
            uid: "".into(),
            path: ".".into(),
            generation: 0,
        }]);


        let resp = test::TestRequest::post().uri("/api/v1/apps").set_json(&json!({
            "metadata": {
                "name": "app1",
            },
        })).send_request(&app).await;

        assert_eq!(resp.status(), StatusCode::CONFLICT);

        // no event for the failed attempt
        assert_eq!(sender.retrieve().unwrap(), vec![]);
    })
}

#[actix_rt::test]
#[serial]
async fn test_app_trust_anchor() -> anyhow::Result<()> {
    let ca = include_bytes!("certs/ca-cert.pem").to_vec();
    let ca = base64::encode(ca);

    test!((app, sender, outbox) => {
        let resp = test::TestRequest::post().uri("/api/v1/apps").set_json(&json!({
            "metadata": {
                "name": "app1",
            },
            "spec": {
                "trustAnchors": {
                    "anchors": [
                        { "certificate": ca, }
                    ],
                }
            }
        })).send_request(&app).await;

        assert_eq!(resp.status(), StatusCode::CREATED);

        // an event must have been fired
        assert_events(vec![sender.retrieve()?, outbox_retrieve(&outbox).await?], vec![Event::Application {
            instance: "drogue-instance".into(),
            application: "app1".into(),
            uid: "".into(),
            path: ".".into(),
            generation: 0,
        }]);

        // read, must exist, with cert
        let resp = test::TestRequest::get().uri("/api/v1/apps/app1").send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let result: serde_json::Value = test::read_body_json(resp).await;

        let creation_timestamp = result["metadata"]["creationTimestamp"].clone();
        let resource_version = result["metadata"]["resourceVersion"].clone();
        let generation = result["metadata"]["generation"].clone();
        let uid = result["metadata"]["uid"].clone();

        assert_eq!(result, json!({
            "metadata": {
                "name": "app1",
                "uid": uid,
                "creationTimestamp": creation_timestamp,
                "generation": generation,
                "resourceVersion": resource_version,
            },
            "spec": {
                "trustAnchors": {
                    "anchors": [
                        { "certificate": ca }
                    ]
                }
            },
            "status": {
                "trustAnchors": {
                    "anchors": [ {
                        "valid": {
                            "subject": "O=Drogue IoT, OU=Cloud, CN=Application 1",
                            "notBefore": "2021-02-02T11:11:31Z",
                            "notAfter": "2031-01-31T11:11:31Z",
                            "certificate": ca,
                        }
                    }]
                }
            }
        }));

        // drop cert

        let resp = test::TestRequest::put().uri("/api/v1/apps/app1").set_json(&json!({
            "metadata": {
                "name": "app1",
            },
        })).send_request(&app).await;

        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // two events must have been fired
        assert_events(vec![sender.retrieve()?, outbox_retrieve(&outbox).await?], vec![Event::Application {
            instance: "drogue-instance".into(),
            application: "app1".into(),
            uid: "".into(),
            path: ".spec.trustAnchors".into(),
            generation: 0,
        }, Event::Application {
            instance: "drogue-instance".into(),
            application: "app1".into(),
            uid: "".into(),
            path: ".status.trustAnchors".into(),
            generation: 0,
        }]);

        // read, must exist, but no cert
        let resp = test::TestRequest::get().uri("/api/v1/apps/app1").send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let result: serde_json::Value = test::read_body_json(resp).await;

        let creation_timestamp = result["metadata"]["creationTimestamp"].clone();
        let new_resource_version = result["metadata"]["resourceVersion"].clone();
        let new_generation = result["metadata"]["generation"].clone();

        // resource version must change
        assert_ne!(resource_version, new_resource_version);
        // generation must change
        assert_ne!(generation, new_generation);

        assert_eq!(result, json!({
            "metadata": {
                "name": "app1",
                "uid": uid,
                "creationTimestamp": creation_timestamp,
                "generation": new_generation,
                "resourceVersion": new_resource_version,
            }
        }));
    })
}

#[actix_rt::test]
#[serial]
async fn test_delete_finalizer() -> anyhow::Result<()> {
    test!((app, sender, outbox) => {
        let resp = test::TestRequest::post().uri("/api/v1/apps").set_json(&json!({
            "metadata": {
                "name": "app1",
                "finalizers": ["foo", "bar"],
            },
        })).send_request(&app).await;

        assert_eq!(resp.status(), StatusCode::CREATED);

        // an event must have been fired
        assert_events(vec![sender.retrieve()?, outbox_retrieve(&outbox).await?], vec![Event::Application {
            instance: "drogue-instance".into(),
            application: "app1".into(),
            uid: "".into(),
            path: ".".into(),
            generation: 0,
        }]);

        let resp = test::TestRequest::delete().uri("/api/v1/apps/app1").send_request(&app).await;

        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // an event must have been fired
        assert_events(vec![sender.retrieve()?, outbox_retrieve(&outbox).await?], vec![Event::Application {
            instance: "drogue-instance".into(),
            application: "app1".into(),
            uid: "".into(),
            path: ".metadata".into(),
            generation: 0,
        }]);

        // read, must exist
        let resp = test::TestRequest::get().uri("/api/v1/apps/app1").send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let mut result: serde_json::Value = test::read_body_json(resp).await;

        let creation_timestamp = result["metadata"]["creationTimestamp"].clone();
        let resource_version = result["metadata"]["resourceVersion"].clone();
        let deletion_timestamp = result["metadata"]["deletionTimestamp"].clone();
        let generation = result["metadata"]["generation"].clone();
        let uid = result["metadata"]["uid"].clone();

        assert_eq!(result, json!({
            "metadata": {
                "name": "app1",
                "uid": uid,
                "creationTimestamp": creation_timestamp,
                "generation": generation,
                "resourceVersion": resource_version,
                "deletionTimestamp": deletion_timestamp,
                "finalizers": ["foo", "bar"],
            }
        }));

        // delete one finalizer
        result["metadata"]["finalizers"] = json!(["bar"]);

        let resp = test::TestRequest::put().uri("/api/v1/apps/app1").set_json(&result).send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // get another metadata event
        assert_events(vec![sender.retrieve()?, outbox_retrieve(&outbox).await?], vec![
            Event::Application {
                instance: "drogue-instance".into(),
                application: "app1".into(),
                uid: "".into(),
                path: ".metadata".into(),
                generation: 0,
            },
        ]);

        // read, must exist (one less finalizer, some deletion timestamp)
        let resp = test::TestRequest::get().uri("/api/v1/apps/app1").send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let mut result: serde_json::Value = test::read_body_json(resp).await;

        let creation_timestamp = result["metadata"]["creationTimestamp"].clone();
        let resource_version = result["metadata"]["resourceVersion"].clone();
        let generation = result["metadata"]["generation"].clone();

        assert_eq!(result, json!({
            "metadata": {
                "name": "app1",
                "uid": uid,
                "creationTimestamp": creation_timestamp,
                "generation": generation,
                "resourceVersion": resource_version,
                "deletionTimestamp": deletion_timestamp,
                "finalizers": ["bar"],
            }
        }));

        // delete last finalizer
        result["metadata"]["finalizers"] = json!([]);

        let resp = test::TestRequest::put().uri("/api/v1/apps/app1").set_json(&result).send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // no more events when cleaning up after finalizers
        assert_eq!(sender.retrieve().unwrap(), vec![]);

    })
}

#[actix_rt::test]
#[serial]
async fn test_delete_precondition() -> anyhow::Result<()> {
    test!((app, _sender, _outbox) => {
        let resp = test::TestRequest::post().uri("/api/v1/apps").set_json(&json!({
            "metadata": {
                "name": "app1",
            },
        })).send_request(&app).await;

        assert_eq!(resp.status(), StatusCode::CREATED);

        // read, must exist ... take uid and resource_version
        let resp = test::TestRequest::get().uri("/api/v1/apps/app1").send_request(&app).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let result: serde_json::Value = test::read_body_json(resp).await;
        let resource_version = result["metadata"]["resourceVersion"].clone();
        let uid = result["metadata"]["uid"].clone();

        // test deleting

        // wrong uid
        let resp = test::TestRequest::delete().uri("/api/v1/apps/app1").set_json(&json!({
            "preconditions": {
                "uid": format!("wrong-{}", uid),
            }
        })).send_request(&app).await;
        // wrong uid, must fail with conflict
        assert_eq!(resp.status(), StatusCode::CONFLICT);

        // wrong resource_version
        let resp = test::TestRequest::delete().uri("/api/v1/apps/app1").set_json(&json!({
            "preconditions": {
                "resourceVersion": format!("wrong-{}", resource_version),
            }
        })).send_request(&app).await;
        // wrong resource_version, must fail with conflict
        assert_eq!(resp.status(), StatusCode::CONFLICT);

        // correct uid and resource version
        let resp = test::TestRequest::delete().uri("/api/v1/apps/app1").set_json(&json!({
            "preconditions": {
                "uid": uid,
                "resourceVersion": resource_version,
            }
        })).send_request(&app).await;
        // all good, must succeed
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    })
}

#[actix_rt::test]
#[serial]
async fn test_auth_app() -> anyhow::Result<()> {
    test!((app, _sender, _outbox) => {
        let resp = call_http(&app, user("foo"), test::TestRequest::post().uri("/api/v1/apps").set_json(&json!({
            "metadata": {
                "name": "app1",
            },
        })))
        .await;

        assert_eq!(resp.status(), StatusCode::CREATED);
        assert_eq!(resp.headers().get(header::LOCATION), Some(&HeaderValue::from_static("http://localhost:8080/api/v1/apps/app1")));

        // get as user "foo"

        let resp = call_http(&app, user("foo"), test::TestRequest::get().uri("/api/v1/apps/app1")).await;

        // must succeed

        assert_eq!(resp.status(), StatusCode::OK);

        // get as user "bar"

        let resp = call_http(&app, user("bar"), test::TestRequest::get().uri("/api/v1/apps/app1")).await;

        // must fail

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // update as user "bar"

        let resp = call_http(&app, user("bar"), test::TestRequest::put().uri("/api/v1/apps/app1").set_json(&json!({
            "metadata": {
                "name": "app1"
            },
            "spec": {
                "core": {
                    "disabled": true,
                }
            }
        }))).await;

        // must fail

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // delete as user "bar"

        let resp = call_http(&app, user("bar"), test::TestRequest::delete().uri("/api/v1/apps/app1")).await;

        // must fail

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // delete as user "foo"

        let resp = call_http(&app, user("foo"), test::TestRequest::delete().uri("/api/v1/apps/app1")).await;

        // must succeed

        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    })
}
