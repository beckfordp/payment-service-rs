// SPDX-License-Identifier: MIT

//! The routes over a throwaway Postgres, through the whole app (the edge, the
//! errors): methods, payments, authorizations and their replays, the
//! operator's expiry trigger, reconciliations, a store failure, the docs.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use payment_service::domain::{OperatorAuth, SimulatedGateway};
use payment_service::server::{AppState, app};
use payment_service::store::{Payments, PgPayments};
use serde_json::{Value, json};
use sqlx::PgPool;
use support::{Broken, Postgres, event_types};
use telemetry::logging::Logger;
use tower::ServiceExt;
use uuid::Uuid;

const MISSING: &str = "00000000-0000-4000-8000-000000000000";
const TOKEN: &str = "Bearer t0k";
const APPROVE: &str = "4242 4242 4242 4242";
const DECLINE: &str = "4000000000000002";

fn service<S: Payments>(store: S) -> Router {
    app(AppState {
        store,
        gateway: SimulatedGateway::default(),
        auth: OperatorAuth::new("t0k", "ops"),
        logger: Logger::stdout("payment-service-test"),
    })
}

struct Answer {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: String,
}

impl Answer {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap()
    }
}

async fn send(
    app: &Router,
    method: &str,
    path: &str,
    body: Option<&str>,
    headers: &[(&str, &str)],
) -> Answer {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let request = request
        .body(body.map_or_else(Body::empty, |b| Body::from(b.to_owned())))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    Answer {
        status,
        headers,
        body: String::from_utf8(bytes.to_vec()).unwrap(),
    }
}

async fn call(app: &Router, method: &str, path: &str, body: Option<Value>) -> Answer {
    send(
        app,
        method,
        path,
        body.map(|b| b.to_string()).as_deref(),
        &[],
    )
    .await
}

fn error(reason: &str) -> Value {
    json!({ "error": reason })
}

async fn setup() -> (Postgres, PgPool, PgPayments, Router) {
    let postgres = Postgres::start();
    let pool = support::pool(&postgres).await;
    let store = PgPayments::new(pool.clone());
    let app = service(store.clone());
    (postgres, pool, store, app)
}

fn method_body(card: &str) -> Value {
    json!({
        "customerId": "c1",
        "cardNumber": card,
        "expiry": "12/99",
        "billingAddress": {
            "line1": "1 High Street",
            "city": "London",
            "postalCode": "SW1A 1AA",
            "country": "GB",
        },
    })
}

async fn add_method(app: &Router, card: &str) -> String {
    let created = call(app, "POST", "/payments/methods", Some(method_body(card))).await;
    assert_eq!(created.status, StatusCode::CREATED, "{}", created.body);
    created.json()["id"].as_str().unwrap().to_owned()
}

async fn authorize(app: &Router, order: &str, amount: i64, method: &str) -> Answer {
    call(
        app,
        "POST",
        "/payments/authorize",
        Some(json!({"orderId": order, "amountCents": amount, "paymentMethodId": method})),
    )
    .await
}

async fn operator(app: &Router, method: &str, path: &str, body: Option<&str>) -> Answer {
    send(app, method, path, body, &[("authorization", TOKEN)]).await
}

#[tokio::test]
async fn a_method_is_tokenized_and_judged_by_its_rules() {
    let (_pg, _pool, store, app) = setup().await;
    let created = call(
        &app,
        "POST",
        "/payments/methods",
        Some(method_body(APPROVE)),
    )
    .await;
    assert_eq!(created.status, StatusCode::CREATED);
    let body = created.json();
    assert_eq!(
        (
            body["last4"].as_str(),
            body["brand"].as_str(),
            body["expiry"].as_str()
        ),
        (Some("4242"), Some("visa"), Some("12/99"))
    );
    assert_eq!(body.as_object().unwrap().len(), 4);
    let id = Uuid::parse_str(body["id"].as_str().unwrap()).unwrap();
    let stored = store.method(id).await.unwrap().unwrap();
    assert_eq!(stored.card_token, "tok_visa_4242");

    let mut bad = method_body("4242");
    let refused = call(&app, "POST", "/payments/methods", Some(bad.clone())).await;
    assert_eq!(
        (refused.status, refused.json()),
        (StatusCode::BAD_REQUEST, error("Invalid card number"))
    );
    bad["cardNumber"] = json!(APPROVE);
    bad["billingAddress"]["country"] = json!("gb");
    let refused = call(&app, "POST", "/payments/methods", Some(bad)).await;
    assert_eq!(
        refused.json(),
        error("country must be an ISO 3166-1 alpha-2 code")
    );
    let undecodable = send(&app, "POST", "/payments/methods", Some("{"), &[]).await;
    assert_eq!(undecodable.status, StatusCode::BAD_REQUEST);
    assert!(
        undecodable.body.starts_with("Invalid value for: body ("),
        "{}",
        undecodable.body
    );
}

#[tokio::test]
async fn a_payment_is_created_pending_one_per_order_read_and_deleted() {
    let (_pg, _pool, _store, app) = setup().await;
    let created = call(
        &app,
        "POST",
        "/payments",
        Some(json!({"orderId": "order-1", "amountCents": 4999})),
    )
    .await;
    assert_eq!(created.status, StatusCode::CREATED);
    let payment = created.json();
    assert_eq!(payment["status"], "pending");
    assert_eq!(payment.as_object().unwrap().len(), 6);
    let again = call(
        &app,
        "POST",
        "/payments",
        Some(json!({"orderId": "order-1", "amountCents": 4999})),
    )
    .await;
    assert_eq!(
        (again.status, again.json()),
        (
            StatusCode::CONFLICT,
            error("Order order-1 already has a payment")
        )
    );
    for (body, reason) in [
        (
            json!({"orderId": " ", "amountCents": 0}),
            "orderId is required",
        ),
        (
            json!({"orderId": "o", "amountCents": 0}),
            "amountCents must be positive",
        ),
    ] {
        let refused = call(&app, "POST", "/payments", Some(body)).await;
        assert_eq!(
            (refused.status, refused.json()),
            (StatusCode::BAD_REQUEST, error(reason))
        );
    }
    let missing = call(&app, "POST", "/payments", Some(json!({"orderId": "o"}))).await;
    assert_eq!(missing.status, StatusCode::BAD_REQUEST);

    let path = format!("/payments/{}", payment["id"].as_str().unwrap());
    assert_eq!(call(&app, "GET", &path, None).await.json(), payment);
    assert_eq!(
        call(&app, "DELETE", &path, None).await.status,
        StatusCode::NO_CONTENT
    );
    for method in ["GET", "DELETE"] {
        let gone = call(&app, method, &path, None).await;
        assert_eq!(
            (gone.status, gone.json()),
            (StatusCode::NOT_FOUND, error("Payment not found"))
        );
    }
    let malformed = call(&app, "GET", "/payments/not-a-uuid", None).await;
    assert_eq!(
        (malformed.status, malformed.body.as_str()),
        (
            StatusCode::BAD_REQUEST,
            "Invalid value for: path parameter id"
        )
    );
}

#[tokio::test]
async fn an_authorization_holds_once_per_order_and_replays() {
    let (_pg, pool, _store, app) = setup().await;
    let method = add_method(&app, APPROVE).await;
    let first = authorize(&app, "o1", 4999, &method).await;
    assert_eq!(first.status, StatusCode::OK, "{}", first.body);
    let held = first.json();
    assert!(held["authId"].as_str().unwrap().starts_with("auth_"));
    assert_eq!(authorize(&app, "o1", 4999, &method).await.json(), held);
    assert_eq!(event_types(&pool, "o1").await, ["PaymentAuthorized"]);

    let other = add_method(&app, APPROVE).await;
    for (amount, method) in [(1, method.as_str()), (4999, other.as_str())] {
        let refused = authorize(&app, "o1", amount, method).await;
        assert_eq!(
            (refused.status, refused.json()),
            (
                StatusCode::CONFLICT,
                error("Order already has a payment with a different amount or payment method")
            )
        );
    }

    let declining = add_method(&app, DECLINE).await;
    let declined = authorize(&app, "o2", 4999, &declining).await;
    let decline = json!({"error": "Payment declined", "reason": "insufficient_funds"});
    assert_eq!(
        (declined.status, declined.json()),
        (StatusCode::PAYMENT_REQUIRED, decline.clone())
    );
    assert_eq!(
        authorize(&app, "o2", 4999, &declining).await.json(),
        decline
    );
    assert_eq!(event_types(&pool, "o2").await, Vec::<String>::new());

    let missing = authorize(&app, "o3", 1, MISSING).await;
    assert_eq!(
        (missing.status, missing.json()),
        (StatusCode::NOT_FOUND, error("Payment method not found"))
    );
    let malformed = authorize(&app, "o3", 1, "nope").await;
    assert_eq!(
        (malformed.status, malformed.body.as_str()),
        (
            StatusCode::BAD_REQUEST,
            "Invalid value for: body (Invalid payment method id: 'nope' at 'paymentMethodId')"
        )
    );
    assert_eq!(
        authorize(&app, " ", 1, &method).await.json(),
        error("orderId is required")
    );
    assert_eq!(
        authorize(&app, "o3", 0, &method).await.json(),
        error("amountCents must be positive")
    );
}

#[tokio::test]
async fn a_spent_or_broken_stored_payment_isnt_replayed() {
    let (_pg, pool, _store, app) = setup().await;
    let method = add_method(&app, APPROVE).await;
    let id = Uuid::parse_str(&method).unwrap();
    let stored = |status: &str, auth: Option<&str>, expires: &str, reason: Option<&str>| {
        let sql = format!(
            "insert into payment (id, order_id, amount_cents, status, payment_method_id, \
             auth_id, expires_at, decline_reason) values ($1, $2, 4999, '{status}', $3, $4, \
             {expires}, $5)"
        );
        let (pool, auth, reason) = (
            pool.clone(),
            auth.map(str::to_owned),
            reason.map(str::to_owned),
        );
        let order = format!("order-{status}-{}", auth.is_some());
        async move {
            sqlx::query(&sql)
                .bind(Uuid::new_v4())
                .bind(&order)
                .bind(id)
                .bind(auth)
                .bind(reason)
                .execute(&pool)
                .await
                .unwrap();
            order
        }
    };
    let pending = stored("pending", None, "null", None).await;
    let voided = stored("voided", Some("auth_1"), "now()", None).await;
    let lapsed = stored(
        "authorized",
        Some("auth_1"),
        "now() - interval '1 second'",
        None,
    )
    .await;
    let incomplete = stored("authorized", None, "null", None).await;
    let reasonless = stored("failed", None, "null", None).await;
    for (order, status, reason) in [
        (
            &pending,
            StatusCode::CONFLICT,
            "Order already has a payment in status pending",
        ),
        (
            &voided,
            StatusCode::CONFLICT,
            "Order already has a payment in status voided",
        ),
        (
            &lapsed,
            StatusCode::CONFLICT,
            "Order's stored authorization has expired",
        ),
        (
            &incomplete,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Stored authorization is incomplete",
        ),
        (
            &reasonless,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Stored decline has no reason",
        ),
    ] {
        let refused = authorize(&app, order, 4999, &method).await;
        assert_eq!(
            (refused.status, refused.json()),
            (status, error(reason)),
            "{order}"
        );
    }
}

#[tokio::test]
async fn an_operator_expires_an_authorization_now() {
    let (_pg, pool, _store, app) = setup().await;
    let method = add_method(&app, APPROVE).await;
    authorize(&app, "o1", 4999, &method).await;
    let path = "/payments/authorizations/o1/expire";
    for headers in [&[][..], &[("authorization", "Bearer nope")][..]] {
        let refused = send(&app, "POST", path, None, headers).await;
        assert_eq!(
            (refused.status, refused.json()),
            (StatusCode::UNAUTHORIZED, error("Invalid operator token"))
        );
    }
    let expired = operator(&app, "POST", path, None).await;
    assert_eq!(expired.status, StatusCode::OK, "{}", expired.body);
    assert_eq!(
        (
            expired.json()["orderId"].as_str(),
            expired.json()["status"].as_str()
        ),
        (Some("o1"), Some("voided"))
    );
    assert_eq!(
        event_types(&pool, "o1").await,
        ["PaymentAuthorized", "PaymentAuthorizationExpired"]
    );
    let again = operator(&app, "POST", path, None).await;
    assert_eq!(
        (again.status, again.json()),
        (
            StatusCode::CONFLICT,
            error("Payment is voided, not authorized")
        )
    );
    let missing = operator(&app, "POST", "/payments/authorizations/nobody/expire", None).await;
    assert_eq!(
        (missing.status, missing.json()),
        (StatusCode::NOT_FOUND, error("Payment not found"))
    );
}

#[tokio::test]
async fn reconciliations_are_listed_and_resolved_by_an_operator() {
    let (_pg, _pool, store, app) = setup().await;
    store.create("o1", 1).await.unwrap();
    store.flag("o1", "void_after_capture").await.unwrap();
    let listed = call(&app, "GET", "/reconciliations?status=open&orderId=o1", None).await;
    let flags = listed.json();
    assert_eq!(flags.as_array().unwrap().len(), 1);
    assert_eq!(flags[0]["operatorId"], Value::Null);
    assert_eq!(
        call(&app, "GET", "/reconciliations?orderId=other", None)
            .await
            .json(),
        json!([])
    );
    let bad = call(&app, "GET", "/reconciliations?status=closed", None).await;
    let only = error("Invalid reconciliation status: only 'resolved' is accepted");
    assert_eq!(
        (bad.status, bad.json()),
        (StatusCode::BAD_REQUEST, only.clone())
    );

    let resolve = r#"{"status":"resolved","note":"  Refunded  "}"#;
    let unauthorized = send(&app, "PATCH", "/reconciliations/o1", Some(resolve), &[]).await;
    assert_eq!(unauthorized.status, StatusCode::UNAUTHORIZED);
    let open = operator(
        &app,
        "PATCH",
        "/reconciliations/o1",
        Some(r#"{"status":"open"}"#),
    )
    .await;
    assert_eq!((open.status, open.json()), (StatusCode::BAD_REQUEST, only));
    let resolved = operator(&app, "PATCH", "/reconciliations/o1", Some(resolve)).await;
    assert_eq!(resolved.status, StatusCode::OK);
    let flag = resolved.json();
    assert_eq!(
        (
            flag["status"].as_str(),
            flag["operatorId"].as_str(),
            flag["note"].as_str()
        ),
        (Some("resolved"), Some("ops"), Some("Refunded"))
    );
    let again = operator(
        &app,
        "PATCH",
        "/reconciliations/o1",
        Some(r#"{"status":"resolved"}"#),
    )
    .await;
    assert_eq!(again.json(), flag);
    let missing = operator(&app, "PATCH", "/reconciliations/nobody", Some(resolve)).await;
    assert_eq!(
        (missing.status, missing.json()),
        (
            StatusCode::NOT_FOUND,
            error("Reconciliation flag not found")
        )
    );
    let undecodable = operator(&app, "PATCH", "/reconciliations/o1", Some("{")).await;
    assert_eq!(undecodable.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_store_failure_is_a_500() {
    let app = service(Broken);
    for (method, path, body) in [
        (
            "POST",
            "/payments/methods".to_owned(),
            Some(method_body(APPROVE)),
        ),
        (
            "POST",
            "/payments".to_owned(),
            Some(json!({"orderId": "o", "amountCents": 1})),
        ),
        ("GET", format!("/payments/{MISSING}"), None),
        ("DELETE", format!("/payments/{MISSING}"), None),
        (
            "POST",
            "/payments/authorize".to_owned(),
            Some(json!({"orderId": "o", "amountCents": 1, "paymentMethodId": MISSING})),
        ),
        ("GET", "/reconciliations".to_owned(), None),
    ] {
        let answer = call(&app, method, &path, body).await;
        assert_eq!(
            (answer.status, answer.body.as_str()),
            (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error"),
            "{method} {path}"
        );
    }
    for (path, body) in [
        ("/payments/authorizations/o/expire", None),
        ("/reconciliations/o", Some(r#"{"status":"resolved"}"#)),
    ] {
        let method = if body.is_some() { "PATCH" } else { "POST" };
        let answer = operator(&app, method, path, body).await;
        assert_eq!(answer.status, StatusCode::INTERNAL_SERVER_ERROR, "{path}");
    }
    assert_eq!(
        call(&app, "GET", "/health/ready", None).await.status,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn the_edge_allows_the_operator_token_and_serves_the_docs() {
    let app = service(Broken);
    let preflight = send(
        &app,
        "OPTIONS",
        "/payments/authorize",
        None,
        &[
            ("origin", "http://localhost:5173"),
            ("access-control-request-method", "POST"),
            (
                "access-control-request-headers",
                "content-type, authorization",
            ),
        ],
    )
    .await;
    assert_eq!(preflight.status, StatusCode::OK);
    let allowed = preflight.headers["access-control-allow-headers"]
        .to_str()
        .unwrap()
        .to_lowercase();
    assert!(allowed.contains("authorization"), "{allowed}");
    assert_eq!(
        call(&app, "GET", "/nope", None).await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "GET", "/payments/authorize", None).await.status,
        StatusCode::METHOD_NOT_ALLOWED
    );

    let yaml = call(&app, "GET", "/docs/docs.yaml", None).await;
    assert_eq!(yaml.status, StatusCode::OK);
    for path in [
        "/payments/methods",
        "/payments/authorizations/{orderId}/expire",
        "/reconciliations/{orderId}",
        "/health/ready",
    ] {
        assert!(yaml.body.contains(path), "{path}");
    }
    let asyncapi = call(&app, "GET", "/docs/asyncapi.json", None).await.json();
    let sent = &asyncapi["channels"]["payment-events"]["messages"];
    assert_eq!(
        sent["PaymentFailed"]["payload"]["required"],
        json!(["orderId", "paymentId", "amountCents", "reason", "timestamp"])
    );
    let dispatch =
        &asyncapi["channels"]["fulfilment-events"]["messages"]["DispatchConfirmed"]["payload"];
    assert_eq!(dispatch["required"], json!(["orderId", "trackingNo"]));
    assert_eq!(asyncapi["operations"]["payment-events"]["action"], "send");
    assert_eq!(asyncapi["operations"]["order-events"]["action"], "receive");
    let page = call(&app, "GET", "/docs/asyncapi/", None).await;
    assert!(page.body.contains("PaymentAuthorizationExpired"));
}
