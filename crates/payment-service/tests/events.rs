// SPDX-License-Identifier: MIT

//! The consumers' routes, less the stream, over a throwaway Postgres: which
//! envelopes they act on, what they do to the payment, the flags they raise
//! and the lines they log; and the expiry sweep. The rest of the per-record
//! decision is gluon-rs `kafka-event-consumer`'s; the consumers against a
//! broker are tests/contract's.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use std::time::Duration;

use kafka_event_consumer::envelope::{Envelope, Routes};
use payment_service::consumers::{LAG_INTERVAL, settings, sweep, sweep_once};
use payment_service::domain::{
    Address, Authorization, CardBrand, OrderId, PaymentMethod, PaymentStatus, SimulatedGateway,
};
use payment_service::events::{
    EventsError, FULFILMENT_EVENTS, FULFILMENT_GROUP, FulfilmentEvents, ORDER_EVENTS, ORDER_GROUP,
    OrderEvents,
};
use payment_service::store::{Payments, PgPayments};
use serde_json::{Value, json};
use sqlx::PgPool;
use support::{Broken, LogBuffer, Postgres, captured_logger, event_types, log_lines};
use telemetry::logging::Logger;
use time::OffsetDateTime;
use tokio::sync::watch;
use uuid::Uuid;

fn envelope(event_type: &str, payload: &Value) -> Envelope {
    serde_json::from_value(json!({
        "eventId": "e1",
        "eventType": event_type,
        "timestamp": "2026-11-20T10:00:00Z",
        "source": "test",
        "correlationId": "c1",
        "payload": payload,
    }))
    .unwrap()
}

fn cancelled(order: &str) -> Envelope {
    envelope(
        "OrderCancelled",
        &json!({"orderId": order, "customerId": "c", "timestamp": "t", "reason": "customer"}),
    )
}

fn dispatched(order: &str) -> Envelope {
    envelope(
        "DispatchConfirmed",
        &json!({"orderId": order, "trackingNo": "TRK-1", "items": [
            {"sku": "S", "productName": "Cup", "quantity": 1, "unitPriceCents": 4999}
        ]}),
    )
}

fn dispatch_failed(order: &str) -> Envelope {
    envelope(
        "DispatchFailed",
        &json!({"orderId": order, "reason": "carrier_unavailable"}),
    )
}

struct Setup {
    _postgres: Postgres,
    pool: PgPool,
    store: PgPayments,
    orders: OrderEvents<PgPayments>,
    fulfilment: FulfilmentEvents<PgPayments>,
    log: LogBuffer,
}

async fn setup() -> Setup {
    let postgres = Postgres::start();
    let pool = support::pool(&postgres).await;
    let store = PgPayments::new(pool.clone());
    let (logger, log) = captured_logger();
    let gateway = SimulatedGateway::default();
    Setup {
        orders: OrderEvents::new(store.clone(), gateway.clone(), logger.clone()),
        fulfilment: FulfilmentEvents::new(store.clone(), gateway, logger),
        _postgres: postgres,
        pool,
        store,
        log,
    }
}

/// The order authorized for 4999 on a method tokenized `token`, the hold
/// lasting `lasting`.
async fn authorized(store: &PgPayments, order: &str, token: &str, lasting: time::Duration) {
    let id = Uuid::new_v4();
    store
        .add_method(&PaymentMethod {
            id,
            customer_id: "c".to_owned(),
            card_token: token.to_owned(),
            last4: "4242".to_owned(),
            brand: CardBrand::Visa,
            expiry: "12/99".to_owned(),
            billing_address: Address {
                line1: "1".to_owned(),
                line2: None,
                city: "L".to_owned(),
                postal_code: "P".to_owned(),
                country: "GB".to_owned(),
            },
        })
        .await
        .unwrap();
    let hold = Authorization {
        auth_id: "auth_1".to_owned(),
        expires_at: OffsetDateTime::now_utc() + lasting,
    };
    store
        .record_authorization(order, 4999, id, &hold)
        .await
        .unwrap()
        .unwrap();
}

async fn handled<R: Routes>(route: &R, envelope: &Envelope) -> Result<(), R::Error> {
    let Some(Ok(event)) = route.decode(envelope) else {
        panic!("not routed and decoded");
    };
    route.handle("e1", &event).await
}

async fn status(store: &PgPayments, order: &str) -> PaymentStatus {
    store.by_order(order).await.unwrap().unwrap().status
}

async fn reasons(store: &PgPayments, order: &str) -> Vec<String> {
    store
        .reconciliations(None, Some(order))
        .await
        .unwrap()
        .into_iter()
        .map(|flag| flag.reason)
        .collect()
}

fn messages(log: &LogBuffer) -> Vec<String> {
    log_lines(log)
        .iter()
        .map(|line| line["message"].as_str().unwrap().to_owned())
        .collect()
}

const DAYS: time::Duration = time::Duration::days(7);

#[tokio::test]
async fn order_cancelled_voids_the_hold_and_skips_what_it_cant_void() {
    let s = setup().await;
    authorized(&s.store, "o1", "tok_visa_4242", DAYS).await;
    handled(&s.orders, &cancelled("o1")).await.unwrap();
    assert_eq!(status(&s.store, "o1").await, PaymentStatus::Voided);
    assert_eq!(event_types(&s.pool, "o1").await, ["PaymentAuthorized"]);
    handled(&s.orders, &cancelled("o1")).await.unwrap();
    handled(&s.orders, &cancelled("nobody")).await.unwrap();
    s.store.create("pending", 1).await.unwrap();
    handled(&s.orders, &cancelled("pending")).await.unwrap();
    assert_eq!(
        messages(&s.log),
        [
            "Authorization voided on order cancellation",
            "OrderCancelled for an already-voided payment - skipping duplicate",
            "OrderCancelled for an unknown payment - skipping",
            "OrderCancelled for a non-authorized payment - skipping",
        ]
    );
}

#[tokio::test]
async fn a_cancel_after_capture_is_flagged() {
    let s = setup().await;
    authorized(&s.store, "o1", "tok_visa_4242", DAYS).await;
    handled(&s.fulfilment, &dispatched("o1")).await.unwrap();
    assert_eq!(status(&s.store, "o1").await, PaymentStatus::Captured);
    handled(&s.orders, &cancelled("o1")).await.unwrap();
    handled(&s.fulfilment, &dispatch_failed("o1"))
        .await
        .unwrap();
    assert_eq!(reasons(&s.store, "o1").await, ["void_after_capture"]);
    assert_eq!(
        event_types(&s.pool, "o1").await,
        ["PaymentAuthorized", "PaymentCaptured"]
    );
}

#[tokio::test]
async fn dispatch_confirmed_captures_and_flags_a_capture_it_cant_make() {
    let s = setup().await;
    authorized(&s.store, "o1", "tok_visa_4242", DAYS).await;
    handled(&s.fulfilment, &dispatched("o1")).await.unwrap();
    handled(&s.fulfilment, &dispatched("o1")).await.unwrap();
    assert_eq!(
        event_types(&s.pool, "o1").await,
        ["PaymentAuthorized", "PaymentCaptured"]
    );

    authorized(&s.store, "void", "tok_visa_4242", DAYS).await;
    handled(&s.fulfilment, &dispatch_failed("void"))
        .await
        .unwrap();
    assert_eq!(status(&s.store, "void").await, PaymentStatus::Voided);
    handled(&s.fulfilment, &dispatched("void")).await.unwrap();
    assert_eq!(reasons(&s.store, "void").await, ["capture_after_void"]);

    authorized(
        &s.store,
        "lapsed",
        "tok_visa_4242",
        time::Duration::seconds(-1),
    )
    .await;
    handled(&s.fulfilment, &dispatched("lapsed")).await.unwrap();
    assert_eq!(status(&s.store, "lapsed").await, PaymentStatus::Authorized);
    assert_eq!(
        reasons(&s.store, "lapsed").await,
        ["dispatch_after_authorization_expired"]
    );

    handled(&s.fulfilment, &dispatched("nobody")).await.unwrap();
    s.store.create("pending", 1).await.unwrap();
    handled(&s.fulfilment, &dispatched("pending"))
        .await
        .unwrap();
    assert_eq!(reasons(&s.store, "pending").await, Vec::<String>::new());
    let logged = messages(&s.log);
    for message in [
        "Payment captured",
        "DispatchConfirmed for an already-captured payment - skipping duplicate",
        "Authorization voided",
        "Capture after void - reconciliation flagged",
        "Capture after authorization expiry - reconciliation flagged",
        "DispatchConfirmed for an unknown payment - skipping",
        "DispatchConfirmed for a non-authorized payment - skipping",
    ] {
        assert!(logged.iter().any(|m| m == message), "{message}: {logged:?}");
    }
}

#[tokio::test]
async fn a_declined_capture_records_payment_failed_and_a_later_dispatch_is_flagged() {
    let s = setup().await;
    authorized(&s.store, "o1", "tok_mastercard_4444", DAYS).await;
    handled(&s.fulfilment, &dispatched("o1")).await.unwrap();
    let payment = s.store.by_order("o1").await.unwrap().unwrap();
    assert_eq!(
        (payment.status, payment.decline_reason.as_deref()),
        (PaymentStatus::Failed, Some("capture_failed"))
    );
    assert_eq!(
        event_types(&s.pool, "o1").await,
        ["PaymentAuthorized", "PaymentFailed"]
    );
    handled(&s.fulfilment, &dispatched("o1")).await.unwrap();
    assert_eq!(reasons(&s.store, "o1").await, ["capture_after_failed"]);
    // A late dispatch failure for a failed payment: nothing to void.
    handled(&s.fulfilment, &dispatch_failed("o1"))
        .await
        .unwrap();
    assert_eq!(status(&s.store, "o1").await, PaymentStatus::Failed);
}

#[tokio::test]
async fn an_authorized_payment_with_no_method_is_an_error() {
    let s = setup().await;
    sqlx::query(
        "insert into payment (id, order_id, amount_cents, status, auth_id, expires_at) \
         values ($1, 'orphan', 1, 'authorized', 'auth_x', now() + interval '1 day')",
    )
    .bind(Uuid::new_v4())
    .execute(&s.pool)
    .await
    .unwrap();
    let error = handled(&s.fulfilment, &dispatched("orphan"))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("has no paymentMethodId"),
        "{error}"
    );
    sqlx::query("update payment set auth_id = null where order_id = 'orphan'")
        .execute(&s.pool)
        .await
        .unwrap();
    for error in [
        handled(&s.fulfilment, &dispatched("orphan"))
            .await
            .unwrap_err(),
        handled(&s.orders, &cancelled("orphan")).await.unwrap_err(),
    ] {
        assert!(error.to_string().contains("has no authId"), "{error}");
    }
    sqlx::query(
        "update payment set auth_id = 'auth_x', payment_method_id = null where order_id = 'orphan'",
    )
    .execute(&s.pool)
    .await
    .unwrap();
    let unknown = Uuid::new_v4();
    sqlx::query("alter table payment drop constraint payment_payment_method_id_fkey")
        .execute(&s.pool)
        .await
        .unwrap();
    sqlx::query("update payment set payment_method_id = $1 where order_id = 'orphan'")
        .bind(unknown)
        .execute(&s.pool)
        .await
        .unwrap();
    let error = handled(&s.fulfilment, &dispatched("orphan"))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("unknown payment method"),
        "{error}"
    );
}

#[tokio::test]
async fn a_store_failure_is_an_error() {
    let logger = Logger::stdout("payment-service-test");
    let orders = OrderEvents::new(Broken, SimulatedGateway::default(), logger.clone());
    let fulfilment = FulfilmentEvents::new(Broken, SimulatedGateway::default(), logger);
    assert!(matches!(
        handled(&orders, &cancelled("o1")).await,
        Err(EventsError::Store(_))
    ));
    assert!(matches!(
        handled(&fulfilment, &dispatched("o1")).await,
        Err(EventsError::Store(_))
    ));
}

#[test]
fn what_each_topic_routes_and_what_is_poison() {
    let logger = Logger::stdout("payment-service-test");
    let orders = OrderEvents::new(Broken, SimulatedGateway::default(), logger.clone());
    let fulfilment = FulfilmentEvents::new(Broken, SimulatedGateway::default(), logger);
    assert!(
        orders
            .decode(&envelope("OrderPlaced", &json!({})))
            .is_none()
    );
    assert!(matches!(
        orders.decode(&envelope("OrderCancelled", &json!({"orderId": "o"}))),
        Some(Err(EventsError::Payload(_)))
    ));
    let blank = json!({"orderId": " ", "customerId": "c", "timestamp": "t"});
    assert!(matches!(
        orders.decode(&envelope("OrderCancelled", &blank)),
        Some(Err(EventsError::OrderId(_)))
    ));
    let (id, _) = orders.decode(&cancelled("o1")).unwrap().unwrap();
    assert_eq!(id, OrderId::parse("o1").unwrap());

    let unknown = fulfilment.decode(&envelope("DispatchStarted", &json!({})));
    let Some(Err(error)) = unknown else {
        panic!("not poison");
    };
    assert_eq!(
        error.to_string(),
        "Unknown fulfilment-events event type 'DispatchStarted'"
    );
    let no_items = json!({"orderId": "o1", "trackingNo": "T"});
    assert!(matches!(
        fulfilment.decode(&envelope("DispatchConfirmed", &no_items)),
        Some(Err(EventsError::NoItems))
    ));
    assert!(matches!(
        fulfilment.decode(&envelope("DispatchFailed", &json!({"orderId": "o1"}))),
        Some(Err(EventsError::Payload(_)))
    ));
    assert!(matches!(fulfilment.decode(&dispatched("o1")), Some(Ok(_))));
    assert!(matches!(
        fulfilment.decode(&dispatch_failed("o1")),
        Some(Ok(_))
    ));
}

#[tokio::test]
async fn the_sweep_voids_lapsed_holds_each_tick_until_stopped() {
    let s = setup().await;
    authorized(
        &s.store,
        "lapsed",
        "tok_visa_4242",
        time::Duration::seconds(-1),
    )
    .await;
    let (logger, log) = captured_logger();
    let (stop, stopped) = watch::channel(false);
    let running = tokio::spawn(sweep(
        s.store.clone(),
        Duration::from_millis(50),
        logger,
        stopped,
    ));
    while status(&s.store, "lapsed").await != PaymentStatus::Voided {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    stop.send(true).unwrap();
    running.await.unwrap();
    assert_eq!(
        event_types(&s.pool, "lapsed").await,
        ["PaymentAuthorized", "PaymentAuthorizationExpired"]
    );
    assert!(messages(&log).contains(&"Authorization expired - payment voided".to_owned()));
}

#[tokio::test]
async fn a_failed_sweep_tick_is_logged_and_retried() {
    let (logger, log) = captured_logger();
    assert!(sweep_once(&Broken, &logger).await.is_err());
    let (stop, stopped) = watch::channel(false);
    let running = tokio::spawn(sweep(Broken, Duration::from_millis(10), logger, stopped));
    while !messages(&log).contains(&"Expiry sweep tick failed - retrying next tick".to_owned()) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop.send(true).unwrap();
    running.await.unwrap();
}

#[test]
fn the_consumers_read_their_topics_as_their_groups() {
    for (topic, group) in [
        (ORDER_EVENTS, ORDER_GROUP),
        (FULFILMENT_EVENTS, FULFILMENT_GROUP),
    ] {
        let settings = settings("localhost:9092", topic, group).unwrap();
        assert_eq!(settings.topic.as_str(), topic);
        assert_eq!(settings.group_id.as_str(), group);
        assert_eq!(settings.lag_interval, LAG_INTERVAL);
    }
}
