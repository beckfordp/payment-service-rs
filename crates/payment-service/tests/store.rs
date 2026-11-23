// SPDX-License-Identifier: MIT

//! The Postgres store over a throwaway cluster, migrated as the service
//! migrates it: one payment per order, each change and its outbox event in
//! one transaction, the conditional transitions, the sweep, and the flags.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use std::time::Duration;

use payment_service::domain::{
    AUTHORIZATION_EXPIRED, Address, Authorization, CardBrand, PaymentMethod, PaymentStatus,
    ReconciliationStatus, ResolveOutcome,
};
use payment_service::store::{Payments, PgPayments};
use sqlx::PgPool;
use support::{Postgres, event_types, outbox};
use time::OffsetDateTime;
use uuid::Uuid;

async fn setup() -> (Postgres, PgPool, PgPayments) {
    let postgres = Postgres::start();
    let pool = support::pool(&postgres).await;
    (postgres, pool.clone(), PgPayments::new(pool))
}

fn method(token: &str) -> PaymentMethod {
    PaymentMethod {
        id: Uuid::new_v4(),
        customer_id: "c1".to_owned(),
        card_token: token.to_owned(),
        last4: "4242".to_owned(),
        brand: CardBrand::Visa,
        expiry: "12/99".to_owned(),
        billing_address: Address {
            line1: "1 High Street".to_owned(),
            line2: Some("Flat 2".to_owned()),
            city: "London".to_owned(),
            postal_code: "SW1A 1AA".to_owned(),
            country: "GB".to_owned(),
        },
    }
}

fn hold(lasting: time::Duration) -> Authorization {
    Authorization {
        auth_id: format!("auth_{}", Uuid::new_v4()),
        expires_at: OffsetDateTime::now_utc() + lasting,
    }
}

/// A stored method and an order authorized on it for 4999.
async fn authorized(store: &PgPayments, order: &str, lasting: time::Duration) -> Uuid {
    let stored = method("tok_visa_4242");
    store.add_method(&stored).await.unwrap();
    store
        .record_authorization(order, 4999, stored.id, &hold(lasting))
        .await
        .unwrap()
        .unwrap();
    stored.id
}

#[tokio::test]
async fn a_method_reads_back_as_written() {
    let (_pg, _pool, store) = setup().await;
    let stored = method("tok_visa_4242");
    store.add_method(&stored).await.unwrap();
    assert_eq!(store.method(stored.id).await.unwrap(), Some(stored));
    assert_eq!(store.method(Uuid::new_v4()).await.unwrap(), None);
}

#[tokio::test]
async fn a_payment_is_one_per_order_and_is_read_and_deleted_by_id() {
    let (_pg, _pool, store) = setup().await;
    let created = store.create("order-1", 4999).await.unwrap().unwrap();
    assert_eq!(created.status, PaymentStatus::Pending);
    assert_eq!(created.created_at, created.updated_at);
    assert_eq!(store.create("order-1", 1).await.unwrap(), None);
    assert_eq!(store.get(created.id).await.unwrap(), Some(created.clone()));
    assert_eq!(
        store.by_order("order-1").await.unwrap(),
        Some(created.clone())
    );
    assert!(store.delete(created.id).await.unwrap());
    assert!(!store.delete(created.id).await.unwrap());
    assert_eq!(store.get(created.id).await.unwrap(), None);
}

#[tokio::test]
async fn an_authorization_writes_payment_authorized_with_it() {
    let (_pg, pool, store) = setup().await;
    let stored = method("tok_visa_4242");
    store.add_method(&stored).await.unwrap();
    let placed = hold(time::Duration::days(7));
    let payment = store
        .record_authorization("o1", 4999, stored.id, &placed)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(payment.status, PaymentStatus::Authorized);
    assert_eq!(payment.auth_id.as_deref(), Some(placed.auth_id.as_str()));
    assert_eq!(payment.payment_method_id, Some(stored.id));
    let written = outbox(&pool, "o1").await;
    assert_eq!(written.len(), 1);
    let envelope = &written[0];
    assert_eq!(
        (
            &envelope["eventType"],
            &envelope["source"],
            &envelope["correlationId"]
        ),
        (
            &"PaymentAuthorized".into(),
            &"payment-service".into(),
            &"o1".into()
        )
    );
    assert_eq!(envelope["payload"]["authId"], placed.auth_id.as_str());
    assert_eq!(envelope["payload"]["orderId"], "o1");
    // A second outcome for the order records nothing and writes nothing.
    assert_eq!(
        store
            .record_authorization("o1", 4999, stored.id, &placed)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .record_decline("o1", 4999, stored.id, "insufficient_funds")
            .await
            .unwrap(),
        None
    );
    assert_eq!(event_types(&pool, "o1").await, ["PaymentAuthorized"]);
}

#[tokio::test]
async fn a_decline_is_recorded_failed_with_no_event() {
    let (_pg, pool, store) = setup().await;
    let stored = method("tok_visa_0002");
    store.add_method(&stored).await.unwrap();
    let payment = store
        .record_decline("o1", 4999, stored.id, "insufficient_funds")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (payment.status, payment.decline_reason.as_deref()),
        (PaymentStatus::Failed, Some("insufficient_funds"))
    );
    assert_eq!(outbox(&pool, "o1").await, Vec::<serde_json::Value>::new());
}

#[tokio::test]
async fn transitions_move_only_an_authorized_payment_and_write_their_events() {
    let (_pg, pool, store) = setup().await;
    authorized(&store, "captured", time::Duration::days(7)).await;
    let captured = store.record_capture("captured").await.unwrap().unwrap();
    assert_eq!(captured.status, PaymentStatus::Captured);
    assert_eq!(store.record_capture("captured").await.unwrap(), None);
    assert_eq!(store.record_void("captured").await.unwrap(), None);
    let written = outbox(&pool, "captured").await;
    assert_eq!(written[1]["eventType"], "PaymentCaptured");
    assert_eq!(written[1]["payload"]["amountCents"], 4999);

    authorized(&store, "failed", time::Duration::days(7)).await;
    let failed = store
        .record_capture_failure("failed", "capture_failed")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (failed.status, failed.decline_reason.as_deref()),
        (PaymentStatus::Failed, Some("capture_failed"))
    );
    let written = outbox(&pool, "failed").await;
    assert_eq!(written[1]["eventType"], "PaymentFailed");
    assert_eq!(written[1]["payload"]["reason"], "capture_failed");
    assert_eq!(written[1]["payload"]["paymentId"], failed.id.to_string());

    authorized(&store, "voided", time::Duration::days(7)).await;
    let voided = store.record_void("voided").await.unwrap().unwrap();
    assert_eq!(voided.status, PaymentStatus::Voided);
    assert_eq!(event_types(&pool, "voided").await, ["PaymentAuthorized"]);
    assert_eq!(store.record_void("nobody").await.unwrap(), None);
}

#[tokio::test]
async fn a_lapsed_hold_cant_be_captured_but_can_be_voided() {
    let (_pg, _pool, store) = setup().await;
    authorized(&store, "o1", time::Duration::seconds(-1)).await;
    assert_eq!(store.record_capture("o1").await.unwrap(), None);
    assert_eq!(
        store.record_void("o1").await.unwrap().unwrap().status,
        PaymentStatus::Voided
    );
}

#[tokio::test]
async fn the_sweep_voids_lapsed_holds_with_payment_authorization_expired() {
    let (_pg, pool, store) = setup().await;
    authorized(&store, "lapsed", time::Duration::seconds(-1)).await;
    authorized(&store, "live", time::Duration::days(7)).await;
    let expired = store.expire_due(100).await.unwrap();
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].order_id, "lapsed");
    assert_eq!(
        (expired[0].status, expired[0].decline_reason.as_deref()),
        (PaymentStatus::Voided, Some(AUTHORIZATION_EXPIRED))
    );
    assert_eq!(
        event_types(&pool, "lapsed").await,
        ["PaymentAuthorized", "PaymentAuthorizationExpired"]
    );
    assert_eq!(store.expire_due(100).await.unwrap().len(), 0);

    let pulled = store.expire_now("live").await.unwrap().unwrap();
    assert!(pulled.expires_at.unwrap() <= OffsetDateTime::now_utc());
    assert_eq!(store.expire_due(100).await.unwrap().len(), 1);
    assert_eq!(store.expire_now("live").await.unwrap(), None);
}

#[tokio::test]
async fn a_flag_needs_a_payment_and_the_first_wins_and_resolves() {
    let (_pg, _pool, store) = setup().await;
    assert!(!store.flag("nobody", "void_after_capture").await.unwrap());
    store.create("o1", 1).await.unwrap();
    store.create("o2", 1).await.unwrap();
    assert!(store.flag("o1", "void_after_capture").await.unwrap());
    assert!(!store.flag("o1", "capture_after_void").await.unwrap());
    assert!(store.flag("o2", "capture_after_void").await.unwrap());

    let all = store.reconciliations(None, None).await.unwrap();
    assert_eq!(
        all.iter().map(|f| f.order_id.as_str()).collect::<Vec<_>>(),
        ["o2", "o1"]
    );
    assert_eq!(all[1].reason, "void_after_capture");
    let only = store.reconciliations(None, Some("o1")).await.unwrap();
    assert_eq!(only.len(), 1);

    let at = OffsetDateTime::now_utc();
    let ResolveOutcome::Resolved(first) =
        store.resolve("o1", "ops", Some("done"), at).await.unwrap()
    else {
        panic!("not resolved");
    };
    assert_eq!(
        (
            first.status,
            first.operator_id.as_deref(),
            first.note.as_deref()
        ),
        (ReconciliationStatus::Resolved, Some("ops"), Some("done"))
    );
    assert_eq!(
        store.resolve("o1", "other", None, at).await.unwrap(),
        ResolveOutcome::AlreadyResolved(first)
    );
    assert_eq!(
        store.resolve("nobody", "ops", None, at).await.unwrap(),
        ResolveOutcome::NotFound
    );
    let open = store
        .reconciliations(Some(ReconciliationStatus::Open), None)
        .await
        .unwrap();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].order_id, "o2");
}

#[tokio::test]
async fn the_ping_follows_the_database() {
    let (postgres, _pool, store) = setup().await;
    assert!(store.ping(Duration::from_secs(2)).await);
    postgres.stop();
    assert!(!store.ping(Duration::from_secs(2)).await);
    assert!(store.by_order("o1").await.is_err());
}
