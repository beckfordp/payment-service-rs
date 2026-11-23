// SPDX-License-Identifier: MIT

//! What the service publishes through the outbox on [`TOPIC`] (Scala
//! `PaymentEvents`), each written in the transaction that records the change
//! (`store.rs`), keyed and correlated by the order id.

use schemars::JsonSchema;
use serde::Serialize;

/// The topic Debezium routes the outbox to.
pub const TOPIC: &str = "payment-events";

/// The service's name as the envelope's `source`.
pub const SOURCE: &str = "payment-service";

/// A hold was placed.
pub const PAYMENT_AUTHORIZED: &str = "PaymentAuthorized";
/// The hold was captured.
pub const PAYMENT_CAPTURED: &str = "PaymentCaptured";
/// The hold lapsed and was voided.
pub const PAYMENT_AUTHORIZATION_EXPIRED: &str = "PaymentAuthorizationExpired";
/// The capture was declined.
pub const PAYMENT_FAILED: &str = "PaymentFailed";

/// Payment authorized: a hold was placed on the customer's card.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "PaymentAuthorizedEvent")]
pub struct PaymentAuthorized {
    /// Order id the payment is for.
    pub order_id: String,
    /// Gateway authorization reference.
    pub auth_id: String,
    /// When the hold lapses (ISO-8601 instant).
    pub expires_at: String,
    /// When the authorization was recorded (ISO-8601 instant).
    pub timestamp: String,
}

/// Payment captured: the money moved, on dispatch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "PaymentCapturedEvent")]
pub struct PaymentCaptured {
    /// Order id the payment is for.
    pub order_id: String,
    /// Amount captured, in cents.
    pub amount_cents: i32,
    /// When the capture was recorded (ISO-8601 instant).
    pub timestamp: String,
}

/// Payment authorization expired: the hold lapsed before capture and was voided.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "PaymentAuthorizationExpiredEvent")]
pub struct PaymentAuthorizationExpired {
    /// Order id the payment is for.
    pub order_id: String,
    /// When the expiry was recorded (ISO-8601 instant).
    pub timestamp: String,
}

/// Payment failed: the capture was declined.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "PaymentFailedEvent")]
pub struct PaymentFailed {
    /// Order id the payment is for.
    pub order_id: String,
    /// Payment id (UUID).
    pub payment_id: String,
    /// Amount that failed to capture, in cents.
    pub amount_cents: i32,
    /// Why: `capture_failed`.
    pub reason: String,
    /// When the failure was recorded (ISO-8601 instant).
    pub timestamp: String,
}
