// SPDX-License-Identifier: MIT

//! The consumers' routes, as the Scala `OrderCancelledConsumer` and
//! `FulfilmentEventConsumer`: `OrderCancelled` and `DispatchFailed` void the
//! hold, `DispatchConfirmed` captures it (a declined capture records
//! `PaymentFailed`); a payment found in another state is skipped, or flagged
//! for reconciliation when money moved the other way, and a transition lost to
//! a concurrent one is classified as Scala's `PaymentRace` does. On
//! `order-events` other event types are skipped; on `fulfilment-events` they
//! are dead-lettered, as Scala does. The rest of the per-record decision
//! (undecodable envelopes and payloads to the DLQ, the DLQ guard) is gluon-rs
//! `kafka-event-consumer`'s `handle_record`.

use kafka_event_consumer::envelope::{Envelope, Routes};
use schemars::JsonSchema;
use serde::Deserialize;
use telemetry::logging::Logger;
use time::OffsetDateTime;

use crate::domain::{
    CAPTURE_AFTER_FAILED, CAPTURE_AFTER_VOID, CAPTURE_FAILED, DISPATCH_AFTER_EXPIRY, OrderId,
    Payment, PaymentStatus, Race, SimulatedGateway, VOID_AFTER_CAPTURE, is_live, race,
};
use crate::store::{Payments, StoreError};

/// The topic `OrderCancelled` comes on.
pub const ORDER_EVENTS: &str = "order-events";
/// Its consumer group.
pub const ORDER_GROUP: &str = "payment-service-order-events";
/// The topic `DispatchConfirmed` and `DispatchFailed` come on.
pub const FULFILMENT_EVENTS: &str = "fulfilment-events";
/// Its consumer group.
pub const FULFILMENT_GROUP: &str = "payment-service-fulfilment-events";

/// The event type routed on `order-events`.
pub const ORDER_CANCELLED: &str = "OrderCancelled";
/// Dispatch succeeded: capture.
pub const DISPATCH_CONFIRMED: &str = "DispatchConfirmed";
/// Dispatch failed: void.
pub const DISPATCH_FAILED: &str = "DispatchFailed";

/// Order cancelled: a customer cancel or a consumed `PaymentAuthorizationExpired`
/// moved the order to cancelled.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "OrderCancelledEvent")]
pub struct OrderCancelled {
    /// Order UUID (order-service's order id).
    pub order_id: String,
    /// Customer who placed the order.
    pub customer_id: String,
    /// When the order was cancelled (ISO-8601 instant).
    pub timestamp: String,
    /// Cancellation trigger: customer | `auth_expired` | stale | operator.
    pub reason: Option<String>,
}

/// Dispatch failed: pick/pack/dispatch hit an error. payment-service voids the
/// hold; order-service marks the order failed.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "DispatchFailedEvent")]
pub struct DispatchFailed {
    /// Order UUID (order-service's order id).
    pub order_id: String,
    /// Why dispatch failed.
    pub reason: String,
}

/// One dispatched line.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(inline)]
pub struct FulfilmentItem {
    /// Product SKU.
    pub sku: String,
    /// Product name at dispatch time.
    pub product_name: String,
    /// Units dispatched.
    pub quantity: i32,
    /// Unit price, in cents.
    pub unit_price_cents: i32,
}

/// Order dispatched: pick/pack/dispatch succeeded. Carries a full item
/// snapshot so consumers need no order lookup.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "DispatchConfirmedEvent")]
pub struct DispatchConfirmed {
    /// Order UUID (order-service's order id).
    pub order_id: String,
    /// Carrier tracking number.
    pub tracking_no: String,
    /// Items dispatched. Always sent.
    // Optional in the schema, as tapir states it; required to decode, as circe does.
    pub items: Option<Vec<FulfilmentItem>>,
}

/// Why an event wasn't handled: logged, retried and dead-lettered by the DLQ guard.
#[derive(Debug, thiserror::Error)]
pub enum EventsError {
    /// The payload isn't the event's (poison).
    #[error(transparent)]
    Payload(#[from] serde_json::Error),
    /// A `DispatchConfirmed` without its items (poison).
    #[error("Missing required field at 'items'")]
    NoItems,
    /// The payload's orderId is blank (poison).
    #[error("{0}")]
    OrderId(String),
    /// An event type `fulfilment-events` doesn't carry (poison).
    #[error("Unknown fulfilment-events event type '{0}'")]
    Unknown(String),
    /// A stored payment that can't be acted on.
    #[error("{0}")]
    Inconsistent(String),
    /// The store failed.
    #[error(transparent)]
    Store(#[from] StoreError),
}

fn decoded<T: serde::de::DeserializeOwned>(
    envelope: &Envelope,
    order_id: impl Fn(&T) -> &str,
) -> Result<(OrderId, T), EventsError> {
    let event: T = serde_json::from_value(envelope.payload.clone())?;
    let id = OrderId::parse(order_id(&event)).map_err(EventsError::OrderId)?;
    Ok((id, event))
}

fn unrecorded(order_id: &OrderId, target: PaymentStatus) -> EventsError {
    EventsError::Inconsistent(format!(
        "Payment for order {} was not authorized when recording {}",
        order_id.as_str(),
        target.as_str()
    ))
}

fn no_auth_id(payment: &Payment) -> EventsError {
    EventsError::Inconsistent(format!("Authorized payment {} has no authId", payment.id))
}

/// What both consumers share: the store, the gateway and the log.
#[derive(Debug, Clone)]
struct Handler<S> {
    store: S,
    gateway: SimulatedGateway,
    logger: Logger,
}

impl<S: Payments> Handler<S> {
    /// A transition matched nothing: classify what it lost to (`PaymentRace.afterNone`).
    async fn after_none(
        &self,
        order_id: &OrderId,
        target: PaymentStatus,
        event: &str,
    ) -> Result<(), EventsError> {
        let Some(payment) = self.store.by_order(order_id.as_str()).await? else {
            return Err(unrecorded(order_id, target));
        };
        let context = [
            ("order_id", order_id.as_str()),
            ("event", event),
            ("current_status", payment.status.as_str()),
            ("target", target.as_str()),
        ];
        match race(target, payment.status) {
            Race::Duplicate => self.logger.info(
                &context,
                "Payment already in the requested state - duplicate",
            ),
            Race::Benign => self.logger.info(
                &context,
                "Late event for a terminal non-capture state - ignoring",
            ),
            Race::Lost(reason) => {
                let mut fields = context.to_vec();
                fields.push(("reconciliation", reason));
                self.logger
                    .warn(&fields, "Payment transition lost - reconciliation flagged");
                self.store.flag(order_id.as_str(), reason).await?;
            }
            Race::Unexpected => return Err(unrecorded(order_id, target)),
        }
        Ok(())
    }

    /// Voids the order's hold (`OrderCancelled`, `DispatchFailed`).
    async fn void(
        &self,
        order_id: &OrderId,
        event: &str,
        reason: Option<&str>,
    ) -> Result<(), EventsError> {
        let id = order_id.as_str();
        let reason = reason.unwrap_or("-");
        let Some(payment) = self.store.by_order(id).await? else {
            self.logger.warn(
                &[("order_id", id)],
                &format!("{event} for an unknown payment - skipping"),
            );
            return Ok(());
        };
        let status = payment.status.as_str();
        match payment.status {
            PaymentStatus::Voided => self.logger.info(
                &[("order_id", id), ("reason", reason)],
                &format!("{event} for an already-voided payment - skipping duplicate"),
            ),
            PaymentStatus::Captured => {
                self.logger.warn(
                    &[("order_id", id), ("status", status), ("reason", reason)],
                    &format!("{event} after capture - reconciliation flagged"),
                );
                self.store.flag(id, VOID_AFTER_CAPTURE).await?;
            }
            PaymentStatus::Authorized => {
                if payment.auth_id.is_none() {
                    return Err(no_auth_id(&payment));
                }
                // The simulated gateway's void always succeeds.
                if self.store.record_void(id).await?.is_some() {
                    let message = if event == ORDER_CANCELLED {
                        "Authorization voided on order cancellation"
                    } else {
                        "Authorization voided"
                    };
                    self.logger
                        .info(&[("order_id", id), ("reason", reason)], message);
                } else {
                    self.after_none(order_id, PaymentStatus::Voided, event)
                        .await?;
                }
            }
            PaymentStatus::Pending | PaymentStatus::Failed => self.logger.warn(
                &[("order_id", id), ("status", status), ("reason", reason)],
                &format!("{event} for a non-authorized payment - skipping"),
            ),
        }
        Ok(())
    }

    /// Captures the order's hold (`DispatchConfirmed`).
    async fn capture(&self, order_id: &OrderId) -> Result<(), EventsError> {
        let id = order_id.as_str();
        let Some(payment) = self.store.by_order(id).await? else {
            self.logger.warn(
                &[("order_id", id)],
                "DispatchConfirmed for an unknown payment - skipping",
            );
            return Ok(());
        };
        let status = payment.status.as_str();
        let flag = |reason: &'static str, message: &str| {
            self.logger
                .warn(&[("order_id", id), ("status", status)], message);
            self.store.flag(id, reason)
        };
        match payment.status {
            PaymentStatus::Captured => self.logger.info(
                &[("order_id", id)],
                "DispatchConfirmed for an already-captured payment - skipping duplicate",
            ),
            PaymentStatus::Voided => {
                flag(
                    CAPTURE_AFTER_VOID,
                    "Capture after void - reconciliation flagged",
                )
                .await?;
            }
            PaymentStatus::Failed => {
                flag(
                    CAPTURE_AFTER_FAILED,
                    "Capture after a failed payment - reconciliation flagged",
                )
                .await?;
            }
            PaymentStatus::Pending => self.logger.warn(
                &[("order_id", id), ("status", status)],
                "DispatchConfirmed for a non-authorized payment - skipping",
            ),
            PaymentStatus::Authorized => self.capture_authorized(order_id, &payment).await?,
        }
        Ok(())
    }

    async fn capture_authorized(
        &self,
        order_id: &OrderId,
        payment: &Payment,
    ) -> Result<(), EventsError> {
        let id = order_id.as_str();
        if payment.auth_id.is_none() {
            return Err(no_auth_id(payment));
        }
        let method_id = payment.payment_method_id.ok_or_else(|| {
            EventsError::Inconsistent(format!(
                "Authorized payment {} has no paymentMethodId",
                payment.id
            ))
        })?;
        let method = self.store.method(method_id).await?.ok_or_else(|| {
            EventsError::Inconsistent(format!(
                "Authorized payment {} references unknown payment method {method_id}",
                payment.id
            ))
        })?;
        if !is_live(payment.expires_at, OffsetDateTime::now_utc()) {
            self.store.flag(id, DISPATCH_AFTER_EXPIRY).await?;
            self.logger.warn(
                &[("order_id", id)],
                "Capture after authorization expiry - reconciliation flagged",
            );
            return Ok(());
        }
        match self.gateway.capture(&method.card_token) {
            Ok(()) => {
                if self.store.record_capture(id).await?.is_some() {
                    self.logger.info(&[("order_id", id)], "Payment captured");
                } else {
                    self.after_none(order_id, PaymentStatus::Captured, DISPATCH_CONFIRMED)
                        .await?;
                }
            }
            Err(decline) => {
                self.logger.warn(
                    &[("order_id", id), ("reason", &decline)],
                    "Capture declined - recording PaymentFailed",
                );
                if self
                    .store
                    .record_capture_failure(id, CAPTURE_FAILED)
                    .await?
                    .is_none()
                {
                    self.after_none(order_id, PaymentStatus::Failed, DISPATCH_CONFIRMED)
                        .await?;
                }
            }
        }
        Ok(())
    }
}

/// `order-events`: `OrderCancelled` → void.
#[derive(Debug, Clone)]
pub struct OrderEvents<S>(Handler<S>);

impl<S> OrderEvents<S> {
    /// Over `store` and `gateway`, logging to `logger`.
    #[must_use]
    pub fn new(store: S, gateway: SimulatedGateway, logger: Logger) -> Self {
        Self(Handler {
            store,
            gateway,
            logger,
        })
    }
}

impl<S: Payments> Routes for OrderEvents<S> {
    type Event = (OrderId, OrderCancelled);
    type Error = EventsError;

    fn decode(&self, envelope: &Envelope) -> Option<Result<Self::Event, EventsError>> {
        (envelope.event_type == ORDER_CANCELLED)
            .then(|| decoded(envelope, |e: &OrderCancelled| e.order_id.as_str()))
    }

    async fn handle(
        &self,
        _event_id: &str,
        (order_id, event): &Self::Event,
    ) -> Result<(), EventsError> {
        self.0
            .void(order_id, ORDER_CANCELLED, event.reason.as_deref())
            .await
    }
}

/// A `fulfilment-events` event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FulfilmentEvent {
    /// Capture.
    Confirmed(DispatchConfirmed),
    /// Void.
    Failed(DispatchFailed),
}

/// `fulfilment-events`: `DispatchConfirmed` → capture, `DispatchFailed` → void.
#[derive(Debug, Clone)]
pub struct FulfilmentEvents<S>(Handler<S>);

impl<S> FulfilmentEvents<S> {
    /// Over `store` and `gateway`, logging to `logger`.
    #[must_use]
    pub fn new(store: S, gateway: SimulatedGateway, logger: Logger) -> Self {
        Self(Handler {
            store,
            gateway,
            logger,
        })
    }
}

impl<S: Payments> Routes for FulfilmentEvents<S> {
    type Event = (OrderId, FulfilmentEvent);
    type Error = EventsError;

    fn decode(&self, envelope: &Envelope) -> Option<Result<Self::Event, EventsError>> {
        Some(match envelope.event_type.as_str() {
            DISPATCH_CONFIRMED => decoded(envelope, |e: &DispatchConfirmed| e.order_id.as_str())
                .and_then(|(id, event)| {
                    if event.items.is_some() {
                        Ok((id, FulfilmentEvent::Confirmed(event)))
                    } else {
                        Err(EventsError::NoItems)
                    }
                }),
            DISPATCH_FAILED => decoded(envelope, |e: &DispatchFailed| e.order_id.as_str())
                .map(|(id, event)| (id, FulfilmentEvent::Failed(event))),
            other => Err(EventsError::Unknown(other.to_owned())),
        })
    }

    async fn handle(
        &self,
        _event_id: &str,
        (order_id, event): &Self::Event,
    ) -> Result<(), EventsError> {
        match event {
            FulfilmentEvent::Confirmed(_) => self.0.capture(order_id).await,
            FulfilmentEvent::Failed(failed) => {
                self.0
                    .void(order_id, DISPATCH_FAILED, Some(&failed.reason))
                    .await
            }
        }
    }
}
