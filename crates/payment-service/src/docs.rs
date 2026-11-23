// SPDX-License-Identifier: MIT

//! The service's API description: what tapir generated from the Scala
//! endpoints (capture 20261123 openapi.yaml), stated once here next to the
//! DTO types it derives the schemas from, served with Swagger UI (PAY-D1); and
//! the AsyncAPI document of what it publishes through the outbox and the two
//! topics it consumes, with its page (PAY-D2). The operator routes' optional
//! `Authorization` header parameter is not listed (the builder has path and
//! query parameters only).

use axum::Router;
use serde_json::{Value, json};
use telemetry::docs::asyncapi::{self, Channel};
use telemetry::docs::openapi::{Api, Operation};
use telemetry::docs::swagger;

use crate::domain::Address;
use crate::events::{
    DISPATCH_CONFIRMED, DISPATCH_FAILED, DispatchConfirmed, DispatchFailed, FULFILMENT_EVENTS,
    ORDER_CANCELLED, ORDER_EVENTS, OrderCancelled,
};
use crate::published::{
    PAYMENT_AUTHORIZATION_EXPIRED, PAYMENT_AUTHORIZED, PAYMENT_CAPTURED, PAYMENT_FAILED,
    PaymentAuthorizationExpired, PaymentAuthorized, PaymentCaptured, PaymentFailed, TOPIC,
};
use crate::routes::{
    AuthorizePaymentRequest, AuthorizePaymentResponse, CreatePaymentMethodRequest,
    CreatePaymentRequest, DeclinedResponse, ErrorResponse, PaymentMethodResponse, PaymentResponse,
    ReconciliationResponse, ResolveReconciliationRequest,
};

const PAYMENT: &str = "9b2f1c3d-4e5a-4b6c-8d7e-9f0a1b2c3d4e";
const ORDER: &str = "3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f";
const METHOD: &str = "5e6f7a8b-9c0d-4e1f-8a2b-3c4d5e6f7a8b";
const FLAGGED: &str = "3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90";
const OPERATOR: &str = "The dev operator token is absent, malformed or does not match.";

fn payment(status: &str, updated_at: &str) -> Value {
    json!({
        "id": PAYMENT,
        "orderId": ORDER,
        "amountCents": 4999,
        "status": status,
        "createdAt": "2026-11-01T12:00:00Z",
        "updatedAt": updated_at,
    })
}

fn flag() -> Value {
    json!({
        "orderId": FLAGGED,
        "reason": "capture_after_void",
        "status": "open",
        "createdAt": "2026-11-02T10:15:30Z",
    })
}

/// A JSON body: its schema component and example. (Always `Some`: the
/// builder takes `None` for a response without one.)
#[allow(clippy::unnecessary_wraps)]
fn body(schema: &'static str, example: Value) -> Option<(&'static str, Value)> {
    Some((schema, example))
}

#[allow(clippy::unnecessary_wraps)]
fn error(message: &str) -> Option<(&'static str, Value)> {
    Some(("ErrorResponse", json!({ "error": message })))
}

fn by_id(operation: Operation, summary: &str, description: &str) -> Operation {
    operation
        .tag("payments")
        .summary(summary)
        .description(description)
        .path_parameter("id", "Payment id (UUID).", json!(PAYMENT))
        .response(404, "No payment with this id.", error("Payment not found"))
}

fn payment_operations(api: Api) -> Api {
    let invalid = "`orderId` is blank or `amountCents` isn't positive.";
    api.operation(
        Operation::post("/payments")
            .tag("payments")
            .summary("Create a payment")
            .description(
                "Records a new payment in `pending` status for an order. `orderId` is unique: \
                 a second payment for the same order is `409`. Does not charge, authorize or \
                 publish an event — that is `POST /payments/authorize`.",
            )
            .request_body(
                "CreatePaymentRequest",
                json!({"orderId": ORDER, "amountCents": 4999}),
            )
            .response(
                201,
                "",
                body(
                    "PaymentResponse",
                    payment("pending", "2026-11-01T12:00:00Z"),
                ),
            )
            .response(400, invalid, error("orderId is required"))
            .response(
                409,
                "The order already has a payment.",
                error(&format!("Order {ORDER} already has a payment")),
            ),
    )
    .operation(
        by_id(
            Operation::get("/payments/{id}"),
            "Get a payment",
            "One payment by its UUID. Read-only.",
        )
        .response(
            200,
            "",
            body(
                "PaymentResponse",
                payment("pending", "2026-11-01T12:00:00Z"),
            ),
        ),
    )
    .operation(
        by_id(
            Operation::delete("/payments/{id}"),
            "Delete a payment",
            "Hard-deletes the payment record. No event published. `404` when the id is unknown.",
        )
        .response(204, "", None),
    )
}

fn method_operations(api: Api) -> Api {
    api.operation(
        Operation::post("/payments/methods")
            .tag("payment-methods")
            .summary("Create a payment method")
            .description(
                "Validates the card (length, digits, Luhn, expiry) and billing address, then \
                 stores only a simulated token, last four digits, brand and expiry. The PAN is \
                 never stored, logged or echoed back. Not idempotent — each call creates a new \
                 method.",
            )
            .request_body(
                "CreatePaymentMethodRequest",
                json!({
                    "customerId": "c1f2a3b4-5d6e-4f70-8a9b-0c1d2e3f4a5b",
                    "cardNumber": "4242 4242 4242 4242",
                    "expiry": "12/99",
                    "billingAddress": {
                        "line1": "1 High Street",
                        "line2": "Flat 2",
                        "city": "London",
                        "postalCode": "SW1A 1AA",
                        "country": "GB",
                    },
                }),
            )
            .response(
                201,
                "",
                body(
                    "PaymentMethodResponse",
                    json!({"id": METHOD, "last4": "4242", "brand": "visa", "expiry": "12/99"}),
                ),
            )
            .response(
                400,
                "`customerId` is blank, the card fails validation (length, digits, Luhn, or an \
                 expiry that isn't `MM/YY` or is in the past), or the billing address is invalid.",
                error("Invalid card number"),
            ),
    )
}

fn authorize_operations(api: Api) -> Api {
    let invalid = "`orderId` is blank or `amountCents` isn't positive.";
    api.operation(
        Operation::post("/payments/authorize")
            .tag("payments")
            .summary("Authorize a payment")
            .description(
                "Places a hold for an order and records it as `authorized`, publishing \
                 `PaymentAuthorized` via the outbox. A repeated identical request replays the \
                 stored authorization (same `authId`/`expiresAt`, no second hold); a different \
                 amount or payment method for the same order is `409`; a replayed decline is \
                 `402` with the original reason. Declines are synchronous and publish nothing.",
            )
            .request_body(
                "AuthorizePaymentRequest",
                json!({"orderId": ORDER, "amountCents": 4999, "paymentMethodId": METHOD}),
            )
            .response(
                200,
                "",
                body(
                    "AuthorizePaymentResponse",
                    json!({
                        "authId": "auth_2f1c3b4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
                        "expiresAt": "2026-11-08T12:00:00Z",
                    }),
                ),
            )
            .response(400, invalid, error("amountCents must be positive"))
            .response(
                402,
                "The provider declined the hold.",
                body(
                    "DeclinedResponse",
                    json!({"error": "Payment declined", "reason": "insufficient_funds"}),
                ),
            )
            .response(
                404,
                "No payment method with this id.",
                error("Payment method not found"),
            )
            .response(
                409,
                "The order already has a payment with a different amount or payment method, \
                 its stored authorization has expired, or it is `pending`, `captured` or \
                 `voided`.",
                error("Order already has a payment with a different amount or payment method"),
            )
            .response(
                500,
                "The stored payment is inconsistent (an authorization missing \
                 `authId`/`expiresAt`, or a decline with no reason), or the payment couldn't be \
                 recorded or read back.",
                error("Stored authorization is incomplete"),
            ),
    )
    .operation(
        Operation::post("/payments/authorizations/{orderId}/expire")
            .tag("payments")
            .summary("Expire an authorization now (dev/demo)")
            .description(
                "Simulates authorization expiry for one order (ADM-5): pulls the hold's expiry \
                 to now and runs one sweep tick, so the payment is voided and \
                 `PaymentAuthorizationExpired` is published through the normal US-20.1 path. \
                 Requires the dev operator token (401). 404 when the order has no payment, 409 \
                 when it is not `authorized`.",
            )
            .path_parameter(
                "orderId",
                "Order id whose authorization to expire.",
                json!(ORDER),
            )
            .response(
                200,
                "",
                body("PaymentResponse", payment("voided", "2026-11-02T09:30:00Z")),
            )
            .response(401, OPERATOR, error("Invalid operator token"))
            .response(404, "The order has no payment.", error("Payment not found"))
            .response(
                409,
                "The payment is not a live authorization (already captured, voided or failed).",
                error("Payment is captured, not authorized"),
            ),
    )
}

fn reconciliation_operations(api: Api) -> Api {
    let only = "Only `resolved` is accepted.";
    api.operation(
        Operation::get("/reconciliations")
            .tag("reconciliations")
            .summary("List reconciliation flags")
            .description(
                "The manual-reconciliation queue, newest first. Both filters are optional and \
                 ANDed; an unknown status → 400 and an unknown orderId returns an empty list. \
                 Unauthenticated local/demo read (ADM-4).",
            )
            .query_parameter("status", "open or resolved (default: both).", json!("open"))
            .query_parameter("orderId", "Only this order's flag.", json!(FLAGGED))
            .response(200, "", body("ReconciliationResponse[]", json!([flag()])))
            .response(400, only, error("Invalid reconciliation status: 'open'")),
    )
    .operation(
        Operation::patch("/reconciliations/{orderId}")
            .tag("reconciliations")
            .summary("Resolve a reconciliation flag")
            .description(
                "Marks an open flag resolved, recording the operator id from the token, an \
                 optional note and the timestamp. The first resolver wins: a repeat is an \
                 idempotent 200 and never overwrites the original. 401 without a valid token, \
                 404 when no flag exists.",
            )
            .path_parameter("orderId", "Order id whose flag to resolve.", json!(FLAGGED))
            .request_body(
                "ResolveReconciliationRequest",
                json!({"status": "resolved", "note": "Refunded manually"}),
            )
            .response(200, "", body("ReconciliationResponse", flag()))
            .response(400, only, error("Invalid reconciliation status: 'open'"))
            .response(401, OPERATOR, error("Invalid operator token"))
            .response(
                404,
                "No flag for this order.",
                error("Reconciliation flag not found"),
            ),
    )
}

/// The OpenAPI document: payments, methods, the expiry trigger,
/// reconciliations, then the two health checks.
#[must_use]
pub fn api() -> Api {
    let api = Api::new("Payment Service", "1.0")
        .schema::<Address>()
        .schema::<AuthorizePaymentRequest>()
        .schema::<AuthorizePaymentResponse>()
        .schema::<CreatePaymentMethodRequest>()
        .schema::<CreatePaymentRequest>()
        .schema::<DeclinedResponse>()
        .schema::<ErrorResponse>()
        .schema::<PaymentMethodResponse>()
        .schema::<PaymentResponse>()
        .schema::<ReconciliationResponse>()
        .schema::<ResolveReconciliationRequest>();
    reconciliation_operations(authorize_operations(method_operations(payment_operations(
        api,
    ))))
    .operation(
        Operation::get("/health")
            .tag("health")
            .summary("Liveness probe")
            .description("`200` while the process is up. Polled by Kubernetes.")
            .response(200, "", None),
    )
    .operation(
        Operation::get("/health/ready")
            .tag("health")
            .summary("Readiness probe")
            .description("`200` when Postgres is reachable, `503` otherwise. Polled by Kubernetes.")
            .response(200, "", None)
            .response(503, "Postgres is unreachable.", None),
    )
}

/// The AsyncAPI document: the four `payment-events` sent, `DispatchConfirmed`
/// and `DispatchFailed` and `OrderCancelled` received.
#[must_use]
pub fn asyncapi() -> Value {
    asyncapi::document(
        "Payment Service",
        "1.0",
        &[
            Channel::publish_cdc::<PaymentAuthorized>(TOPIC, PAYMENT_AUTHORIZED),
            Channel::publish_cdc::<PaymentCaptured>(TOPIC, PAYMENT_CAPTURED),
            Channel::publish_cdc::<PaymentAuthorizationExpired>(
                TOPIC,
                PAYMENT_AUTHORIZATION_EXPIRED,
            ),
            Channel::publish_cdc::<PaymentFailed>(TOPIC, PAYMENT_FAILED),
            Channel::subscribe::<DispatchFailed>(FULFILMENT_EVENTS, DISPATCH_FAILED),
            Channel::subscribe::<DispatchConfirmed>(FULFILMENT_EVENTS, DISPATCH_CONFIRMED),
            Channel::subscribe::<OrderCancelled>(ORDER_EVENTS, ORDER_CANCELLED),
        ],
    )
}

/// Swagger UI at `/docs/` (`/docs/docs.yaml`), and the AsyncAPI document and page.
pub fn router() -> Router {
    swagger::router(api().to_yaml()).merge(asyncapi::router_with_page(&asyncapi()))
}
