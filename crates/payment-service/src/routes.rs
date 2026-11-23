// SPDX-License-Identifier: MIT

//! The routes, as the Scala `PaymentMethodRoutes`, `PaymentRoutes`,
//! `AuthorizeRoutes`, `ExpiryTriggerRoutes` and `ReconciliationRoutes`: the
//! path and body decoded first (tapir's plain-text 400), then the rules (400
//! with the JSON reason), then the store and the gateway (402, 404, 409). A
//! store failure is a 500.

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use service_basics::jvm::java_instant;
use service_basics::text_error::TextError;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::consumers::sweep_once;
use crate::domain::{
    Address, OrderId, Payment, PaymentMethod, PaymentStatus, Reconciliation, ReconciliationStatus,
    ResolveOutcome, is_live, method_rules, parse_uuid, payment_rules,
};
use crate::server::AppState;
use crate::store::{Payments, StoreError};

/// A card to tokenize, with its billing address.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreatePaymentMethodRequest {
    /// Customer the method belongs to.
    pub customer_id: String,
    /// Card number, 13–19 digits (spaces allowed); never stored.
    pub card_number: String,
    /// Expiry, `MM/YY`.
    pub expiry: String,
    /// Billing address.
    pub billing_address: Address,
}

/// A stored payment method: only the last four digits come back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PaymentMethodResponse {
    /// Payment method id (UUID).
    pub id: String,
    /// The card's last four digits.
    pub last4: String,
    /// `visa`, `mastercard`, `amex` or `other`.
    pub brand: String,
    /// Expiry, `MM/YY`.
    pub expiry: String,
}

/// A payment to record, pending.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreatePaymentRequest {
    /// Order id the payment is for; one payment per order.
    pub order_id: String,
    /// Amount to charge, in cents, > 0.
    pub amount_cents: i32,
}

/// A payment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PaymentResponse {
    /// Payment id (UUID).
    pub id: String,
    /// Order id the payment is for.
    pub order_id: String,
    /// Amount to charge, in cents.
    pub amount_cents: i32,
    /// Lifecycle status: `pending`, `authorized`, `captured`, `failed` or `voided`.
    pub status: String,
    /// When it was recorded (UTC).
    pub created_at: String,
    /// When it last changed (UTC).
    pub updated_at: String,
}

impl From<Payment> for PaymentResponse {
    fn from(payment: Payment) -> Self {
        Self {
            id: payment.id.to_string(),
            order_id: payment.order_id,
            amount_cents: payment.amount_cents,
            status: payment.status.as_str().to_owned(),
            created_at: java_instant(payment.created_at),
            updated_at: java_instant(payment.updated_at),
        }
    }
}

/// A hold to place for an order on a stored method.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizePaymentRequest {
    /// Order id; the idempotency key.
    pub order_id: String,
    /// Amount to hold, in cents, > 0.
    pub amount_cents: i32,
    /// Payment method id (UUID).
    pub payment_method_id: String,
}

/// The hold placed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizePaymentResponse {
    /// Gateway authorization reference.
    pub auth_id: String,
    /// When the hold lapses (UTC).
    pub expires_at: String,
}

/// The provider declined the hold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, schemars::JsonSchema)]
pub struct DeclinedResponse {
    /// Always `Payment declined`.
    pub error: String,
    /// The provider's reason, e.g. `insufficient_funds`.
    pub reason: String,
}

/// One manual-reconciliation flag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationResponse {
    /// Order the flag belongs to.
    pub order_id: String,
    /// Why manual reconciliation is needed.
    pub reason: String,
    /// `open` or `resolved`.
    pub status: String,
    /// When the flag was recorded.
    pub created_at: String,
    /// Operator who resolved it, when resolved.
    pub operator_id: Option<String>,
    /// Operator's note, when supplied.
    pub note: Option<String>,
    /// When it was resolved.
    pub resolved_at: Option<String>,
}

impl From<Reconciliation> for ReconciliationResponse {
    fn from(flag: Reconciliation) -> Self {
        Self {
            order_id: flag.order_id,
            reason: flag.reason,
            status: flag.status.as_str().to_owned(),
            created_at: java_instant(flag.created_at),
            operator_id: flag.operator_id,
            note: flag.note,
            resolved_at: flag.resolved_at.map(java_instant),
        }
    }
}

/// Resolve a reconciliation flag.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct ResolveReconciliationRequest {
    /// Only `resolved` is accepted.
    pub status: String,
    /// Optional operator note.
    pub note: Option<String>,
}

/// Error body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, schemars::JsonSchema)]
pub struct ErrorResponse {
    /// Human-readable reason.
    pub error: String,
}

/// The reconciliations' filters.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FilterQuery {
    /// An order.
    pub order_id: Option<String>,
    /// A status.
    pub status: Option<String>,
}

/// The reconciliation-status 400's reason, whatever status was asked for (as
/// tapir's single error variant maps it).
const ONLY_RESOLVED: &str = "Invalid reconciliation status: only 'resolved' is accepted";

/// Why a route didn't answer its success.
#[derive(Debug)]
pub enum RouteError {
    /// tapir's text 400: what didn't decode (`path parameter id`, `body (…)`).
    Decode(String),
    /// A rule's 400, a 401, 404, 409 or 500, with its JSON reason.
    Refused(StatusCode, String),
    /// 402: the provider declined, with its reason.
    Declined(String),
    /// Postgres failed.
    Store(StoreError),
}

impl From<StoreError> for RouteError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl IntoResponse for RouteError {
    fn into_response(self) -> Response {
        match self {
            Self::Decode(what) => text(
                StatusCode::BAD_REQUEST,
                format!("Invalid value for: {what}"),
            ),
            Self::Refused(status, error) => (status, Json(ErrorResponse { error })).into_response(),
            Self::Declined(reason) => (
                StatusCode::PAYMENT_REQUIRED,
                Json(DeclinedResponse {
                    error: "Payment declined".to_owned(),
                    reason,
                }),
            )
                .into_response(),
            Self::Store(_) => text(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".to_owned(),
            ),
        }
    }
}

fn text(status: StatusCode, message: String) -> Response {
    TextError { status, message }.into_response()
}

fn refused(status: StatusCode, reason: &str) -> RouteError {
    RouteError::Refused(status, reason.to_owned())
}

fn bad_request(reason: String) -> RouteError {
    RouteError::Refused(StatusCode::BAD_REQUEST, reason)
}

fn not_found() -> RouteError {
    refused(StatusCode::NOT_FOUND, "Payment not found")
}

fn payment_id(raw: &str) -> Result<Uuid, RouteError> {
    parse_uuid(raw).ok_or_else(|| RouteError::Decode("path parameter id".to_owned()))
}

fn body<T: serde::de::DeserializeOwned>(bytes: &Bytes) -> Result<T, RouteError> {
    serde_json::from_slice(bytes).map_err(|error| RouteError::Decode(format!("body ({error})")))
}

fn authorization(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
}

/// Logs a failure the way the Scala routes do, and passes it on.
fn logged<T, S>(state: &AppState<S>, result: Result<T, RouteError>) -> Result<T, RouteError> {
    match &result {
        Err(RouteError::Store(error)) => state
            .logger
            .error(&[("error", &error.to_string())], "Request failed"),
        Err(RouteError::Refused(_, reason)) => {
            state.logger.warn(&[("reason", reason)], "Request refused");
        }
        Err(RouteError::Declined(reason)) => {
            state.logger.warn(&[("reason", reason)], "Payment declined");
        }
        _ => {}
    }
    result
}

/// The month it is now (UTC), as (year, month).
fn this_month() -> (i32, u8) {
    let now = OffsetDateTime::now_utc();
    (now.year(), u8::from(now.month()))
}

/// `POST /payments/methods` (201): the card tokenized, its number dropped.
///
/// # Errors
/// An undecodable body or a broken rule (400); a store failure (500).
pub async fn add_method<S: Payments>(
    State(state): State<AppState<S>>,
    bytes: Bytes,
) -> Result<(StatusCode, Json<PaymentMethodResponse>), RouteError> {
    let request: CreatePaymentMethodRequest = body(&bytes)?;
    let result = async {
        let card = method_rules(
            &request.customer_id,
            &request.card_number,
            &request.expiry,
            &request.billing_address,
            this_month(),
        )
        .map_err(bad_request)?;
        let method = PaymentMethod {
            id: Uuid::new_v4(),
            customer_id: request.customer_id.clone(),
            card_token: card.token(),
            last4: card.last4,
            brand: card.brand,
            expiry: card.expiry,
            billing_address: request.billing_address.clone(),
        };
        state.store.add_method(&method).await?;
        Ok(PaymentMethodResponse {
            id: method.id.to_string(),
            last4: method.last4,
            brand: method.brand.as_str().to_owned(),
            expiry: method.expiry,
        })
    }
    .await;
    Ok((StatusCode::CREATED, Json(logged(&state, result)?)))
}

/// `POST /payments` (201): pending, one per order.
///
/// # Errors
/// An undecodable body or a broken rule (400); the order has a payment (409);
/// a store failure (500).
pub async fn create<S: Payments>(
    State(state): State<AppState<S>>,
    bytes: Bytes,
) -> Result<(StatusCode, Json<PaymentResponse>), RouteError> {
    let request: CreatePaymentRequest = body(&bytes)?;
    let result = async {
        let order = payment_rules(&request.order_id, request.amount_cents).map_err(bad_request)?;
        state
            .store
            .create(order.as_str(), request.amount_cents)
            .await?
            .ok_or_else(|| {
                refused(
                    StatusCode::CONFLICT,
                    &format!("Order {} already has a payment", order.as_str()),
                )
            })
    }
    .await;
    let payment = logged(&state, result)?;
    Ok((StatusCode::CREATED, Json(payment.into())))
}

/// `GET /payments/{id}`.
///
/// # Errors
/// A malformed id (400), no such payment (404), a store failure (500).
pub async fn get<S: Payments>(
    State(state): State<AppState<S>>,
    Path(raw): Path<String>,
) -> Result<Json<PaymentResponse>, RouteError> {
    let id = payment_id(&raw)?;
    let result = state
        .store
        .get(id)
        .await
        .map_err(RouteError::from)
        .and_then(|found| found.ok_or_else(not_found));
    Ok(Json(logged(&state, result)?.into()))
}

/// `DELETE /payments/{id}` (204).
///
/// # Errors
/// A malformed id (400), no such payment (404), a store failure (500).
pub async fn delete<S: Payments>(
    State(state): State<AppState<S>>,
    Path(raw): Path<String>,
) -> Result<StatusCode, RouteError> {
    let id = payment_id(&raw)?;
    let result = match state.store.delete(id).await {
        Ok(true) => Ok(StatusCode::NO_CONTENT),
        Ok(false) => Err(not_found()),
        Err(error) => Err(error.into()),
    };
    logged(&state, result)
}

/// The stored outcome for the order, replayed instead of placing a second
/// hold: a different amount or method is a 409, a decline its 402, an
/// expired hold a 409.
fn replay(
    existing: &Payment,
    amount_cents: i32,
    method_id: Uuid,
) -> Result<AuthorizePaymentResponse, RouteError> {
    if existing.amount_cents != amount_cents || existing.payment_method_id != Some(method_id) {
        return Err(refused(
            StatusCode::CONFLICT,
            "Order already has a payment with a different amount or payment method",
        ));
    }
    match existing.status {
        PaymentStatus::Authorized => match (&existing.auth_id, existing.expires_at) {
            (Some(_), Some(_)) if !is_live(existing.expires_at, OffsetDateTime::now_utc()) => {
                Err(refused(
                    StatusCode::CONFLICT,
                    "Order's stored authorization has expired",
                ))
            }
            (Some(auth_id), Some(expires_at)) => Ok(AuthorizePaymentResponse {
                auth_id: auth_id.clone(),
                expires_at: java_instant(expires_at),
            }),
            _ => Err(refused(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Stored authorization is incomplete",
            )),
        },
        PaymentStatus::Failed => existing.decline_reason.clone().map_or_else(
            || {
                Err(refused(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Stored decline has no reason",
                ))
            },
            |reason| Err(RouteError::Declined(reason)),
        ),
        status => Err(refused(
            StatusCode::CONFLICT,
            &format!("Order already has a payment in status {}", status.as_str()),
        )),
    }
}

/// A concurrent call recorded the order first: replay the winner (the
/// simulated gateway's void of this call's hold always succeeds).
async fn replay_winner<S: Payments>(
    state: &AppState<S>,
    order: &OrderId,
    amount_cents: i32,
    method_id: Uuid,
) -> Result<AuthorizePaymentResponse, RouteError> {
    match state.store.by_order(order.as_str()).await? {
        Some(existing) => replay(&existing, amount_cents, method_id),
        None => Err(refused(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Payment could not be recorded or read back",
        )),
    }
}

/// `POST /payments/authorize`: a hold for the order, idempotent per order.
///
/// # Errors
/// An undecodable body or a broken rule (400); a decline (402); no such method
/// (404); a conflicting or spent payment (409); an inconsistent stored payment
/// or a store failure (500).
pub async fn authorize<S: Payments>(
    State(state): State<AppState<S>>,
    bytes: Bytes,
) -> Result<Json<AuthorizePaymentResponse>, RouteError> {
    let request: AuthorizePaymentRequest = body(&bytes)?;
    let method_id = parse_uuid(&request.payment_method_id).ok_or_else(|| {
        RouteError::Decode(format!(
            "body (Invalid payment method id: '{}' at 'paymentMethodId')",
            request.payment_method_id
        ))
    })?;
    let amount = request.amount_cents;
    let result = async {
        let order = payment_rules(&request.order_id, amount).map_err(bad_request)?;
        let method = state
            .store
            .method(method_id)
            .await?
            .ok_or_else(|| refused(StatusCode::NOT_FOUND, "Payment method not found"))?;
        if let Some(existing) = state.store.by_order(order.as_str()).await? {
            return replay(&existing, amount, method_id);
        }
        match state
            .gateway
            .authorize(&method.card_token, OffsetDateTime::now_utc())
        {
            Ok(hold) => match state
                .store
                .record_authorization(order.as_str(), amount, method_id, &hold)
                .await?
            {
                Some(_) => Ok(AuthorizePaymentResponse {
                    auth_id: hold.auth_id,
                    expires_at: java_instant(hold.expires_at),
                }),
                None => replay_winner(&state, &order, amount, method_id).await,
            },
            Err(reason) => match state
                .store
                .record_decline(order.as_str(), amount, method_id, &reason)
                .await?
            {
                Some(_) => Err(RouteError::Declined(reason)),
                None => replay_winner(&state, &order, amount, method_id).await,
            },
        }
    }
    .await;
    Ok(Json(logged(&state, result)?))
}

/// `POST /payments/authorizations/{orderId}/expire`: an operator makes the
/// order's hold lapse now, and the sweep voids it.
///
/// # Errors
/// No valid operator token (401); no payment (404); not authorized (409); a
/// store failure (500).
pub async fn expire<S: Payments>(
    State(state): State<AppState<S>>,
    Path(raw): Path<String>,
    headers: HeaderMap,
) -> Result<Json<PaymentResponse>, RouteError> {
    let result = async {
        state
            .auth
            .operator(authorization(&headers))
            .ok_or_else(|| refused(StatusCode::UNAUTHORIZED, "Invalid operator token"))?;
        let order = OrderId::parse(&raw).map_err(|_| not_found())?;
        if state.store.expire_now(order.as_str()).await?.is_some() {
            sweep_once(&state.store, &state.logger).await?;
            return state
                .store
                .by_order(order.as_str())
                .await?
                .ok_or_else(not_found);
        }
        match state.store.by_order(order.as_str()).await? {
            Some(payment) if payment.status != PaymentStatus::Authorized => Err(refused(
                StatusCode::CONFLICT,
                &format!("Payment is {}, not authorized", payment.status.as_str()),
            )),
            _ => Err(not_found()),
        }
    }
    .await;
    Ok(Json(logged(&state, result)?.into()))
}

/// Trimmed, and absent when blank (as the Scala list reads its filters).
fn filter(raw: Option<&str>) -> Option<&str> {
    raw.map(str::trim).filter(|text| !text.is_empty())
}

/// `GET /reconciliations?status&orderId`: flags, newest first.
///
/// # Errors
/// An unknown status (400); a store failure (500).
pub async fn reconciliations<S: Payments>(
    State(state): State<AppState<S>>,
    Query(query): Query<FilterQuery>,
) -> Result<Json<Vec<ReconciliationResponse>>, RouteError> {
    let result = async {
        let status = filter(query.status.as_deref())
            .map(|raw| {
                ReconciliationStatus::parse(raw)
                    .ok_or_else(|| refused(StatusCode::BAD_REQUEST, ONLY_RESOLVED))
            })
            .transpose()?;
        let order_id = filter(query.order_id.as_deref());
        Ok(state.store.reconciliations(status, order_id).await?)
    }
    .await;
    let flags = logged(&state, result)?;
    Ok(Json(flags.into_iter().map(Into::into).collect()))
}

/// `PATCH /reconciliations/{orderId}`: an operator resolves the flag; the
/// first resolver wins.
///
/// # Errors
/// An undecodable body (400); no valid operator token (401); a status other
/// than `resolved` (400); no flag (404); a store failure (500).
pub async fn resolve<S: Payments>(
    State(state): State<AppState<S>>,
    Path(raw): Path<String>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<Json<ReconciliationResponse>, RouteError> {
    let request: ResolveReconciliationRequest = body(&bytes)?;
    let result = async {
        let operator = state
            .auth
            .operator(authorization(&headers))
            .ok_or_else(|| refused(StatusCode::UNAUTHORIZED, "Invalid operator token"))?;
        if request.status.trim() != ReconciliationStatus::Resolved.as_str() {
            return Err(refused(StatusCode::BAD_REQUEST, ONLY_RESOLVED));
        }
        let flag_missing = || refused(StatusCode::NOT_FOUND, "Reconciliation flag not found");
        let order = OrderId::parse(&raw).map_err(|_| flag_missing())?;
        let note = filter(request.note.as_deref());
        let outcome = state
            .store
            .resolve(order.as_str(), operator, note, OffsetDateTime::now_utc())
            .await?;
        match outcome {
            ResolveOutcome::Resolved(flag) | ResolveOutcome::AlreadyResolved(flag) => Ok(flag),
            ResolveOutcome::NotFound => Err(flag_missing()),
        }
    }
    .await;
    Ok(Json(logged(&state, result)?.into()))
}
