// SPDX-License-Identifier: MIT

//! The tables, as the Scala `PaymentStore` and `PaymentMethodStore` use them
//! (the same SQL): payments one per order, their outbox events written in the
//! transaction that records the change, payment methods, and the
//! reconciliation flags. Handlers reach them through the [`Payments`] trait,
//! so they are testable with a store that fails (`tests/support`);
//! [`PgPayments`] is the Postgres one.

use service_basics::jvm::java_instant;
use service_basics::outbox;
use sqlx::postgres::PgRow;
use sqlx::{FromRow, PgPool, Postgres, Row, Transaction};
use std::future::Future;
use std::time::Duration;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::domain::{
    AUTHORIZATION_EXPIRED, Address, Authorization, CardBrand, Payment, PaymentMethod,
    PaymentStatus, Reconciliation, ReconciliationStatus, ResolveOutcome,
};
use crate::published::{
    PAYMENT_AUTHORIZATION_EXPIRED, PAYMENT_AUTHORIZED, PAYMENT_CAPTURED, PAYMENT_FAILED,
    PaymentAuthorizationExpired, PaymentAuthorized, PaymentCaptured, PaymentFailed, SOURCE,
};

/// Why a store call failed.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The database refused or failed.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    /// An event payload or address didn't serialise.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// A stored value outside its closed set.
    #[error("unreadable stored {0}: {1:?}")]
    Unreadable(&'static str, String),
}

/// What the service needs from its tables.
pub trait Payments: Clone + Send + Sync + 'static {
    /// Stores `method`; when it was created.
    fn add_method(
        &self,
        method: &PaymentMethod,
    ) -> impl Future<Output = Result<OffsetDateTime, StoreError>> + Send;

    /// The method with `id`, if there is one.
    fn method(
        &self,
        id: Uuid,
    ) -> impl Future<Output = Result<Option<PaymentMethod>, StoreError>> + Send;

    /// A pending payment for `order_id`; `None` when the order has one.
    fn create(
        &self,
        order_id: &str,
        amount_cents: i32,
    ) -> impl Future<Output = Result<Option<Payment>, StoreError>> + Send;

    /// The payment with `id`, if there is one.
    fn get(&self, id: Uuid) -> impl Future<Output = Result<Option<Payment>, StoreError>> + Send;

    /// Deletes the payment with `id`; false when there was none.
    fn delete(&self, id: Uuid) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// The order's payment, if it has one.
    fn by_order(
        &self,
        order_id: &str,
    ) -> impl Future<Output = Result<Option<Payment>, StoreError>> + Send;

    /// An authorized payment and its `PaymentAuthorized`, in one transaction;
    /// `None` when the order already has a payment.
    fn record_authorization(
        &self,
        order_id: &str,
        amount_cents: i32,
        method_id: Uuid,
        authorization: &Authorization,
    ) -> impl Future<Output = Result<Option<Payment>, StoreError>> + Send;

    /// A failed payment with the decline reason, no event; `None` when the
    /// order already has a payment.
    fn record_decline(
        &self,
        order_id: &str,
        amount_cents: i32,
        method_id: Uuid,
        reason: &str,
    ) -> impl Future<Output = Result<Option<Payment>, StoreError>> + Send;

    /// Authorized (and live) → captured, with `PaymentCaptured`; `None` when
    /// no live authorized payment matched.
    fn record_capture(
        &self,
        order_id: &str,
    ) -> impl Future<Output = Result<Option<Payment>, StoreError>> + Send;

    /// Authorized → failed with `reason`, with `PaymentFailed`; `None` when no
    /// authorized payment matched.
    fn record_capture_failure(
        &self,
        order_id: &str,
        reason: &str,
    ) -> impl Future<Output = Result<Option<Payment>, StoreError>> + Send;

    /// Authorized → voided, no event; `None` when no authorized payment matched.
    fn record_void(
        &self,
        order_id: &str,
    ) -> impl Future<Output = Result<Option<Payment>, StoreError>> + Send;

    /// Up to `limit` lapsed authorizations voided, oldest expiry first, each
    /// with `PaymentAuthorizationExpired`; concurrent sweeps take disjoint rows.
    fn expire_due(
        &self,
        limit: i32,
    ) -> impl Future<Output = Result<Vec<Payment>, StoreError>> + Send;

    /// An authorized payment's hold made to lapse now; `None` when the order
    /// has no authorized payment.
    fn expire_now(
        &self,
        order_id: &str,
    ) -> impl Future<Output = Result<Option<Payment>, StoreError>> + Send;

    /// Flags the order for manual reconciliation with `reason`; the first
    /// flag wins, and an order with no payment isn't flagged. True when recorded.
    fn flag(
        &self,
        order_id: &str,
        reason: &str,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Flags, newest first, by status and order.
    fn reconciliations(
        &self,
        status: Option<ReconciliationStatus>,
        order_id: Option<&str>,
    ) -> impl Future<Output = Result<Vec<Reconciliation>, StoreError>> + Send;

    /// Resolves the order's open flag as `operator`; the first resolver wins.
    fn resolve(
        &self,
        order_id: &str,
        operator: &str,
        note: Option<&str>,
        at: OffsetDateTime,
    ) -> impl Future<Output = Result<ResolveOutcome, StoreError>> + Send;

    /// True when the database answers within `within` (readiness).
    fn ping(&self, within: Duration) -> impl Future<Output = bool> + Send;
}

/// The tables in Postgres.
#[derive(Debug, Clone)]
pub struct PgPayments {
    pool: PgPool,
}

impl PgPayments {
    /// Over `pool`, migrated.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

const PAYMENT_COLUMNS: &str = "id, order_id, amount_cents, status, created_at, updated_at, \
     payment_method_id, auth_id, expires_at, decline_reason";

const RECONCILIATION_COLUMNS: &str =
    "order_id, reason, status, created_at, operator_id, note, resolved_at";

struct PaymentRow(Payment);

impl FromRow<'_, PgRow> for PaymentRow {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        let status: String = row.try_get("status")?;
        let status = PaymentStatus::parse(&status).ok_or_else(|| decode("status", &status))?;
        Ok(Self(Payment {
            id: row.try_get("id")?,
            order_id: row.try_get("order_id")?,
            amount_cents: row.try_get("amount_cents")?,
            status,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
            payment_method_id: row.try_get("payment_method_id")?,
            auth_id: row.try_get("auth_id")?,
            expires_at: row.try_get("expires_at")?,
            decline_reason: row.try_get("decline_reason")?,
        }))
    }
}

struct ReconciliationRow(Reconciliation);

impl FromRow<'_, PgRow> for ReconciliationRow {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        let status: String = row.try_get("status")?;
        let status =
            ReconciliationStatus::parse(&status).ok_or_else(|| decode("status", &status))?;
        Ok(Self(Reconciliation {
            order_id: row.try_get("order_id")?,
            reason: row.try_get("reason")?,
            status,
            created_at: row.try_get("created_at")?,
            operator_id: row.try_get("operator_id")?,
            note: row.try_get("note")?,
            resolved_at: row.try_get("resolved_at")?,
        }))
    }
}

fn decode(column: &str, value: &str) -> sqlx::Error {
    sqlx::Error::ColumnDecode {
        index: column.to_owned(),
        source: format!("unknown value {value:?}").into(),
    }
}

/// Writes one event for `order_id` in `tx`, stamped `now`.
async fn publish<T: serde::Serialize + Sync>(
    tx: &mut Transaction<'_, Postgres>,
    event_type: &str,
    order_id: &str,
    payload: &T,
    now: OffsetDateTime,
) -> Result<(), StoreError> {
    let event = outbox::envelope(event_type, SOURCE, order_id, order_id, payload, now)?;
    // No span yet to parent consumers' traces.
    outbox::write(tx, &event, None).await?;
    Ok(())
}

impl PgPayments {
    /// An outcome row for a new order, `event` written with it when inserted.
    #[allow(clippy::too_many_arguments)]
    async fn insert_outcome(
        &self,
        order_id: &str,
        amount_cents: i32,
        status: PaymentStatus,
        method_id: Uuid,
        authorization: Option<&Authorization>,
        decline_reason: Option<&str>,
    ) -> Result<Option<Payment>, StoreError> {
        let mut tx = self.pool.begin().await?;
        let inserted: Option<PaymentRow> = sqlx::query_as(&format!(
            "insert into payment (id, order_id, amount_cents, status, payment_method_id, \
             auth_id, expires_at, decline_reason) values ($1, $2, $3, $4, $5, $6, $7, $8) \
             on conflict (order_id) do nothing returning {PAYMENT_COLUMNS}"
        ))
        .bind(Uuid::new_v4())
        .bind(order_id)
        .bind(amount_cents)
        .bind(status.as_str())
        .bind(method_id)
        .bind(authorization.map(|a| a.auth_id.as_str()))
        .bind(authorization.map(|a| a.expires_at))
        .bind(decline_reason)
        .fetch_optional(&mut *tx)
        .await?;
        if let (Some(_), Some(authorization)) = (&inserted, authorization) {
            let now = OffsetDateTime::now_utc();
            let payload = PaymentAuthorized {
                order_id: order_id.to_owned(),
                auth_id: authorization.auth_id.clone(),
                expires_at: java_instant(authorization.expires_at),
                timestamp: java_instant(now),
            };
            publish(&mut tx, PAYMENT_AUTHORIZED, order_id, &payload, now).await?;
        }
        tx.commit().await?;
        Ok(inserted.map(|row| row.0))
    }

    /// The `authorized → status` transition (live authorizations only for a
    /// capture), and its event when it matched.
    async fn transition(
        &self,
        order_id: &str,
        status: PaymentStatus,
        reason: Option<&str>,
    ) -> Result<Option<Payment>, StoreError> {
        let mut tx = self.pool.begin().await?;
        let moved: Option<PaymentRow> = sqlx::query_as(&format!(
            "update payment set status = $1, decline_reason = coalesce($2, decline_reason), \
             updated_at = now() where order_id = $3 and status = 'authorized' \
             and (not $4 or expires_at > now()) returning {PAYMENT_COLUMNS}"
        ))
        .bind(status.as_str())
        .bind(reason)
        .bind(order_id)
        .bind(status == PaymentStatus::Captured)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(PaymentRow(payment)) = &moved {
            let now = OffsetDateTime::now_utc();
            let timestamp = java_instant(now);
            match status {
                PaymentStatus::Captured => {
                    let payload = PaymentCaptured {
                        order_id: order_id.to_owned(),
                        amount_cents: payment.amount_cents,
                        timestamp,
                    };
                    publish(&mut tx, PAYMENT_CAPTURED, order_id, &payload, now).await?;
                }
                PaymentStatus::Failed => {
                    let payload = PaymentFailed {
                        order_id: order_id.to_owned(),
                        payment_id: payment.id.to_string(),
                        amount_cents: payment.amount_cents,
                        reason: reason.unwrap_or_default().to_owned(),
                        timestamp,
                    };
                    publish(&mut tx, PAYMENT_FAILED, order_id, &payload, now).await?;
                }
                _ => {}
            }
        }
        tx.commit().await?;
        Ok(moved.map(|row| row.0))
    }
}

impl Payments for PgPayments {
    async fn add_method(&self, method: &PaymentMethod) -> Result<OffsetDateTime, StoreError> {
        let created_at = sqlx::query_scalar(
            "insert into payment_methods (id, customer_id, card_token, last4, brand, expiry, \
             billing_address) values ($1, $2, $3, $4, $5, $6, $7) returning created_at",
        )
        .bind(method.id)
        .bind(&method.customer_id)
        .bind(&method.card_token)
        .bind(&method.last4)
        .bind(method.brand.as_str())
        .bind(&method.expiry)
        .bind(sqlx::types::Json(&method.billing_address))
        .fetch_one(&self.pool)
        .await?;
        Ok(created_at)
    }

    async fn method(&self, id: Uuid) -> Result<Option<PaymentMethod>, StoreError> {
        let row: Option<(
            String,
            String,
            String,
            String,
            String,
            sqlx::types::Json<Address>,
        )> = sqlx::query_as(
            "select customer_id, card_token, last4, brand, expiry, billing_address \
             from payment_methods where id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|(customer_id, card_token, last4, brand, expiry, address)| {
            Ok(PaymentMethod {
                id,
                customer_id,
                card_token,
                last4,
                brand: CardBrand::parse(&brand)
                    .ok_or_else(|| StoreError::Unreadable("brand", brand.clone()))?,
                expiry,
                billing_address: address.0,
            })
        })
        .transpose()
    }

    async fn create(
        &self,
        order_id: &str,
        amount_cents: i32,
    ) -> Result<Option<Payment>, StoreError> {
        let row: Option<PaymentRow> = sqlx::query_as(&format!(
            "insert into payment (id, order_id, amount_cents, status) values ($1, $2, $3, $4) \
             on conflict (order_id) do nothing returning {PAYMENT_COLUMNS}"
        ))
        .bind(Uuid::new_v4())
        .bind(order_id)
        .bind(amount_cents)
        .bind(PaymentStatus::Pending.as_str())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| row.0))
    }

    async fn get(&self, id: Uuid) -> Result<Option<Payment>, StoreError> {
        let row: Option<PaymentRow> = sqlx::query_as(&format!(
            "select {PAYMENT_COLUMNS} from payment where id = $1"
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| row.0))
    }

    async fn delete(&self, id: Uuid) -> Result<bool, StoreError> {
        let deleted = sqlx::query("delete from payment where id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(deleted.rows_affected() > 0)
    }

    async fn by_order(&self, order_id: &str) -> Result<Option<Payment>, StoreError> {
        let row: Option<PaymentRow> = sqlx::query_as(&format!(
            "select {PAYMENT_COLUMNS} from payment where order_id = $1"
        ))
        .bind(order_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| row.0))
    }

    async fn record_authorization(
        &self,
        order_id: &str,
        amount_cents: i32,
        method_id: Uuid,
        authorization: &Authorization,
    ) -> Result<Option<Payment>, StoreError> {
        self.insert_outcome(
            order_id,
            amount_cents,
            PaymentStatus::Authorized,
            method_id,
            Some(authorization),
            None,
        )
        .await
    }

    async fn record_decline(
        &self,
        order_id: &str,
        amount_cents: i32,
        method_id: Uuid,
        reason: &str,
    ) -> Result<Option<Payment>, StoreError> {
        self.insert_outcome(
            order_id,
            amount_cents,
            PaymentStatus::Failed,
            method_id,
            None,
            Some(reason),
        )
        .await
    }

    async fn record_capture(&self, order_id: &str) -> Result<Option<Payment>, StoreError> {
        self.transition(order_id, PaymentStatus::Captured, None)
            .await
    }

    async fn record_capture_failure(
        &self,
        order_id: &str,
        reason: &str,
    ) -> Result<Option<Payment>, StoreError> {
        self.transition(order_id, PaymentStatus::Failed, Some(reason))
            .await
    }

    async fn record_void(&self, order_id: &str) -> Result<Option<Payment>, StoreError> {
        self.transition(order_id, PaymentStatus::Voided, None).await
    }

    async fn expire_due(&self, limit: i32) -> Result<Vec<Payment>, StoreError> {
        let mut tx = self.pool.begin().await?;
        let rows: Vec<PaymentRow> = sqlx::query_as(
            "with due as (select order_id from payment where status = 'authorized' \
             and expires_at <= now() order by expires_at limit $1 for update skip locked) \
             update payment p set status = 'voided', decline_reason = $2, updated_at = now() \
             from due where p.order_id = due.order_id returning p.id, p.order_id, \
             p.amount_cents, p.status, p.created_at, p.updated_at, p.payment_method_id, \
             p.auth_id, p.expires_at, p.decline_reason",
        )
        .bind(limit)
        .bind(AUTHORIZATION_EXPIRED)
        .fetch_all(&mut *tx)
        .await?;
        for PaymentRow(payment) in &rows {
            let now = OffsetDateTime::now_utc();
            let payload = PaymentAuthorizationExpired {
                order_id: payment.order_id.clone(),
                timestamp: java_instant(now),
            };
            publish(
                &mut tx,
                PAYMENT_AUTHORIZATION_EXPIRED,
                &payment.order_id,
                &payload,
                now,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(rows.into_iter().map(|row| row.0).collect())
    }

    async fn expire_now(&self, order_id: &str) -> Result<Option<Payment>, StoreError> {
        let row: Option<PaymentRow> = sqlx::query_as(&format!(
            "update payment set expires_at = now() where order_id = $1 \
             and status = 'authorized' returning {PAYMENT_COLUMNS}"
        ))
        .bind(order_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| row.0))
    }

    async fn flag(&self, order_id: &str, reason: &str) -> Result<bool, StoreError> {
        let flagged: Option<String> = sqlx::query_scalar(
            "insert into payment_reconciliation (order_id, reason) select $1, $2 \
             where exists (select 1 from payment where order_id = $1) \
             on conflict (order_id) do nothing returning order_id",
        )
        .bind(order_id)
        .bind(reason)
        .fetch_optional(&self.pool)
        .await?;
        Ok(flagged.is_some())
    }

    async fn reconciliations(
        &self,
        status: Option<ReconciliationStatus>,
        order_id: Option<&str>,
    ) -> Result<Vec<Reconciliation>, StoreError> {
        let rows: Vec<ReconciliationRow> = sqlx::query_as(&format!(
            "select {RECONCILIATION_COLUMNS} from payment_reconciliation \
             where ($1::text is null or status = $1) and ($2::text is null or order_id = $2) \
             order by created_at desc, order_id"
        ))
        .bind(status.map(ReconciliationStatus::as_str))
        .bind(order_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|row| row.0).collect())
    }

    async fn resolve(
        &self,
        order_id: &str,
        operator: &str,
        note: Option<&str>,
        at: OffsetDateTime,
    ) -> Result<ResolveOutcome, StoreError> {
        let mut tx = self.pool.begin().await?;
        let resolved: Option<ReconciliationRow> = sqlx::query_as(&format!(
            "update payment_reconciliation set status = 'resolved', operator_id = $1, \
             note = $2, resolved_at = $3 where order_id = $4 and status = 'open' \
             returning {RECONCILIATION_COLUMNS}"
        ))
        .bind(operator)
        .bind(note)
        .bind(at)
        .bind(order_id)
        .fetch_optional(&mut *tx)
        .await?;
        let outcome = if let Some(row) = resolved {
            ResolveOutcome::Resolved(row.0)
        } else {
            let existing: Option<ReconciliationRow> = sqlx::query_as(&format!(
                "select {RECONCILIATION_COLUMNS} from payment_reconciliation where order_id = $1"
            ))
            .bind(order_id)
            .fetch_optional(&mut *tx)
            .await?;
            existing.map_or(ResolveOutcome::NotFound, |row| {
                ResolveOutcome::AlreadyResolved(row.0)
            })
        };
        tx.commit().await?;
        Ok(outcome)
    }

    async fn ping(&self, within: Duration) -> bool {
        service_basics::postgres::ping(&self.pool, within)
            .await
            .is_ok()
    }
}
