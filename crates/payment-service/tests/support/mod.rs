// SPDX-License-Identifier: MIT

//! Shared by the integration tests: gluon-rs `service-testing`'s throwaway
//! PostgreSQL cluster, this service's store over it and its binary under
//! test, a store that always fails, and a logger whose lines the
//! test can read back.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unused_async_trait_impl
)]

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use payment_service::config::{PORT_VARIABLE, PostgresConfig};
use payment_service::db;
use payment_service::domain::{
    Authorization, Payment, PaymentMethod, Reconciliation, ReconciliationStatus, ResolveOutcome,
};
use payment_service::store::{Payments, PgPayments, StoreError};
use serde_json::Value;
use sqlx::PgPool;
use telemetry::logging::Logger;
use time::OffsetDateTime;
use uuid::Uuid;

// Not every test file uses both.
#[allow(unused_imports)]
pub use service_testing::{Postgres, free_port};

/// The payment-service binary, on a fresh port.
pub struct Service(service_testing::Service);

impl Service {
    /// Starts it with exactly `extra` (plus its port); does not wait.
    pub fn spawn(extra: &[(&str, String)]) -> Self {
        Self(service_testing::Service::spawn(
            env!("CARGO_BIN_EXE_payment-service"),
            PORT_VARIABLE,
            extra,
        ))
    }

    /// Starts it and waits until `/health` answers.
    pub fn start(extra: &[(&str, String)]) -> Self {
        Self(service_testing::Service::start(
            env!("CARGO_BIN_EXE_payment-service"),
            PORT_VARIABLE,
            extra,
        ))
    }
}

impl Deref for Service {
    type Target = service_testing::Service;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Service {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// The service's store over `postgres`, migrated as the service migrates it.
pub async fn store(postgres: &Postgres) -> PgPayments {
    PgPayments::new(pool(postgres).await)
}

/// A pool on `postgres`, migrated as the service migrates it.
pub async fn pool(postgres: &Postgres) -> PgPool {
    let env: HashMap<_, _> = postgres.env().into_iter().collect();
    let config = PostgresConfig {
        host: env["POSTGRES_HOST"].clone(),
        port: env["POSTGRES_PORT"].parse().unwrap(),
        database: env["POSTGRES_DB"].clone(),
        user: env["POSTGRES_USER"].clone(),
        password: env["POSTGRES_PASSWORD"].clone(),
    };
    let pool = db::connect(&config).await.unwrap();
    db::migrate(&pool).await.unwrap();
    pool
}

/// A store whose every call fails as a database would, and whose ping fails.
#[derive(Clone, Default)]
pub struct Broken;

fn broken<T>() -> Result<T, StoreError> {
    Err(StoreError::Database(sqlx::Error::PoolClosed))
}

impl Payments for Broken {
    async fn add_method(&self, _: &PaymentMethod) -> Result<OffsetDateTime, StoreError> {
        broken()
    }
    async fn method(&self, _: Uuid) -> Result<Option<PaymentMethod>, StoreError> {
        broken()
    }
    async fn create(&self, _: &str, _: i32) -> Result<Option<Payment>, StoreError> {
        broken()
    }
    async fn get(&self, _: Uuid) -> Result<Option<Payment>, StoreError> {
        broken()
    }
    async fn delete(&self, _: Uuid) -> Result<bool, StoreError> {
        broken()
    }
    async fn by_order(&self, _: &str) -> Result<Option<Payment>, StoreError> {
        broken()
    }
    async fn record_authorization(
        &self,
        _: &str,
        _: i32,
        _: Uuid,
        _: &Authorization,
    ) -> Result<Option<Payment>, StoreError> {
        broken()
    }
    async fn record_decline(
        &self,
        _: &str,
        _: i32,
        _: Uuid,
        _: &str,
    ) -> Result<Option<Payment>, StoreError> {
        broken()
    }
    async fn record_capture(&self, _: &str) -> Result<Option<Payment>, StoreError> {
        broken()
    }
    async fn record_capture_failure(
        &self,
        _: &str,
        _: &str,
    ) -> Result<Option<Payment>, StoreError> {
        broken()
    }
    async fn record_void(&self, _: &str) -> Result<Option<Payment>, StoreError> {
        broken()
    }
    async fn expire_due(&self, _: i32) -> Result<Vec<Payment>, StoreError> {
        broken()
    }
    async fn expire_now(&self, _: &str) -> Result<Option<Payment>, StoreError> {
        broken()
    }
    async fn flag(&self, _: &str, _: &str) -> Result<bool, StoreError> {
        broken()
    }
    async fn reconciliations(
        &self,
        _: Option<ReconciliationStatus>,
        _: Option<&str>,
    ) -> Result<Vec<Reconciliation>, StoreError> {
        broken()
    }
    async fn resolve(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: OffsetDateTime,
    ) -> Result<ResolveOutcome, StoreError> {
        broken()
    }
    async fn ping(&self, _: Duration) -> bool {
        false
    }
}

/// The order's outbox rows' envelopes, in write order.
pub async fn outbox(pool: &PgPool, order_id: &str) -> Vec<Value> {
    let rows: Vec<(sqlx::types::Json<Value>,)> = sqlx::query_as(
        "select payload from outbox_events where aggregate_id = $1 order by created_at, id",
    )
    .bind(order_id)
    .fetch_all(pool)
    .await
    .unwrap();
    rows.into_iter().map(|(json,)| json.0).collect()
}

/// The eventTypes of the order's outbox rows, in write order.
pub async fn event_types(pool: &PgPool, order_id: &str) -> Vec<String> {
    outbox(pool, order_id)
        .await
        .iter()
        .map(|envelope| envelope["eventType"].as_str().unwrap().to_owned())
        .collect()
}

/// Where a [`captured_logger`] writes.
pub type LogBuffer = Arc<Mutex<Vec<u8>>>;

/// A logger named as the service's, writing to a buffer the test reads back.
pub fn captured_logger() -> (Logger, LogBuffer) {
    let buffer = LogBuffer::default();
    (Logger::to_writer("payment-service", buffer.clone()), buffer)
}

/// The lines logged to `buffer` so far, each as JSON.
pub fn log_lines(buffer: &LogBuffer) -> Vec<Value> {
    let text = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
    text.lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
