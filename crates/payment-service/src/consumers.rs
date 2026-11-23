// SPDX-License-Identifier: MIT

//! The background work (Scala `KafkaConsumers.supervised` and `ExpirySweep`):
//! the `order-events` and `fulfilment-events` consumers, each supervised with
//! its own DLQ guard, and the expiry sweep, run together until `stop`.

use std::time::Duration;

use kafka_client::KafkaError;
use kafka_client::settings::{ConsumerSettings, SettingsError};
use kafka_client::supervised::{ConsumerTelemetry, Resilience, run_supervised};
use telemetry::logging::Logger;
use tokio::sync::watch;

use crate::domain::SimulatedGateway;
use crate::events::{
    FULFILMENT_EVENTS, FULFILMENT_GROUP, FulfilmentEvents, ORDER_EVENTS, ORDER_GROUP, OrderEvents,
};
use crate::store::{Payments, StoreError};

/// How often the consumers report their lag.
pub const LAG_INTERVAL: Duration = Duration::from_secs(5);

/// Lapsed authorizations voided per sweep (`expiry-sweep.batch-size`).
pub const SWEEP_BATCH: i32 = 100;

/// One consumer's settings.
///
/// # Errors
/// A topic or group name the client refuses.
pub fn settings(
    bootstrap: &str,
    topic: &str,
    group: &str,
) -> Result<ConsumerSettings, SettingsError> {
    Ok(ConsumerSettings {
        bootstrap: bootstrap.to_owned(),
        group_id: group.parse()?,
        topic: topic.parse()?,
        lag_interval: LAG_INTERVAL,
    })
}

/// One sweep: voids up to [`SWEEP_BATCH`] lapsed authorizations (each with
/// its `PaymentAuthorizationExpired`); how many.
///
/// # Errors
/// A store failure.
pub async fn sweep_once<S: Payments>(store: &S, logger: &Logger) -> Result<usize, StoreError> {
    let expired = store.expire_due(SWEEP_BATCH).await?;
    for payment in &expired {
        logger.info(
            &[("order_id", &payment.order_id)],
            "Authorization expired - payment voided",
        );
    }
    Ok(expired.len())
}

/// Sweeps every `interval` (after the first wait) until `stop` is set; a
/// failed tick is logged and retried next tick.
pub async fn sweep<S: Payments>(
    store: S,
    interval: Duration,
    logger: Logger,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = stop.wait_for(|stop| *stop) => return,
            () = tokio::time::sleep(interval) => {}
        }
        if let Err(error) = sweep_once(&store, &logger).await {
            logger.error(
                &[("error", &error.to_string())],
                "Expiry sweep tick failed - retrying next tick",
            );
        }
    }
}

/// Runs both consumers and the sweep over `store` until `stop` is set.
///
/// # Errors
/// When a consumer can't be built (its settings, or its DLQ producer).
pub async fn run<S: Payments>(
    bootstrap: &str,
    store: S,
    sweep_interval: Duration,
    telemetry: ConsumerTelemetry,
    stop: watch::Receiver<bool>,
) -> Result<(), KafkaError> {
    let created = |error: SettingsError| KafkaError::ClientCreation(error.to_string());
    let orders = settings(bootstrap, ORDER_EVENTS, ORDER_GROUP).map_err(created)?;
    let fulfilment = settings(bootstrap, FULFILMENT_EVENTS, FULFILMENT_GROUP).map_err(created)?;
    let resilience = Resilience::default();
    let logger = telemetry.logger.clone();
    let gateway = SimulatedGateway::default();
    let sweeping = sweep(store.clone(), sweep_interval, logger.clone(), stop.clone());
    tokio::try_join!(
        run_supervised(
            orders,
            &resilience,
            OrderEvents::new(store.clone(), gateway.clone(), logger.clone()),
            telemetry.clone(),
            stop.clone(),
        ),
        run_supervised(
            fulfilment,
            &resilience,
            FulfilmentEvents::new(store, gateway, logger),
            telemetry,
            stop,
        ),
        async {
            sweeping.await;
            Ok(())
        },
    )?;
    Ok(())
}
