// SPDX-License-Identifier: MIT

//! The config contract: the Scala service's environment variables and
//! defaults (its `application.conf`). Pure: an environment map in, a
//! [`Config`] or every problem out; the pieces every service reads the same
//! way are gluon-rs `service-basics` and `telemetry`.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::time::Duration;

pub use service_basics::config::{ConfigError, PostgresConfig};
use service_basics::config::{port, postgres};
pub use telemetry::config::TracingConfig;
use telemetry::config::{parse_duration, tracing_from_env};

/// The HTTP port's variable.
pub const PORT_VARIABLE: &str = "PAYMENT_SERVICE_PORT";

/// The expiry sweep interval's variable (a HOCON duration).
pub const SWEEP_INTERVAL_VARIABLE: &str = "PAYMENT_EXPIRY_SWEEP_INTERVAL";

/// Everything the service reads from its environment.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    /// `PAYMENT_SERVICE_PORT`: the HTTP API.
    pub port: u16,
    /// `PAYMENT_SERVICE_NAME`: `service.name` on spans, the logger name.
    pub service_name: String,
    /// `POSTGRES_*`.
    pub postgres: PostgresConfig,
    /// `KAFKA_BOOTSTRAP_SERVERS`: the consumers' and DLQ producer's brokers.
    pub kafka_bootstrap: String,
    /// `PAYMENT_EXPIRY_SWEEP_INTERVAL`: how often the expiry sweep runs (30s).
    pub sweep_interval: Duration,
    /// `OPERATOR_TOKEN`: the dev operator token (ADR 0024).
    pub operator_token: String,
    /// `OPERATOR_ID`: the operator the token stands for.
    pub operator_id: String,
    /// `OTEL_*`.
    pub tracing: TracingConfig,
}

/// Reads the config from an environment map.
///
/// # Errors
/// [`ConfigError`] listing every invalid variable.
pub fn from_env<S: BuildHasher>(env: &HashMap<String, String, S>) -> Result<Config, ConfigError> {
    let mut problems = Vec::new();
    let text = |name: &str, default: &str| env.get(name).map_or(default, String::as_str).to_owned();
    let http_port = port(env, PORT_VARIABLE, 8080, &mut problems);
    let postgres = postgres(env, "payment", &mut problems);
    let tracing = tracing_from_env(env, &mut problems);
    let sweep_interval = match env.get(SWEEP_INTERVAL_VARIABLE) {
        None => Duration::from_secs(30),
        Some(raw) => match parse_duration(raw) {
            Ok(interval) if !interval.is_zero() => interval,
            Ok(_) => {
                problems.push(format!("{SWEEP_INTERVAL_VARIABLE} must be positive"));
                Duration::from_secs(30)
            }
            Err(error) => {
                problems.push(format!("{SWEEP_INTERVAL_VARIABLE}: {error}"));
                Duration::from_secs(30)
            }
        },
    };
    if !problems.is_empty() {
        return Err(ConfigError { problems });
    }
    Ok(Config {
        port: http_port,
        service_name: text("PAYMENT_SERVICE_NAME", "payment-service"),
        postgres,
        kafka_bootstrap: text("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        sweep_interval,
        operator_token: text("OPERATOR_TOKEN", "dev-operator-token"),
        operator_id: text("OPERATOR_ID", "dev-operator"),
        tracing,
    })
}
