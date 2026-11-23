// SPDX-License-Identifier: MIT

//! The config contract: the Scala service's environment variables and defaults.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;
use std::time::Duration;

use payment_service::config::{Config, PostgresConfig, TracingConfig, from_env};

fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

#[test]
fn defaults() {
    assert_eq!(
        from_env(&HashMap::new()).unwrap(),
        Config {
            port: 8080,
            service_name: "payment-service".to_owned(),
            postgres: PostgresConfig {
                host: "localhost".to_owned(),
                port: 5432,
                database: "payment".to_owned(),
                user: "payment".to_owned(),
                password: "payment".to_owned(),
            },
            kafka_bootstrap: "localhost:9092".to_owned(),
            sweep_interval: Duration::from_secs(30),
            operator_token: "dev-operator-token".to_owned(),
            operator_id: "dev-operator".to_owned(),
            tracing: TracingConfig {
                otlp_endpoint: None,
                sampling_ratio: 1.0,
            },
        }
    );
}

#[test]
fn every_variable_overrides_its_default() {
    let config = from_env(&env(&[
        ("PAYMENT_SERVICE_PORT", "18080"),
        ("PAYMENT_SERVICE_NAME", "payment-service-rs"),
        ("POSTGRES_HOST", "127.0.0.1"),
        ("POSTGRES_PORT", "15432"),
        ("POSTGRES_DB", "db"),
        ("POSTGRES_USER", "u"),
        ("POSTGRES_PASSWORD", "p"),
        ("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092"),
        ("PAYMENT_EXPIRY_SWEEP_INTERVAL", "5s"),
        ("OPERATOR_TOKEN", "t0k"),
        ("OPERATOR_ID", "ops"),
        ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318"),
        ("OTEL_TRACES_SAMPLER_ARG", "0.25"),
    ]))
    .unwrap();
    assert_eq!(config.port, 18080);
    assert_eq!(config.service_name, "payment-service-rs");
    assert_eq!(config.postgres.port, 15432);
    assert_eq!(config.kafka_bootstrap, "127.0.0.1:19092");
    assert_eq!(config.tracing.sampling_ratio, 0.25);
    assert_eq!(config.sweep_interval, Duration::from_secs(5));
    assert_eq!(
        (config.operator_token.as_str(), config.operator_id.as_str()),
        ("t0k", "ops")
    );
}

#[test]
fn every_problem_is_reported() {
    let error = from_env(&env(&[
        ("PAYMENT_SERVICE_PORT", "http"),
        ("POSTGRES_PORT", "x"),
        ("PAYMENT_EXPIRY_SWEEP_INTERVAL", "soon"),
    ]))
    .unwrap_err();
    let text = error.to_string();
    assert!(text.contains("PAYMENT_SERVICE_PORT"), "{text}");
    assert!(text.contains("POSTGRES_PORT"), "{text}");
    assert!(text.contains("PAYMENT_EXPIRY_SWEEP_INTERVAL"), "{text}");
}

#[test]
fn a_zero_sweep_interval_is_refused() {
    let error = from_env(&env(&[("PAYMENT_EXPIRY_SWEEP_INTERVAL", "0s")])).unwrap_err();
    assert!(error.to_string().contains("must be positive"), "{error}");
}
