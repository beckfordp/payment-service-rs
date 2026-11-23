// SPDX-License-Identifier: MIT

//! The `payment-service` binary: reads the config contract, migrates the
//! database, then serves the HTTP API and runs the Kafka
//! consumers until SIGTERM or SIGINT. A consumer or
//! server that stops by itself stops the service, non-zero (Scala
//! `KafkaConsumers.watched`).

use kafka_client::supervised::ConsumerTelemetry;
use payment_service::config::{self, Config};
use payment_service::store::PgExamples;
use payment_service::{consumers, lifecycle};
use payment_service::{db, server};
use service_basics::{environment, runtime, shutdown};
use std::process::ExitCode;
use telemetry::logging::Logger;
use telemetry::metrics::Metrics;
use telemetry::tracing::{SdkTracer, Tracing};
use tokio::sync::watch;

fn main() -> ExitCode {
    let config = match config::from_env(&environment::variables()) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{SERVICE}: {error}");
            return ExitCode::FAILURE;
        }
    };
    // Built outside the async runtime: the OTLP exporter's blocking HTTP
    // client must not be created (or dropped) on a runtime thread.
    let tracing = match Tracing::new(
        &config.service_name,
        config.tracing.otlp_endpoint.as_deref(),
        config.tracing.sampling_ratio,
    ) {
        Ok(tracing) => tracing,
        Err(error) => {
            eprintln!("{SERVICE}: {error}");
            return ExitCode::FAILURE;
        }
    };
    let runtime = match runtime::multi_thread() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("{SERVICE}: can't start the async runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    let code = runtime.block_on(serve(&config, tracing.tracer()));
    drop(runtime);
    tracing.shutdown();
    code
}

/// The service's name in its stderr lines and SIGTERM warning.
const SERVICE: &str = "payment-service";

async fn serve(config: &Config, tracer: SdkTracer) -> ExitCode {
    let pool = match db::connect(&config.postgres).await {
        Ok(pool) => pool,
        Err(error) => {
            eprintln!(
                "{SERVICE}: database {}:{}/{} unreachable: {error}",
                config.postgres.host, config.postgres.port, config.postgres.database
            );
            return ExitCode::FAILURE;
        }
    };
    if let Err(error) = db::migrate(&pool).await {
        eprintln!("{SERVICE}: database migration failed: {error}");
        return ExitCode::FAILURE;
    }
    let logger = Logger::stdout(&config.service_name);
    logger.info(
        &[("port", &config.port.to_string())],
        "payment-service starting",
    );
    let store = PgExamples::new(pool);
    let telemetry = ConsumerTelemetry {
        tracer,
        logger: logger.clone(),
        metrics: Metrics::new(&config.service_name),
    };
    let (stop, stopped) = watch::channel(false);
    let server = {
        let (port, mut stopped) = (config.port, stopped.clone());
        async move {
            let until_stopped = async move {
                let _ = stopped.wait_for(|stop| *stop).await;
            };
            server::run(port, store, until_stopped).await
        }
    };
    let consumers = {
        let bootstrap = config.kafka_bootstrap.clone();
        let logger = logger.clone();
        async move { consumers::run(&bootstrap, logger, telemetry, stopped).await }
    };
    let signal = shutdown::signal(SERVICE);
    if lifecycle::run_until_stopped(server, consumers, signal, stop, &logger).await {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
