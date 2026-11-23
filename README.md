# payment-service-rs

The Rust port of gluon's Scala `payment-service` (ADR 0029), generated from
gluon's `tools/rust-service-template`. Conductor-driven: start at
[`conductor/index.md`](conductor/index.md).

## After generating
1. `mv payment-service payment-service-rs` (in gluon's `services/`).
2. `cargo update -w --offline`: the lock covers every option (versions the sandbox
   mirror serves); this drops what the service doesn't use and adds nothing.
3. `git init`, then add `("services/payment-service", "services/payment-service-rs")` to
   gluon `bin/lib/sandbox.py` `PORTS`.
4. Copy `deploy/payment-service-rs.values.yaml` to gluon `environments/local/`, its env
   from `payment-service.values.yaml`.
5. `scripts/ci.sh`: green before the first change.
6. Replace the example: `migrations/` (the Scala service's Flyway files, verbatim),
   `domain.rs`, `store.rs`, `examples.rs`, `events.rs`, and their tests.

## Layout
- `src/config.rs`: the env contract (`PAYMENT_SERVICE_PORT`, `POSTGRES_*`,
  `KAFKA_BOOTSTRAP_SERVERS`, `OTEL_*`); every problem reported at once.
- `src/domain.rs`: the pure core; `src/ids.rs`, `src/clock.rs`: injected ids and time.
- `src/store.rs`: the tables behind a trait (Postgres here, in memory in tests).
- `src/server.rs`: routes behind gluon-rs `service-basics::edge` (CORS, the Allow-less
  405); `src/examples.rs`: one route and its error enum; `src/docs.rs`: the OpenAPI and
  AsyncAPI documents and their pages (`/docs/`, `/docs/asyncapi/`): extend both per route
  and event.
- `src/published.rs`, `migrations/2_create_outbox_events.sql`: the event the store writes
  to the outbox in its transaction (gluon-rs `service-basics::outbox`); Debezium
  publishes it.
- `src/events.rs`, `src/consumers.rs`: one consumer on gluon-rs `kafka-client`;
  `src/lifecycle.rs`: the server and consumers stop together.
- `tests/`: in-memory route tests; Postgres-backed tests on a throwaway cluster (gluon-rs
  `service-testing`); the binary's exits.
