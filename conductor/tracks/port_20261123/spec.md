# Spec — port payment-service (gluon all-Rust Stage A, GREEN)

## Overview
The Scala payment-service in idiomatic Rust on the template's shape: its tables (the
Flyway V1–V12 end state), payment methods (card and address validation, the simulated
tokenizer), payments CRUD, the synchronous authorize against the simulated gateway
(hold, decline, replay, conflicts) writing `PaymentAuthorized` to the outbox in its
transaction, the operator's expire and the background expiry sweep
(`PaymentAuthorizationExpired`), the `fulfilment-events` consumer (DispatchConfirmed
captures → `PaymentCaptured` or `PaymentFailed`; DispatchFailed voids) and the
`order-events` consumer (OrderCancelled voids), race flags, DLQs, reconciliations and
operator auth, readiness on Postgres, CORS, the docs (OpenAPI and AsyncAPI).

## Functional requirements
PAY-F1–F18, PAY-D1–D2 (tests/contract `docs/behaviours.md`).

## Acceptance criteria
`scripts/ci.sh` green, ≥90% lines; `bin/verify-port payment-service` all MET.

## Out of scope
Wire rows: metrics (db durations, pool, journey metrics, flag counter, event age), log
messages, tracing spans and the outbox's traceparent.
