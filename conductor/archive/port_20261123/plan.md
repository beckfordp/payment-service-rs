# Plan — port payment-service (gluon all-Rust Stage A, GREEN)

Spec: [spec.md](./spec.md).

## Phase 1: Port
- [x] Task 1.1: schema (the V1–V12 end state) and domain (card, address, payment rules and status, gateway outcomes, races, reconciliation, operator auth) — tests first b462d57
- [x] Task 1.2: Postgres store (the Scala SQL; outcomes and their outbox rows in one transaction; the sweep's `SKIP LOCKED`) — tests first (throwaway Postgres) b462d57
- [x] Task 1.3: routes and errors (methods, payments, authorize, expire, reconciliations); docs — tests first b462d57
- [x] Task 1.4: consumers (fulfilment-events, order-events, DLQ) and the expiry sweep — tests first b462d57
- [x] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md): gluon `bin/verify-port payment-service` all MET; CI ≥90% lines — PASS (gluon docs/ports/evidence/payment-service/green.txt; contract 20/20 first run, CI 94.71% lines)

## Review

Self-review against the Scala service: SQL, validation order, replay rules, race
classification and log lines followed one for one; deliberate differences recorded as gluon
decision A12. Tests written with the code (the RED contract was the failing test); no findings.
