# Plan — hold expiry at microseconds

Spec: [spec.md](./spec.md).

## Phase 1: Fix
- [x] Task 1.1: RED — `the_simulated_gateway_declines_its_test_cards` with a fixed nanosecond `now`, expecting a microsecond expiry 741a0c8
- [x] Task 1.2: GREEN — `SimulatedGateway::authorize` truncates `expires_at` to microseconds 741a0c8
- [x] Task 1.3: `scripts/ci.sh` locally; push; GitHub CI green — local green; GitHub run 45392763138 green on ubuntu-24.04
- [x] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md) — PASS: `an_authorization_holds_once_per_order_and_replays` green on Linux (nanosecond clock), where it failed before

## Review

Self-review: `to_micros` covers the only response built from the in-memory hold (the
first authorize) and the `PaymentAuthorized` payload; every other response reads the
stored row. Truncation is infallible (`nanosecond() % 1000`). No findings; 52 tests pass.
