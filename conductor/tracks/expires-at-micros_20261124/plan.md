# Plan — hold expiry at microseconds

Spec: [spec.md](./spec.md).

## Phase 1: Fix
- [x] Task 1.1: RED — `the_simulated_gateway_declines_its_test_cards` with a fixed nanosecond `now`, expecting a microsecond expiry 741a0c8
- [x] Task 1.2: GREEN — `SimulatedGateway::authorize` truncates `expires_at` to microseconds 741a0c8
- [ ] Task 1.3: `scripts/ci.sh` locally; push; GitHub CI green
- [ ] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md)
