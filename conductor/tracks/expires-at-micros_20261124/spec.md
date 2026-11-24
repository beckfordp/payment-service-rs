# Spec — hold expiry at microseconds

## Overview
Authorize returns the hold's `expiresAt` at the clock's precision (nanoseconds on Linux);
a replay reads it back from Postgres, which keeps microseconds, so the replay differs
from the first response. Breaks the replay contract; CI red on Linux.

## Functional requirements
- The hold's `expires_at` is truncated to microseconds where it's created
  (`SimulatedGateway::authorize`): first response, stored row, event and replay agree.

## Acceptance criteria
- Domain test with a fixed nanosecond `now` expects a microsecond expiry, 7 days on.
- `an_authorization_holds_once_per_order_and_replays` green on Linux (GitHub CI).
- `scripts/ci.sh` green, ≥90% lines.

## Out of scope
Other timestamps (event `timestamp`, `updated_at`): not compared on replay.
