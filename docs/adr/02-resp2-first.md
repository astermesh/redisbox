# ADR-02: RESP2 Protocol First

## Status

Accepted

## Context

Redis supports two wire protocol versions: RESP2 (default since Redis 1.2) and RESP3 (opt-in since Redis 6.0). Both must eventually be supported for full parity.

## Decision

Implement RESP2 first. RESP3 deferred to a future epic.

## Rationale

- All major JS Redis clients (ioredis, node-redis) default to RESP2
- RESP2 covers 100% of Redis commands — no command requires RESP3
- RESP2 has only 5 data types (simple string, error, integer, bulk string, array) — simpler to implement and test
- RESP3 adds 8 new types (null, boolean, double, big number, bulk error, verbatim string, map, set, push, attribute) — significant additional complexity
- RESP3 is primarily needed for client-side caching (push notifications) and richer type information
- The HELLO command negotiates protocol version — HELLO 3 can return an error until RESP3 is implemented

## Consequences

- HELLO 3 returns `-NOPROTO` error
- Client-side caching (CLIENT TRACKING with push notifications) will work in redirect mode only, not inline push mode
- Some response types are less precise (e.g., maps returned as flat arrays, booleans as integers)

---

[← Back to ADRs](README.md)
