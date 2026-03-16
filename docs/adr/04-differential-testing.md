# ADR-04: Differential Testing for Parity Verification

## Status

Accepted

## Context

RedisBox must match real Redis behavior exactly. Behavioral differences are bugs. A testing strategy is needed that systematically detects divergences.

Approaches considered:
- **Manual test writing** — write tests based on Redis docs, run against RedisBox only
- **Differential testing** — run identical tests against both RedisBox and real Redis, compare responses
- **Redis TCL test suite** — adapt Redis's own test suite to run against RedisBox in external mode

## Decision

**Differential testing** as the primary parity verification technique, supplemented by Redis TCL test suite adaptation.

## Rationale

- Manual tests based on docs miss edge cases — Redis behavior is defined by implementation, not just documentation
- Differential testing catches any divergence automatically: return values, error messages, side effects, encoding transitions
- fakeredis-py (Python Redis emulator) uses this exact approach successfully — every test runs against both fake and real Redis
- Redis TCL test suite provides ~2000+ test cases covering edge cases we might never think to test
- The combination (our differential tests + Redis TCL suite) provides the highest confidence in parity

## Implementation

1. Dual-backend test harness: each test case runs against both RedisBox and a real Redis instance
2. Response comparison: exact match on return values, error messages, type, structure
3. Side-effect verification: key state, TTL, encoding checked after each operation
4. CI pipeline runs real Redis alongside RedisBox for every PR
5. Track parity pass rate as the primary coverage metric

## Consequences

- CI requires a real Redis instance (Docker or installed binary)
- Tests are slower (two executions per test case)
- Some Redis behaviors are non-deterministic (RANDOMKEY, SRANDMEMBER) — need comparison logic that accounts for this
- Redis TCL suite adaptation requires Tcl runtime in CI and "heavy modification" per Redis maintainer guidance

---

[← Back to ADRs](README.md)
