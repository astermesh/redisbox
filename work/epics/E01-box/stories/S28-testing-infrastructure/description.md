# S28: Testing Infrastructure

**Status:** done

Set up the differential testing infrastructure for verifying exact Redis behavioral parity. This is the foundation for the project's primary quality metric: every test must produce identical results against both RedisBox and real Redis.

## Scope

- Dual-backend test harness (same test runs against RedisBox and real Redis, compares responses)
- Response comparison utilities (exact match on values, types, error messages, side effects)
- Redis TCL test suite integration (run Redis's own tests against RedisBox in external server mode)
- CI pipeline with real Redis for automated parity verification
- Parity pass rate tracking and reporting

## Dependencies

- S00 (project setup — testing framework must exist)
- S01-S05 (basic engine must work for initial differential tests)

## Design Reference

Follows the fakeredis-py model: every test case runs against both fake and real Redis. Same test, two backends, compare results. See ADR-04 for rationale.

## Tasks

1. T01 — Dual-backend test harness
2. T02 — Redis TCL test suite integration
3. T03 — CI pipeline for parity verification

---

[← Back](README.md)
