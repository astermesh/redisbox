# T02: Redis TCL Test Suite Integration

**Status:** done

Adapt the Redis TCL test suite to run against RedisBox in external server mode.

## Details

- Clone or vendor the Redis test suite (tests/ directory from Redis repository)
- Configure external server mode: `./runtest --host 127.0.0.1 --port <redisbox-port>`
- Identify and document which test files can run in external mode (tests tagged `external:skip` are automatically skipped)
- Create a runner script that starts RedisBox, runs applicable TCL tests, collects results
- Track pass/fail counts as the parity metric
- Document known incompatibilities and create defect items for each failing test

## Acceptance Criteria

- TCL test suite runs against RedisBox in external server mode
- Pass rate tracked and reported
- Failing tests categorized (missing command, behavioral difference, test infra issue)
- Runner script automates the full cycle (start RedisBox → run tests → report results)

---

[← Back](README.md)
