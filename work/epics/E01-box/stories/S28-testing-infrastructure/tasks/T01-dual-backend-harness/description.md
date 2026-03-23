# T01: Dual-Backend Test Harness

**Status:** done

Build a test utility that runs the same Redis commands against both RedisBox and a real Redis instance, then compares responses.

## Details

- Create a test helper that connects to both RedisBox (in-process) and real Redis (localhost)
- Provide a `compareCommand(command, ...args)` function that executes on both backends and asserts identical responses
- Handle response comparison: exact match for deterministic commands, set-based comparison for unordered results (SMEMBERS, KEYS)
- Handle non-deterministic commands (RANDOMKEY, SRANDMEMBER with negative count) — compare type and structure, not exact values
- Support side-effect comparison: after a command, verify key state (EXISTS, TYPE, TTL, OBJECT ENCODING) matches on both backends
- Flush both backends before each test case for isolation
- Skip comparison when real Redis is not available (allow unit-only test runs)

## Acceptance Criteria

- Test helper connects to both RedisBox and real Redis
- Deterministic commands produce byte-identical responses on both backends
- Non-deterministic commands compared by type and structure
- Side effects (key state, TTL, encoding) verified after mutations
- Tests pass when real Redis is running, skip gracefully when it is not
- Error messages compared exactly (clients parse error strings)

---

[← Back](README.md)
