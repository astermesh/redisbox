# T01: Active Expiration Cycle

**Status:** done

Implement the slow expiration cycle running at `hz` frequency (default 10/sec). For each database with keys-with-TTL: sample 20 random keys from the expiry index, delete all expired, repeat if >25% were expired. Time budget: ~25ms at hz=10.

## Acceptance Criteria

- Expired keys proactively deleted
- Cycle respects time budget
- Sampling rate correct

---

[← Back](README.md)
