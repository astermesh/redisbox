# T03: Hash Field Active Expiration

**Status:** done

Extend active expiration to cover per-field TTL on hashes (Redis 7.4+). Sample hash fields with TTL and delete expired fields. If all fields expired, delete the key.

## Acceptance Criteria

- Expired hash fields proactively deleted
- Empty hashes cleaned up

---

[← Back](README.md)
