# T03: Hash Field Expiration

Implement HEXPIRE, HPEXPIRE, HEXPIREAT, HPEXPIREAT, HTTL, HPTTL, HPERSIST, HEXPIRETIME, HPEXPIRETIME (Redis 7.4+). Per-field TTL requires a two-level expiry index: (key, field) to timestamp. Lazy expiration checks per field on access.

## Acceptance Criteria

- Per-field TTL works correctly
- Expired fields invisible on read
- TTL commands return correct values

---

[← Back to T03](README.md)
