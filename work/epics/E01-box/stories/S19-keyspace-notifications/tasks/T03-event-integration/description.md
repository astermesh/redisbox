# T03: Event Integration

Wire notification calls into all type engines and the expiration/eviction managers. Event names must match Redis: set, del, expire, rename_from, rename_to, lpush, rpush, hset, sadd, zadd, xadd, expired, evicted, etc.

## Acceptance Criteria

- Every key-mutating operation emits correct event name
- Expiration/eviction events emitted

---

[← Back](README.md)
