# T01: Core Hash Commands

Implement HSET (variadic), HGET, HMSET, HMGET, HGETALL, HDEL, HEXISTS, HLEN, HKEYS, HVALS, HSETNX. Track encoding: listpack for small hashes (≤128 fields, each ≤64 bytes), hashtable for large hashes. HSET returns count of new fields (not updated).

## Acceptance Criteria

- All core hash commands work
- Encoding transitions happen at correct thresholds

---

[← Back to T01](README.md)
