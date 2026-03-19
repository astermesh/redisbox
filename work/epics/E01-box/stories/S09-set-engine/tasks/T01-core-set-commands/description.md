# T01: Core Set Commands

**Status:** done

Implement SADD, SREM, SISMEMBER, SMISMEMBER, SMEMBERS, SCARD, SMOVE. Track encoding: intset (all integers, ≤128 members), listpack (≤128 members, each ≤64 bytes), hashtable (large or mixed).

## Acceptance Criteria

- All core commands work
- Encoding transitions correct

---

[← Back to T01](README.md)
