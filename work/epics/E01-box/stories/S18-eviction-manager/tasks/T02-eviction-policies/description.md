# T02: Eviction Policies

**Status:** done

Implement all 8 policies: noeviction (return OOM error on writes), allkeys-lru, volatile-lru, allkeys-lfu, volatile-lfu, allkeys-random, volatile-random, volatile-ttl. Pre-execution check: if maxmemory set and exceeded, run eviction before allowing write commands.

## Acceptance Criteria

- All policies select correct keys for eviction
- noeviction returns OOM error

---

[← Back](README.md)
