# T02: ZADD and Core Mutations

**Status:** done

Implement ZADD with all flags: NX (only add new), XX (only update existing), GT (update if new > current), LT (update if new < current), CH (return changed count). Implement ZREM, ZINCRBY, ZCARD. Maintain dual index (skip list + hash table) for large sets.

## Acceptance Criteria

- All ZADD flag combinations work
- Dual index consistent
- ZINCRBY atomic

---

[← Back to T02](README.md)
