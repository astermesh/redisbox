# T05: TTL Commands

**Status:** done

Implement EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT (with NX|XX|GT|LT flags, Redis 7.0+), TTL, PTTL, EXPIRETIME, PEXPIRETIME. Return -1 for keys without TTL, -2 for non-existent keys.

## Acceptance Criteria

- All TTL commands work correctly
- Flags applied properly
- Return values match Redis

---

[← Back to Tasks](../README.md)
