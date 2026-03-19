# T02: SELECT, DBSIZE, FLUSHDB, FLUSHALL, SWAPDB

**Status:** done

Database commands. SELECT switches database (0-15, error on out of range). DBSIZE returns key count. FLUSHDB/FLUSHALL clear keys (support ASYNC|SYNC flag, clear expiry index, unblock clients, invalidate WATCH). SWAPDB atomically swaps two databases.

## Acceptance Criteria

- All database operations work
- FLUSHDB clears all state including expiry/watches

---

[← Back to T02](README.md)
