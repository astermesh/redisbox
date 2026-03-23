# T04: Script Caching and Management

**Status:** done

Implement SCRIPT LOAD (cache script, return SHA1), SCRIPT EXISTS (check cache), SCRIPT FLUSH (clear cache with ASYNC|SYNC), SCRIPT DEBUG (stub). SHA1 computed from script source. Cache is per-server, not per-database.

## Acceptance Criteria

- Scripts cached by SHA1
- SCRIPT EXISTS returns correct results
- SCRIPT FLUSH clears cache

---

[← Back](README.md)
