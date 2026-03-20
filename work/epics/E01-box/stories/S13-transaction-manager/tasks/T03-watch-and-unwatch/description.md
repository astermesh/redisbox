# T03: WATCH and UNWATCH

**Status:** done

WATCH key [key ...]: only valid outside MULTI. Record current version of each watched key. UNWATCH: clear all watches. WATCH must detect modifications from any source: other clients, Lua scripts, expiration, eviction.

## Acceptance Criteria

- WATCH detects key changes
- UNWATCH clears watches
- Expired keys trigger WATCH

---

[← Back](README.md)
