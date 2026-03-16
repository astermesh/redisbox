# T03: Key Version Tracking

Add per-key version counter (monotonically incrementing number). Increment on every mutation (set, delete, expire, rename). Used by WATCH for optimistic locking.

## Acceptance Criteria

- Version increments on mutation
- Version queryable per key
- Version increments on expiration/deletion

---

[← Back to Tasks](../README.md)
