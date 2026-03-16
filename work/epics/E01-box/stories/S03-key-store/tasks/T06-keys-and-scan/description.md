# T06: KEYS and SCAN

Implement KEYS pattern (glob matching), SCAN cursor [MATCH pattern] [COUNT hint] [TYPE type]. SCAN uses cursor-based iteration returning 0 when complete. KEYS supports glob patterns: *, ?, [abc], [^abc], \.

## Acceptance Criteria

- KEYS matches Redis glob behavior
- SCAN iterates all keys with correct cursor semantics

---

[← Back to Tasks](../README.md)
