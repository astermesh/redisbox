# T06: Sorted Set Encoding Transitions

Listpack encoding for small sets (≤128 elements, each ≤64 bytes). Transition to skiplist+hashtable when threshold exceeded. OBJECT ENCODING returns `listpack` or `skiplist`.

## Acceptance Criteria

- Transitions at correct thresholds
- Encoding reported correctly

---

[← Back to T06](README.md)
