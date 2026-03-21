# T04: List Encoding Transitions

**Status:** done

Track list encoding: listpack for small lists (≤128 entries, each ≤64 bytes), quicklist for large lists. Implement encoding transition when thresholds exceeded. OBJECT ENCODING returns correct encoding name.

## Acceptance Criteria

- Encoding transitions happen at correct thresholds
- OBJECT ENCODING correct

---

[← Back to T04](README.md)
