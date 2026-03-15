# T02: Stream Read Commands

Implement XLEN, XRANGE, XREVRANGE, XREAD. XRANGE/XREVRANGE: inclusive range by ID with optional COUNT. Special IDs: - (minimum), + (maximum). XREAD: read from multiple streams with COUNT, returns entries after specified ID. $ means "last ID at read time".

## Acceptance Criteria

- Range queries correct
- XREAD multi-stream works
- Special IDs handled

---

[← Back](README.md)
