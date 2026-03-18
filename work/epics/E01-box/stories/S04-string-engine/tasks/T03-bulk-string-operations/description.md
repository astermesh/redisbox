# T03: Bulk String Operations

**Status:** done

Implement MGET, MSET, MSETNX, APPEND, STRLEN, SETRANGE, GETRANGE (alias SUBSTR), GETEX, GETDEL, GETSET (deprecated alias), SETNX, SETEX, PSETEX, LCS. MSET is atomic, MSETNX is all-or-nothing. LCS supports LEN, IDX, MINMATCHLEN, WITHMATCHLEN options.

## Acceptance Criteria

- All commands match Redis behavior
- Atomicity preserved for MSET/MSETNX

---

[← Back to Tasks](../README.md)
