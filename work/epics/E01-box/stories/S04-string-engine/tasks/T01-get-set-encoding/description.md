# T01: GET, SET, and Encoding Tracking

Implement GET and SET with all flags: EX, PX, EXAT, PXAT, NX, XX, KEEPTTL, GET (Redis 6.2+). Track string encoding: `int` for numeric values fitting 64-bit signed integer, `embstr` for strings ≤44 bytes, `raw` for strings >44 bytes. After mutation of embstr, convert to raw.

## Acceptance Criteria

- GET/SET work with all flag combinations (EX, PX, EXAT, PXAT, NX, XX, KEEPTTL, GET)
- SET with NX returns nil when key exists, SET with XX returns nil when key does not exist
- SET with GET flag returns old value (nil if key did not exist)
- OBJECT ENCODING returns correct encoding (`int`, `embstr`, `raw`)
- Encoding transitions: `int` → `raw` when value becomes non-numeric, `embstr` → `raw` on mutation (APPEND, SETRANGE)
- Binary safety: keys and values can contain any bytes including `\0`
- Maximum value size: 512 MB

---

[← Back to Tasks](../README.md)
