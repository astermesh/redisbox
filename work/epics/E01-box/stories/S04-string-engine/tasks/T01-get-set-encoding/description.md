# T01: GET, SET, and Encoding Tracking

Implement GET and SET with all flags: EX, PX, EXAT, PXAT, NX, XX, KEEPTTL, GET (Redis 6.2+). Track string encoding: `int` for numeric values fitting 64-bit signed integer, `embstr` for strings ≤44 bytes, `raw` for strings >44 bytes. After mutation of embstr, convert to raw.

## Acceptance Criteria

- GET/SET work with all flag combinations
- OBJECT ENCODING returns correct encoding

---

[← Back to Tasks](../README.md)
