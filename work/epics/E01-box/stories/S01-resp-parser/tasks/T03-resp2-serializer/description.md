# T03: RESP2 Serializer

Encode Redis response values into RESP2 wire format. Encode simple strings, errors, integers, bulk strings, arrays (including nested), null bulk string, null array.

## Acceptance Criteria

- All RESP2 types serialize correctly
- Output matches Redis byte-for-byte

---

[← Back to Tasks](../README.md)
