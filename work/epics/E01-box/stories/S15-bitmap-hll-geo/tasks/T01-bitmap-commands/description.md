# T01: Bitmap Commands

Implement SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP (AND|OR|XOR|NOT), BITFIELD. Bitmaps are string values treated as bit arrays. SETBIT auto-extends string with zero bytes. BITCOUNT/BITPOS support byte ranges with BYTE|BIT unit (Redis 7.0+). BITFIELD supports GET/SET/INCRBY sub-commands with type specifiers (u8, i16, etc.) and overflow handling (WRAP, SAT, FAIL).

## Acceptance Criteria

- All bitmap operations work on string values
- BITFIELD overflow modes correct

---

[← Back](README.md)
