# T01: Stream Data Structure and XADD

Implement stream storage: entries keyed by ID (milliseconds-sequence format), strictly increasing IDs. XADD with auto-generated IDs (*), partial auto (<ms>-*), explicit IDs. Support MAXLEN and MINID trimming with optional ~ (approximate). NOMKSTREAM flag (Redis 6.2+).

## Acceptance Criteria

- IDs strictly increasing
- Auto-generation works
- Trimming correct

---

[← Back](README.md)
