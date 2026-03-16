# T04: XREADGROUP, XACK, XPENDING

XREADGROUP GROUP group consumer STREAMS key >: read new messages for consumer group. Using specific ID reads from consumer's PEL. XACK: acknowledge messages (remove from PEL). XPENDING: summary form (count, min-id, max-id, consumers) and detail form with IDLE filter (Redis 6.2+).

## Acceptance Criteria

- Consumer group reads work
- Acknowledgment removes from PEL
- XPENDING returns correct data

---

[← Back](README.md)
