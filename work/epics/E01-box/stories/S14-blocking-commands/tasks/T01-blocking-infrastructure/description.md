# T01: Blocking Infrastructure

Build blocking command manager: per-key blocking queue (Map<key, BlockedClient[]>), signalKeyAsReady mechanism called after mutations (LPUSH, ZADD, XADD etc.), FIFO wakeup order. Process ready keys in beforeSleep phase. Re-evaluate blocking condition before serving (data may have been consumed).

## Acceptance Criteria

- Blocking queue managed correctly
- FIFO order maintained
- Re-evaluation prevents serving stale data

---

[← Back](README.md)
