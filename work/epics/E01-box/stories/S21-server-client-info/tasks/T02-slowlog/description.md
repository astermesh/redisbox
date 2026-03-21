# T02: SLOWLOG

**Status:** done

Implement SLOWLOG GET [count], SLOWLOG LEN, SLOWLOG RESET. Record commands exceeding slowlog-log-slower-than microseconds (default 10000). Store up to slowlog-max-len entries (default 128) with: id, timestamp, duration, command+args, client address, client name.

## Acceptance Criteria

- Slow commands recorded
- SLOWLOG GET returns correct entries
- SLOWLOG RESET clears

---

[← Back](README.md)
