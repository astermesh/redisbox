# T02: EXEC and DISCARD

EXEC: check abort flag (syntax errors during queuing lead to EXECABORT error). Check watched key versions. If any changed, return null array. Otherwise execute all queued commands atomically, return array of results. DISCARD: clear queue, clear watches, exit MULTI mode.

## Acceptance Criteria

- EXEC executes atomically
- EXECABORT on syntax errors
- Null on WATCH failure
- DISCARD clears state

---

[← Back](README.md)
