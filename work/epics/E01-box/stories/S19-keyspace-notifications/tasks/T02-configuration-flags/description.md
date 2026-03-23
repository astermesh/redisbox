# T02: Configuration Flags

**Status:** done

Parse notify-keyspace-events config string: K (keyspace), E (keyevent), g (generic), $ (string), l (list), s (set), h (hash), z (sorted set), x (expired), e (evicted), m (key miss), t (stream), d (module), A (alias for g$lshzxet). At least K or E must be set for any notifications.

## Acceptance Criteria

- Configuration correctly enables/disables event types
- A shortcut works

---

[← Back](README.md)
