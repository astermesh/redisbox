# T01: Notification Dispatch

Implement notifyKeyspaceEvent(type, event, key, dbid) called after every key mutation. Check configuration flags to determine if notification should be emitted. Publish to __keyspace@{db}__:{key} (if K flag) and/or __keyevent@{db}__:{event} (if E flag).

## Acceptance Criteria

- Notifications emitted for all configured event types
- Published to correct channels

---

[← Back](README.md)
