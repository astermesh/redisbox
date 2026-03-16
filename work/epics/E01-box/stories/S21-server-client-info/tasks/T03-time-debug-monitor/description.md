# T03: TIME, DEBUG, MONITOR

TIME: return [unix-seconds, microseconds]. DEBUG SLEEP seconds: block server. DEBUG SET-ACTIVE-EXPIRE 0|1. DEBUG OBJECT key: return object info. MONITOR: enter monitor mode, receive copy of all processed commands in "+timestamp [db clientaddr] command args" format.

## Acceptance Criteria

- TIME returns current time
- MONITOR streams commands

---

[← Back](README.md)
