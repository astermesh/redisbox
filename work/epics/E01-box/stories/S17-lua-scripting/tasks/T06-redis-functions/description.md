# T06: Redis Functions

**Status:** done

Implement FUNCTION LOAD, FUNCTION DELETE, FUNCTION LIST, FUNCTION FLUSH, FUNCTION DUMP (stub), FUNCTION RESTORE (stub), FUNCTION STATS, FCALL, FCALL_RO. Library registry stores named libraries with registered functions. redis.register_function() available during FUNCTION LOAD. Function flags: no-writes, allow-oom, allow-stale, no-cluster.

## Acceptance Criteria

- Functions load and execute correctly
- Function flags enforced
- Library management works

---

[← Back](README.md)
