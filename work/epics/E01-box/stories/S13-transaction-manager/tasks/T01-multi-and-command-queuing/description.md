# T01: MULTI and Command Queuing

**Status:** done

Implement MULTI command that enters transaction mode. All subsequent commands (except EXEC, DISCARD, WATCH, MULTI) are validated for syntax (arity, command existence) and queued. Server responds +QUEUED for each queued command. If command has syntax error, mark transaction for abort. Nested MULTI returns error but does not abort.

## Acceptance Criteria

- Commands queued correctly
- Syntax errors detected at queue time
- +QUEUED response sent

---

[← Back](README.md)
