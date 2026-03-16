# T02: Command Routing and Execution

Route parsed commands to registered handlers. Normalize command names to uppercase. Handle sub-command dispatch (two-word commands). Check client state before execution: if in MULTI mode, queue command (except EXEC/DISCARD/WATCH/MULTI); if in subscribe mode, reject non-subscribe commands. Return correct error messages matching Redis exactly.

## Acceptance Criteria

- Commands route correctly
- MULTI queuing works
- Subscribe mode restrictions enforced

---

[← Back to Tasks](../README.md)
