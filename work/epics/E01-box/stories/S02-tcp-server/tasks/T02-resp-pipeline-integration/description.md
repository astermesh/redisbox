# T02: RESP Pipeline Integration

Wire the RESP parser to incoming socket data and the RESP serializer to outgoing responses. Process parsed commands through the command dispatcher. Maintain strict response ordering for pipelined commands. Handle backpressure (pause reading if write buffer is full).

## Acceptance Criteria

- Pipelined commands execute in order
- Responses match command order
- Backpressure handled

---

[← Back to Tasks](../README.md)
