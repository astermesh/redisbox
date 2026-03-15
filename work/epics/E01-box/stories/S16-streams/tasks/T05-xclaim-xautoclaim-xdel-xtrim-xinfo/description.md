# T05: XCLAIM, XAUTOCLAIM, XDEL, XTRIM, XINFO

XCLAIM: transfer ownership of pending messages. XAUTOCLAIM (Redis 6.2+): combine XPENDING + XCLAIM for dead-letter processing. XDEL: mark entries as deleted (logical delete). XTRIM: trim stream by MAXLEN or MINID. XINFO STREAM [FULL], XINFO GROUPS, XINFO CONSUMERS.

## Acceptance Criteria

- Claiming works
- XAUTOCLAIM processes idle entries
- XINFO returns complete state

---

[← Back](README.md)
