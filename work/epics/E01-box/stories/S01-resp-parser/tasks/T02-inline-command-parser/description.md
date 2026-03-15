# T02: Inline Command Parser

Parse inline commands (plain text without RESP framing) for redis-cli and telnet compatibility. Detect inline vs multibulk by checking first byte (not `*` = inline). Split on spaces, handle quoted strings with double quotes. Max inline length 1KB.

## Acceptance Criteria

- Inline commands parsed correctly
- Quoted strings handled
- Mixed inline/multibulk on same connection works

---

[← Back to Tasks](../README.md)
