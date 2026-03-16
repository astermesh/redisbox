# T02: Inline Command Parser

Parse inline commands (plain text without RESP framing) for redis-cli and telnet compatibility. Detect inline vs multibulk by checking first byte (not `*` = inline). Max inline length 1KB (64KB since Redis 7.2).

## Parsing Rules

Redis server inline parsing (matching `sdssplitargs` in Redis source):

- Split arguments on whitespace (spaces and tabs)
- **Double-quoted strings**: content between `"..."`. Supports escape sequences:
  - `\\` → backslash, `\"` → double quote
  - `\n` → newline, `\r` → carriage return, `\t` → tab
  - `\a` → bell, `\b` → backspace
  - `\xNN` → hex byte (e.g., `\x00` for null byte)
- **Single-quoted strings**: content between `'...'`. Literal content, only two escapes:
  - `\\` → backslash, `\'` → single quote
- Unquoted arguments: terminated by whitespace
- Empty string: `""` or `''`

## Acceptance Criteria

- Inline commands parsed correctly (space-separated arguments)
- Double-quoted strings handled with all escape sequences (`\n`, `\r`, `\t`, `\xNN`, `\\`, `\"`)
- Single-quoted strings handled (literal content, only `\\` and `\'` escapes)
- Mixed inline/multibulk on same connection works (detect by first byte)
- Empty quoted strings produce empty arguments
- Max inline length enforced

---

[← Back to Tasks](../README.md)
