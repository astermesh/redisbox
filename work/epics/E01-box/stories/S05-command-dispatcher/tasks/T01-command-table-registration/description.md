# T01: Command Table and Registration

Build command table with CommandDefinition entries: name, handler, arity (positive=exact, negative=minimum), flags (write, readonly, denyoom, fast, etc.), key positions (firstKey, lastKey, keyStep), ACL categories. Register all implemented commands. Support sub-commands (CLIENT LIST, CONFIG GET, etc.).

## Acceptance Criteria

- All commands registered
- Arity checked on every call
- Unknown commands return correct error

---

[← Back to Tasks](../README.md)
