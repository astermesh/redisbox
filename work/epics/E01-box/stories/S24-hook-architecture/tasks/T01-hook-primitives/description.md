# T01: Hook Primitives

**Status:** done

Implement AsyncHook and SyncHook types matching SimBox specification. Hook chain with next() pattern for middleware-style composition. Pre-phase decisions: continue, delay, fail, short_circuit, execute_with. Post-phase decisions: pass, transform, fail.

## Acceptance Criteria

- Hooks compose correctly
- Pre/post phases work
- Decisions applied

---

[← Back](README.md)
