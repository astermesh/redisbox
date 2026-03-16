# T03: OBI Hooks

Implement time hook (redis:time) — Sim controls the clock for virtual time. Implement random hook (redis:random) — Sim controls randomness for determinism. Implement persist hook (redis:persist) — Sim controls persistence signals. Wire time hook into all time-dependent operations (expiration, OBJECT IDLETIME, etc.). Wire random hook into all random operations (RANDOMKEY, SRANDMEMBER, SPOP, etc.).

## Acceptance Criteria

- Virtual time works throughout engine
- Deterministic randomness produces repeatable results

---

[← Back](README.md)
