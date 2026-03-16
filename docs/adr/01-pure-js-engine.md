# ADR-01: Pure JavaScript Engine

## Status

Accepted

## Context

RedisBox needs to emulate Redis for browser and Node.js environments. Three approaches were considered:

- **Path A: Pure JS reimplementation** — implement all Redis commands in TypeScript
- **Path B: RESP proxy over embedded Redis binary** — run real Redis as subprocess, proxy traffic
- **Path C: Hybrid** — proxy on Node.js, JS engine in browser

## Decision

Pure JavaScript engine (Path A). Full reimplementation of Redis in TypeScript.

## Rationale

- **Full control**: every command passes through our code — hooks, virtual time, deterministic replay work naturally
- **No external dependencies**: no Redis binary, no subprocess, no platform-specific binaries
- **Single code path**: same engine runs on Node.js and in browser (via NodeBox)
- **SimBox integration**: IBI/OBI hooks attach directly to the engine, not to a wire protocol proxy
- **Virtual time is trivial**: engine controls its own clock, no need to hack Redis internals
- **Deterministic replay**: all randomness and time controlled at the engine level

Path B rejected because: requires platform-specific binary, no browser support, virtual time is imperfect, contradicts SimBox philosophy.

Path C rejected because: two implementations to maintain, proxy is a crutch that delays the real engine.

## Consequences

- Significant implementation effort (~460 core commands, ~3-4 months estimated)
- Must match exact Redis behavior for every command (error messages, edge cases, encoding transitions)
- Version drift risk — Redis adds commands each release, must track

---

[← Back to ADRs](README.md)
