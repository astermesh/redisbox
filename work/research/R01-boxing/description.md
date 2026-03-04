# R01: Boxing

**Status:** done

Technical research for building RedisBox — an in-memory Redis emulator for browser and Node.js.

## Scope

- Survey of existing Redis implementations in JS/WASM
- Redis command surface analysis (~460+ core, ~650+ with modules)
- RESP2/RESP3 wire protocol analysis
- Redis internals: expiration, eviction, pub/sub, transactions, scripting
- Architecture design for dual-mode operation (proxy + JS engine)
- Coverage strategy: paths to 100% Redis command parity

## Key Decision

**Path C (Hybrid)** — RESP proxy over real Redis binary for Node.js (100% coverage immediately), pure JS engine for browser (incremental coverage). Parity verified via Redis TCL test suite.

## Research Materials

Full research documents: [rnd/boxing](../../../rnd/boxing/README.md)

---

[← Back](README.md)
