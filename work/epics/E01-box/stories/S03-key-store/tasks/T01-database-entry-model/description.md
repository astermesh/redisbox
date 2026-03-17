# T01: Database and Entry Model

**Status:** done

Define RedisEntry interface with type, encoding, value, lruClock fields. Create Database class with Map-based store and expiry index. Create RedisEngine class holding 16 Database instances.

## Engine Dependencies (DI)

RedisEngine constructor accepts external dependencies instead of using globals:

```typescript
interface EngineDeps {
  clock: () => number   // default: Date.now — used for expiration, OBJECT IDLETIME, stream IDs, TTL
  rng: () => number     // default: Math.random — used for RANDOMKEY, SRANDMEMBER, SPOP, LRU sampling, skip list levels
}
```

All subsystems use `this.clock()` and `this.rng()` instead of `Date.now()` and `Math.random()`. This enables virtual time and deterministic replay when SimBox hooks are attached later (S24), and makes the engine testable without real time dependencies.

## Acceptance Criteria

- Entries can be stored and retrieved
- Type and encoding tracked per entry
- Engine accepts clock and rng via constructor (defaults to Date.now/Math.random)
- All time-dependent operations use the injected clock, not Date.now directly

---

[← Back to Tasks](../README.md)
