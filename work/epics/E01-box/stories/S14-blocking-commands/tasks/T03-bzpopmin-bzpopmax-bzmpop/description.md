# T03: BZPOPMIN, BZPOPMAX, BZMPOP

Blocking sorted set commands. Block until sorted set has data or timeout. BZPOPMIN/BZPOPMAX: return [key, element, score]. BZMPOP (Redis 7.0+): pop MIN|MAX from first non-empty set.

## Acceptance Criteria

- Blocking sorted set commands work with timeout
- Correct return format

---

[← Back](README.md)
