# T04: Approximated LFU

**Status:** done

Implement Morris counter-based LFU: 16-bit last-decrement-time + 8-bit logarithmic frequency counter. Probabilistic increment on access (higher counter = lower increment probability). Decay based on elapsed time (lfu-decay-time config, default 1 minute).

## Acceptance Criteria

- Frequency counter tracks access patterns
- Decay works correctly

---

[← Back](README.md)
