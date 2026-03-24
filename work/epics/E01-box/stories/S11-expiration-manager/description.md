# S11: Expiration Manager

**Status:** done

Implement active (periodic) key expiration. Lazy expiration is in S03; this story adds the background cycle that proactively deletes expired keys. Matches Redis's sampling algorithm with configurable hz frequency.

## Tasks

- T01: Active expiration cycle
- T02: Fast expiration cycle
- T03: Hash field active expiration

---

[← Back](README.md)
