# S14: Blocking Commands

**Status:** done

Implement blocking variants of list, sorted set, and stream commands. Clients block until data is available or timeout expires. Requires cross-client notification when data is pushed.

## Tasks

- T01: Blocking infrastructure
- T02: BLPOP, BRPOP, BLMOVE, BLMPOP
- T03: BZPOPMIN, BZPOPMAX, BZMPOP
- T04: Timeout management

---

[← Back](README.md)
