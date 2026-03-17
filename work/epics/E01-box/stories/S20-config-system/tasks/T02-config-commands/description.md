# T02: CONFIG Commands

**Status:** done

Implement CONFIG GET pattern, CONFIG SET key value, CONFIG RESETSTAT, CONFIG REWRITE (no-op, returns OK). CONFIG SET must validate and apply changes immediately (e.g., changing maxmemory triggers eviction check, changing hz adjusts timer frequency).

## Acceptance Criteria

- CONFIG GET/SET work correctly
- Changes take immediate effect

---

[← Back](README.md)
