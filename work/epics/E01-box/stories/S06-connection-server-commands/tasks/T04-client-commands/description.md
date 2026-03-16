# T04: CLIENT Commands

## Core CLIENT Subcommands

CLIENT ID, CLIENT GETNAME, CLIENT SETNAME, CLIENT LIST, CLIENT INFO, CLIENT KILL, CLIENT PAUSE, CLIENT UNPAUSE, CLIENT REPLY, CLIENT HELP.

## Client Connection Control

- **CLIENT NO-EVICT ON|OFF** (Redis 7.0): exempt this connection from client eviction when maxmemory-clients is reached
- **CLIENT NO-TOUCH ON|OFF** (Redis 7.4): commands from this client do not update LRU/LFU metadata of accessed keys

## Client-Side Caching

- **CLIENT TRACKING ON|OFF [REDIRECT client-id] [PREFIX prefix] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]** (Redis 6.0): enable/disable server-assisted client-side caching. In RESP2, requires REDIRECT to another client for invalidation messages (push notifications require RESP3). BCAST enables broadcast mode. OPTIN/OPTOUT control per-command opt-in/out.
- **CLIENT CACHING YES|NO** (Redis 6.0): when tracking is in OPTIN or OPTOUT mode, controls whether the next command's keys are tracked. Only valid immediately before the next command.
- **CLIENT TRACKINGINFO** (Redis 6.2): return current tracking status for the connection
- **CLIENT GETREDIR** (Redis 6.2): return the client ID of the tracking redirect target

## Format

CLIENT LIST output format must match Redis exactly: `id=N addr=... fd=N name=... age=N idle=N flags=N db=N ...` (space-separated key=value pairs, one line per client).

## Acceptance Criteria

- All CLIENT subcommands work and return correct responses
- CLIENT LIST format matches Redis exactly
- CLIENT NO-EVICT and CLIENT NO-TOUCH set per-connection flags
- CLIENT TRACKING enables/disables tracking with correct flag handling
- CLIENT TRACKING in RESP2 requires REDIRECT (error without it unless BCAST mode)
- CLIENT CACHING only valid when tracking is OPTIN/OPTOUT
- CLIENT TRACKINGINFO returns current tracking configuration

---

[← Back to T04](README.md)
