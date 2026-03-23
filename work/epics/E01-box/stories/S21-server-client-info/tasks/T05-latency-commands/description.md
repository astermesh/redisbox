# T05: LATENCY Commands

**Status:** done

Implement the LATENCY monitoring subsystem (since Redis 2.8.13). Tracks latency spikes for various server events.

## Commands

- **LATENCY LATEST**: return the latest latency samples for all monitored events. Each entry: [event-name, timestamp-of-latest, latest-latency-ms, all-time-max-latency-ms].
- **LATENCY HISTORY event**: return timestamp-latency pairs for a specific event.
- **LATENCY RESET [event ...]**: clear latency data for specified events (or all if none specified). Returns count of reset events.
- **LATENCY GRAPH event**: return ASCII art graph of latency samples for an event.
- **LATENCY DOCTOR**: return human-readable analysis report of latency issues.
- **LATENCY HELP**: return list of LATENCY subcommands.

## Details

- Latency events are recorded when they exceed the `latency-monitor-threshold` config (default 0 = disabled)
- Events include: command processing, fork operations, expiration cycles, AOF operations, etc.
- Each event stores up to 160 samples (oldest evicted)
- In the RedisBox emulator, the relevant events are: `command` (slow commands), `fast-command`, `expire-cycle`

## Acceptance Criteria

- LATENCY LATEST returns correct format with all monitored events
- LATENCY HISTORY returns samples for a specific event
- LATENCY RESET clears data correctly
- LATENCY GRAPH produces readable output
- LATENCY DOCTOR returns analysis text
- LATENCY HELP lists all subcommands
- Events recorded when exceeding configured threshold

---

[← Back](README.md)
