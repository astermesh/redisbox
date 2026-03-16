# S27: Geo Commands

Implement geospatial commands. Geo is built on top of sorted sets — members are stored with their 52-bit geohash as the score. This story requires a working sorted set engine (S10).

## Commands

GEOADD, GEOPOS, GEODIST, GEOSEARCH, GEOSEARCHSTORE, GEOHASH, GEORADIUS, GEORADIUSBYMEMBER, GEORADIUS_RO, GEORADIUSBYMEMBER_RO (~10 commands)

## Key Behavioral Details

- Geo uses sorted sets internally — `TYPE` returns `zset` for geo keys
- Members are stored in a sorted set with 52-bit geohash as score (26 bits longitude + 26 bits latitude, interleaved)
- Longitude range: -180 to 180 degrees
- Latitude range: -85.05112878 to 85.05112878 degrees (Mercator projection limit)
- GEOSEARCH (Redis 6.2+) is the unified interface replacing GEORADIUS/GEORADIUSBYMEMBER
- Distance units: m (meters), km (kilometers), mi (miles), ft (feet)
- GEORADIUS and GEORADIUSBYMEMBER are deprecated since Redis 6.2 but must exist for compatibility
- GEORADIUS_RO and GEORADIUSBYMEMBER_RO are read-only variants (since Redis 3.2.10)
- GEOADD supports NX, XX, CH flags (since Redis 6.2) — same as ZADD flags

## Dependencies

- S10 (sorted set engine — geo is built on sorted sets)

## Tasks

1. T01 — Geohash encoding and core commands
2. T02 — Geo search and radius queries

---

[← Back](README.md)
