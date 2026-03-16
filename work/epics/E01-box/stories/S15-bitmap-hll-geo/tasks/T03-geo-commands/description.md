# T03: Geo Commands

Implement GEOADD, GEOPOS, GEODIST, GEOSEARCH, GEOSEARCHSTORE, GEOHASH, GEORADIUS, GEORADIUSBYMEMBER, GEORADIUS_RO, GEORADIUSBYMEMBER_RO. Geo uses sorted sets with 52-bit geohash as score. GEOSEARCH (Redis 6.2+) unified interface: FROMMEMBER|FROMLONLAT, BYRADIUS|BYBOX, ASC|DESC, COUNT, WITHCOORD, WITHDIST, WITHHASH. Longitude -180 to 180, latitude -85.05112878 to 85.05112878. Distance units: m, km, mi, ft.

## Acceptance Criteria

- Geohash encoding correct
- Radius/box queries return correct results
- Distance calculations accurate

---

[← Back](README.md)
