# T05: XCLAIM, XAUTOCLAIM, XDEL, XTRIM, XINFO, XSETID

XCLAIM: transfer ownership of pending messages. XAUTOCLAIM (Redis 6.2+): combine XPENDING + XCLAIM for dead-letter processing. XDEL: mark entries as deleted (logical delete). XTRIM: trim stream by MAXLEN or MINID. XINFO STREAM [FULL], XINFO GROUPS, XINFO CONSUMERS.

## XSETID

`XSETID key last-id [ENTRIESADDED entries-added] [MAXDELETEDID max-deleted-id]` (since Redis 5.0, options since 7.0)

Internal command used for replication, but must exist for compatibility. Sets the stream's last generated ID without adding entries. The new ID must be >= current last ID (fails otherwise). ENTRIESADDED and MAXDELETEDID update stream metadata.

## Acceptance Criteria

- Claiming works correctly (ownership transfer in PEL)
- XAUTOCLAIM processes idle entries exceeding min-idle-time
- XDEL marks entries as deleted (logical, not physical)
- XTRIM by MAXLEN and MINID with approximate (~) option
- XINFO STREAM returns stream metadata (length, first/last entry, etc.)
- XINFO STREAM FULL returns complete state including all PELs
- XINFO GROUPS returns consumer group list with pending counts
- XINFO CONSUMERS returns consumer list with idle times
- XSETID sets last ID correctly, rejects IDs less than current

---

[← Back](README.md)
