# T03: Consumer Groups

Implement XGROUP CREATE, XGROUP SETID, XGROUP DELCONSUMER, XGROUP DESTROY, XGROUP CREATECONSUMER. Each group tracks: last-delivered-ID, pending entries list (PEL) per consumer. XGROUP CREATE supports MKSTREAM and entry ID or $ for latest.

## Acceptance Criteria

- Consumer groups created and managed correctly
- PEL tracking works

---

[← Back](README.md)
