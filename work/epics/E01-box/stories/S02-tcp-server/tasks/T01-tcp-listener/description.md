# T01: TCP Listener and Connection Handling

**Status:** done

Create TCP server using `net.createServer`. Listen on configurable host/port (default 127.0.0.1:6379). Accept client connections, assign unique client IDs, track active connections. Handle connection close and error events, clean up client state on disconnect.

## Acceptance Criteria

- Server listens on port
- Accepts multiple concurrent connections
- Cleans up on disconnect

---

[← Back to Tasks](../README.md)
