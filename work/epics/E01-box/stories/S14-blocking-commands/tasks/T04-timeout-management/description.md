# T04: Timeout Management

**Status:** done

Implement timeout handling for blocked clients. Use virtual-time-aware timers. On timeout, return nil/empty result and remove from blocking queue. Clean up blocking state on client disconnect.

## Acceptance Criteria

- Timeouts fire correctly
- Cleanup on disconnect
- Virtual time integration

---

[← Back](README.md)
