# T04: Subscriber Mode Restrictions

When client has active subscriptions, restrict to: SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PING, RESET, QUIT. All other commands return error: `ERR Can't execute '<cmd>': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context`.

## Acceptance Criteria

- Restricted commands rejected with correct error message

---

[← Back](README.md)
