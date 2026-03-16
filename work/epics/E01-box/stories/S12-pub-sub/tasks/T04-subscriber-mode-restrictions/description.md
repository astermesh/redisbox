# T04: Subscriber Mode Restrictions

When client has active subscriptions (channel or pattern count > 0), restrict to: SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, SSUBSCRIBE, SUNSUBSCRIBE, PING, RESET, QUIT. All other commands return error: `-ERR Can't execute '<cmd>': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context`.

## Edge Cases

- Client enters subscriber mode on first SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE
- Client exits subscriber mode when ALL subscriptions removed (channel + pattern count = 0)
- UNSUBSCRIBE without args removes all channel subscriptions; PUNSUBSCRIBE without args removes all pattern subscriptions
- Both must reach 0 for the client to exit subscriber mode
- PING in subscriber mode returns a push message `[pong, ""]` (not simple `+PONG`)

## Acceptance Criteria

- Restricted commands rejected with correct error message (exact string match)
- Client enters subscriber mode on first subscription
- Client exits subscriber mode when all subscriptions removed
- PING works in subscriber mode with correct push-style response

---

[← Back](README.md)
