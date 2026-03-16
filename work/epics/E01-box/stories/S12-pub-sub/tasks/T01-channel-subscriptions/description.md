# T01: Channel Subscriptions

Implement SUBSCRIBE, UNSUBSCRIBE. Server-wide channel-to-subscribers index. Per-client subscription tracking.

## Response Format

When subscribing to multiple channels in a single SUBSCRIBE command (`SUBSCRIBE ch1 ch2 ch3`), Redis sends **one response message per channel**, not one aggregated response:

```
*3\r\n$9\r\nsubscribe\r\n$3\r\nch1\r\n:1\r\n
*3\r\n$9\r\nsubscribe\r\n$3\r\nch2\r\n:2\r\n
*3\r\n$9\r\nsubscribe\r\n$3\r\nch3\r\n:3\r\n
```

Each message: `[subscribe, channelName, totalSubscriptionCount]`. The count increments with each channel.

UNSUBSCRIBE follows the same pattern — one message per channel: `[unsubscribe, channelName, remainingCount]`.

**UNSUBSCRIBE without arguments** unsubscribes from ALL channels. One message per channel is sent. When the last channel is unsubscribed (count reaches 0), the client exits subscriber mode and can execute normal commands again.

## Acceptance Criteria

- Subscribe/unsubscribe works for single and multiple channels
- Subscription count correct and incrementing per channel
- Multi-channel SUBSCRIBE sends one response per channel (not one aggregated response)
- UNSUBSCRIBE without args unsubscribes from all channels
- Client exits subscriber mode when subscription count reaches 0
- Client state tracked (per-client subscription sets)

---

[← Back](README.md)
