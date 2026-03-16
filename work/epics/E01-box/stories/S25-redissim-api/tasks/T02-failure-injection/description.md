# T02: Failure Injection

Implement injectLatency(ms, options?{commands?}), injectError(error, options?{commands?, probability?}), simulateSlowCommand(command, durationMs). Latency added before command execution via pre-phase delay. Errors returned via pre-phase fail.

## Acceptance Criteria

- Latency affects command execution time
- Errors returned for configured commands
- Probability-based injection works

---

[← Back](README.md)
