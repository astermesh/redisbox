# Redis Transactions (MULTI/EXEC)

## Connection State During MULTI

When `MULTI` is issued, the connection enters **transactional state**:
- All subsequent commands return `QUEUED` (not their usual response)
- Commands that can escape transactional state: `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`
- Commands are stored in a per-connection queue in order received

## EXEC — Atomic Execution

- Executes all queued commands sequentially as an isolated operation
- Returns an array of replies in the exact order commands were issued
- Returns `null` if the transaction was aborted due to WATCH detecting modifications
- Automatically exits transactional state and restores connection to normal mode
- No command interleaving between clients — each client has its own transaction queue

## WATCH / UNWATCH — Optimistic Locking

### WATCH
- Implements check-and-set (CAS) behavior without pessimistic locking
- Monitors watched keys for **any** modifications (writes, expirations, evictions)
- If any watched key is modified before EXEC, the entire transaction is canceled (EXEC returns null)
- Can watch multiple keys across multiple WATCH calls
- Must be called **before** MULTI

### UNWATCH
- Flushes all watched keys without executing commands
- Both EXEC and DISCARD automatically unwatch all keys (regardless of success/failure)
- Client disconnect also automatically unwatches

## DISCARD

- Discards all queued commands without executing
- Exits transactional mode, restores normal state
- Unwatches all watched keys
- No side effects

## Error Handling

### Pre-EXEC Errors (Queue-Time) — Redis 2.6.5+

Errors detected during queueing:
- Syntax errors (wrong number of arguments, unknown command)
- Out of memory conditions

Behavior:
- Server remembers the error
- EXEC returns an error reply and automatically discards the entire transaction
- No commands execute at all

### Execution-Time Errors (Post-EXEC)

Errors that occur during command execution:
- Type errors (e.g., `LPOP` on a string key)
- Index out of range, etc.

Behavior:
- These errors do **NOT** stop the transaction
- EXEC returns an array with mixed results — successful commands show results, failed commands show error messages
- Other commands continue executing normally
- **Redis does NOT provide rollback** — if command N fails, commands N+1...M still execute

### Design Philosophy

No rollbacks by design — transaction rollback would significantly impact simplicity and performance. Redis errors only happen due to programming mistakes (wrong command on wrong type), not runtime failures. Lua scripts provide similar transactional behavior when rollback-like semantics are needed.

## Client Disconnect During MULTI

- **Before EXEC**: Transaction is lost, no commands execute
- **After EXEC**: Commands have been submitted for execution
- **Reconnection**: Client must issue new MULTI and re-queue — no automatic recovery
- **WATCH cleanup**: All watched keys automatically unwatched on disconnect

## Thread Safety

If multiple threads call MULTI/EXEC on the same connection concurrently, connection state is destroyed. Each thread must use its own connection (connection pooling).

## Implications for RedisBox Node Simulator

Per-connection state to track:
- `in_transaction` flag (set by MULTI, cleared by EXEC/DISCARD)
- `command_queue` — ordered list of queued commands
- `watched_keys` — set of keys being monitored, with their values/versions at WATCH time
- `transaction_error` — flag if a queue-time error occurred (EXEC should fail)

Key behaviors:
- QUEUED response for every command after MULTI
- Atomic sequential execution on EXEC (no interleaving)
- Null response from EXEC if any watched key was modified
- Mixed success/error array from EXEC if execution errors occur
- Auto-unwatch on EXEC, DISCARD, and disconnect

---

[← Back to Node Simulator Research](README.md)
