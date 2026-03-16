# T02: IBI Hooks

Implement generic command hook (redis:command) on every command with CommandCtx (command, args, clientId, db, meta). Implement per-command-family hooks: redis:string:read, redis:string:write, redis:hash:read, redis:hash:write, redis:list:read, redis:list:write, redis:set:read, redis:set:write, redis:zset:read, redis:zset:write, redis:stream:read, redis:stream:write, redis:pubsub, redis:tx, redis:script, redis:key, redis:server, redis:connection.

## Acceptance Criteria

- Hooks fire for correct command families
- Context populated correctly
- Sim can intercept

---

[← Back](README.md)
