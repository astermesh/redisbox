# T03: HELLO and AUTH

**Status:** done

Connection setup commands.

## HELLO

`HELLO [protover] [AUTH username password] [SETNAME name]` — negotiate protocol version, optionally authenticate and set name.

For now only RESP2 (protover 2) supported. `HELLO 3` returns `-NOPROTO sorry, this protocol version is not supported` (client should retry with lower version).

**Response format in RESP2** — flat array of alternating key-value pairs:

```
 1) "server"
 2) "redis"
 3) "version"
 4) "7.2.0"
 5) "proto"
 6) (integer) 2
 7) "id"
 8) (integer) <client-id>
 9) "mode"
10) "standalone"
11) "role"
12) "master"
13) "modules"
14) (empty array)
```

`HELLO` without arguments returns current connection properties (since Redis 6.2).

## AUTH

`AUTH [username] password` — authenticate client. Default: single user ("default") with optional password via `requirepass` config. Old-style `AUTH password` (single arg) authenticates as "default" user.

## Acceptance Criteria

- HELLO returns server info as flat array with all 7 fields (server, version, proto, id, mode, role, modules)
- HELLO 2 succeeds and returns proto=2
- HELLO 3 returns `-NOPROTO` error
- HELLO with invalid AUTH returns error but connection remains in current protocol mode
- HELLO without arguments returns current connection properties
- AUTH validates password correctly
- AUTH with wrong password returns `-ERR invalid password` (or `-WRONGPASS invalid username-password pair or user is disabled` with ACL)
- AUTH with username+password works (since Redis 6.0)

---

[← Back to T03](README.md)
