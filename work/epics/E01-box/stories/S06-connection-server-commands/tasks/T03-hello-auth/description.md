# T03: HELLO and AUTH

Connection setup commands. HELLO [protover] [AUTH username password] [SETNAME name]: negotiate protocol version, optionally authenticate and set name. For now only RESP2 (protover 2) supported; HELLO 3 returns error. AUTH [username] password: authenticate client. Default: single user with optional requirepass.

## Acceptance Criteria

- HELLO returns server info map
- AUTH validates password correctly

---

[← Back to T03](README.md)
