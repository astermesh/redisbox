# T01: User Model and AUTH

**Status:** done

Define user model: username, password hash, enabled/disabled, allowed commands, allowed keys. Default user: "default", all permissions, optional password via requirepass. AUTH [username] password: validate credentials. Support both old-style (AUTH password) and new-style (AUTH username password).

## Acceptance Criteria

- AUTH validates correctly
- Default user works with requirepass

---

[← Back](README.md)
