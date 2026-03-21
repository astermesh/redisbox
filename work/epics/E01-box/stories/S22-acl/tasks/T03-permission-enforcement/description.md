# T03: Permission Enforcement

**Status:** done

Integrate ACL checks into command dispatcher. Before executing any command, verify: user is authenticated, user has permission for this command, user has access to the key(s). Return `-NOPERM this user has no permissions to run the '<cmd>' command` on denial.

## Acceptance Criteria

- Unauthorized commands rejected
- Key-level permissions work

---

[← Back](README.md)
