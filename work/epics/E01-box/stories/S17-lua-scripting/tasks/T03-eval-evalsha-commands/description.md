# T03: EVAL/EVALSHA Commands

**Status:** done

Implement EVAL script numkeys key [key ...] arg [arg ...]. Set up KEYS and ARGV global tables. Implement EVALSHA sha1 numkeys .... EVAL_RO and EVALSHA_RO (read-only variants, reject write commands).

## Acceptance Criteria

- Scripts execute correctly
- KEYS/ARGV accessible
- EVALSHA works for cached scripts
- NOSCRIPT error for missing scripts

---

[← Back](README.md)
