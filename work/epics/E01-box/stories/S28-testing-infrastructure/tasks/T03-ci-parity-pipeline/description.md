# T03: CI Pipeline for Parity Verification

Set up GitHub Actions CI to run differential tests and TCL suite against real Redis on every PR.

## Details

- Add Redis service container to GitHub Actions workflow (redis:7.2 or latest stable)
- Run dual-backend test suite with real Redis available
- Run applicable Redis TCL tests in external mode
- Report parity pass rate in CI output
- Fail CI if parity rate drops below established baseline
- Cache Tcl installation for faster CI runs

## Acceptance Criteria

- CI runs differential tests against real Redis on every PR
- CI runs Redis TCL test suite against RedisBox
- Parity pass rate reported in CI output
- CI fails on parity regression

---

[← Back](README.md)
