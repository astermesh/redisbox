# S00: Project Setup

**Status:** done

Set up the TypeScript project with build system, testing framework, and code quality tooling. This must be completed before any implementation work begins.

## Scope

- TypeScript configuration (tsconfig.json for library targeting Node.js)
- Build system (tsup or unbuild for dual CJS/ESM output)
- Testing framework (vitest — fast, TypeScript-native, compatible with Node.js test patterns)
- Code quality (eslint with TypeScript rules, prettier for formatting)
- Package.json scripts (build, test, lint, typecheck, format)
- Source directory structure (src/ with initial module layout)
- CI configuration (GitHub Actions for lint + typecheck + test on PRs)

## Out of Scope

- Actual Redis implementation (starts in S01)
- npm publishing configuration (future)

## Tasks

1. T01 — Project setup (TypeScript, build, testing, code quality, CI)

---

[← Back](README.md)
