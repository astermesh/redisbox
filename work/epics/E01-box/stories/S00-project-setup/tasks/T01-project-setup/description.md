# T01: Project Setup

Set up TypeScript project with build system, testing framework, code quality tooling, and CI pipeline.

## Details

**TypeScript and build:**

- tsconfig.json targeting ES2022 with strict mode
- Build system (tsup) for dual CJS/ESM output
- package.json with proper main/module/types/exports fields
- src/ directory with initial entry point (src/index.ts)

**Testing:**

- vitest configured with TypeScript support
- Test scripts in package.json (test, test:watch)
- Tests co-located with source code (foo.ts → foo.test.ts)

**Code quality and CI:**

- eslint with TypeScript plugin (@typescript-eslint)
- prettier for formatting
- npm scripts: lint, format, format:check, typecheck
- GitHub Actions workflow: lint, typecheck, and test on every PR

## Acceptance Criteria

- `npm run build` produces working CJS and ESM bundles
- `npm run test` runs all tests and exits with correct code
- `npm run lint` checks all source files
- `npm run typecheck` runs tsc --noEmit
- `npm run format:check` verifies formatting
- CI runs all checks on pull requests and fails if any check fails

---

[← Back](README.md)
