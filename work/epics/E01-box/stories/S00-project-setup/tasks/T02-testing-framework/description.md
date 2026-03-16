# T02: Testing Framework Setup

Set up vitest as the testing framework with configuration for unit and integration tests.

## Details

- Install and configure vitest
- Create vitest.config.ts with TypeScript support
- Set up test scripts in package.json (test, test:watch, test:coverage)
- Create a sample test to verify the setup works
- Configure coverage reporting (v8 or istanbul provider)
- Tests are co-located with source code (foo.ts → foo.test.ts)

## Acceptance Criteria

- `npm run test` runs all tests and exits with correct code
- `npm run test:watch` runs in watch mode
- Coverage reporting works
- TypeScript files are tested without pre-compilation step

---

[← Back](README.md)
