# T03: Code Quality Tooling and CI

Set up eslint, prettier, and GitHub Actions CI pipeline.

## Details

- Install and configure eslint with TypeScript plugin (@typescript-eslint)
- Install and configure prettier
- Create .eslintrc / eslint.config.js with project rules
- Create .prettierrc with project formatting preferences
- Add npm scripts: lint, lint:fix, format, format:check, typecheck
- Create GitHub Actions workflow: run lint, typecheck, and test on every PR
- Add .editorconfig for consistent editor settings

## Acceptance Criteria

- `npm run lint` checks all source files
- `npm run typecheck` runs tsc --noEmit
- `npm run format:check` verifies formatting
- CI runs all checks on pull requests
- CI fails if any check fails

---

[← Back](README.md)
