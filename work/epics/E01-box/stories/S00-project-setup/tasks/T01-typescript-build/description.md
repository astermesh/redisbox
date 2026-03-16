# T01: TypeScript and Build Configuration

Set up TypeScript compiler configuration and build pipeline for a library targeting Node.js (>=18).

## Details

- Create tsconfig.json targeting ES2022/NodeNext with strict mode
- Configure build system (tsup or unbuild) for dual CJS/ESM output
- Set up package.json with proper main/module/types/exports fields
- Create src/ directory with initial entry point (src/index.ts)
- Configure path aliases if needed

## Acceptance Criteria

- `npm run build` produces working CJS and ESM bundles
- TypeScript strict mode enabled (strict: true)
- Source maps generated for debugging
- Package exports configured correctly for both CJS and ESM consumers

---

[← Back](README.md)
