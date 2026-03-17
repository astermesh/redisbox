import { describe, it, expect, beforeAll } from 'vitest';
import { execSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const root = resolve(__dirname, '..');
const dist = resolve(root, 'dist');

interface PkgExportCondition {
  types: string;
  default: string;
}

interface PkgJson {
  main: string;
  module: string;
  types: string;
  exports: {
    '.': {
      import: PkgExportCondition;
      require: PkgExportCondition;
    };
  };
}

function readPkg(): PkgJson {
  return JSON.parse(
    readFileSync(resolve(root, 'package.json'), 'utf-8')
  ) as PkgJson;
}

describe('build output', () => {
  beforeAll(() => {
    execSync('npm run build', { cwd: root, stdio: 'pipe' });
  });

  describe('file existence', () => {
    it('produces ESM bundle', () => {
      expect(existsSync(resolve(dist, 'index.js'))).toBe(true);
    });

    it('produces CJS bundle', () => {
      expect(existsSync(resolve(dist, 'index.cjs'))).toBe(true);
    });

    it('produces ESM type declarations', () => {
      expect(existsSync(resolve(dist, 'index.d.ts'))).toBe(true);
    });

    it('produces CJS type declarations', () => {
      expect(existsSync(resolve(dist, 'index.d.cts'))).toBe(true);
    });

    it('produces ESM sourcemap', () => {
      expect(existsSync(resolve(dist, 'index.js.map'))).toBe(true);
    });

    it('produces CJS sourcemap', () => {
      expect(existsSync(resolve(dist, 'index.cjs.map'))).toBe(true);
    });
  });

  describe('ESM bundle content', () => {
    it('exports createRedisBox', () => {
      const content = readFileSync(resolve(dist, 'index.js'), 'utf-8');
      expect(content).toContain('createRedisBox');
    });

    it('uses ESM export syntax', () => {
      const content = readFileSync(resolve(dist, 'index.js'), 'utf-8');
      expect(content).toMatch(/export\s*\{/);
    });
  });

  describe('CJS bundle content', () => {
    it('exports createRedisBox', () => {
      const content = readFileSync(resolve(dist, 'index.cjs'), 'utf-8');
      expect(content).toContain('createRedisBox');
    });

    it('uses CJS module pattern', () => {
      const content = readFileSync(resolve(dist, 'index.cjs'), 'utf-8');
      expect(content).toMatch(/module\.exports|exports\./);
    });
  });

  describe('type declarations', () => {
    it('ESM declarations export createRedisBox', () => {
      const content = readFileSync(resolve(dist, 'index.d.ts'), 'utf-8');
      expect(content).toContain('createRedisBox');
    });

    it('CJS declarations export createRedisBox', () => {
      const content = readFileSync(resolve(dist, 'index.d.cts'), 'utf-8');
      expect(content).toContain('createRedisBox');
    });

    it('ESM declarations export RedisBoxOptions type', () => {
      const content = readFileSync(resolve(dist, 'index.d.ts'), 'utf-8');
      expect(content).toContain('RedisBoxOptions');
    });

    it('CJS declarations export RedisBoxOptions type', () => {
      const content = readFileSync(resolve(dist, 'index.d.cts'), 'utf-8');
      expect(content).toContain('RedisBoxOptions');
    });
  });

  describe('package.json consistency', () => {
    it('main field points to existing CJS file', () => {
      const pkg = readPkg();
      expect(existsSync(resolve(root, pkg.main))).toBe(true);
    });

    it('module field points to existing ESM file', () => {
      const pkg = readPkg();
      expect(existsSync(resolve(root, pkg.module))).toBe(true);
    });

    it('types field points to existing declaration file', () => {
      const pkg = readPkg();
      expect(existsSync(resolve(root, pkg.types))).toBe(true);
    });

    it('exports import path points to existing file', () => {
      const pkg = readPkg();
      expect(existsSync(resolve(root, pkg.exports['.'].import.default))).toBe(
        true
      );
    });

    it('exports require path points to existing file', () => {
      const pkg = readPkg();
      expect(existsSync(resolve(root, pkg.exports['.'].require.default))).toBe(
        true
      );
    });

    it('exports import types path points to existing file', () => {
      const pkg = readPkg();
      expect(existsSync(resolve(root, pkg.exports['.'].import.types))).toBe(
        true
      );
    });

    it('exports require types path points to existing file', () => {
      const pkg = readPkg();
      expect(existsSync(resolve(root, pkg.exports['.'].require.types))).toBe(
        true
      );
    });
  });

  describe('CJS require', () => {
    it('can be required as CommonJS', () => {
      const result = execSync(
        `node -e "const m = require('./dist/index.cjs'); console.log(typeof m.createRedisBox)"`,
        { cwd: root, encoding: 'utf-8' }
      );
      expect(result.trim()).toBe('function');
    });
  });

  describe('ESM import', () => {
    it('can be imported as ESM', () => {
      const result = execSync(
        `node -e "import('./dist/index.js').then(m => console.log(typeof m.createRedisBox))"`,
        { cwd: root, encoding: 'utf-8' }
      );
      expect(result.trim()).toBe('function');
    });
  });
});
