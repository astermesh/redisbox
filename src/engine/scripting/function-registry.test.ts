import { describe, it, expect, beforeEach } from 'vitest';
import {
  FunctionRegistry,
  type Library,
  type FunctionDef,
  type FunctionFlags,
} from './function-registry.ts';

function makeFlags(overrides: Partial<FunctionFlags> = {}): FunctionFlags {
  return {
    noWrites: false,
    allowOom: false,
    allowStale: false,
    noCluster: false,
    ...overrides,
  };
}

function makeFunc(name: string, flags?: Partial<FunctionFlags>): FunctionDef {
  return { name, flags: makeFlags(flags), description: '' };
}

function makeLib(name: string, funcNames: string[], code = 'stub'): Library {
  const functions = new Map<string, FunctionDef>();
  for (const fn of funcNames) {
    functions.set(fn, makeFunc(fn));
  }
  return { name, engine: 'LUA', code, functions };
}

let registry: FunctionRegistry;

beforeEach(() => {
  registry = new FunctionRegistry();
});

describe('FunctionRegistry', () => {
  describe('addLibrary / getLibrary', () => {
    it('stores and retrieves a library', () => {
      const lib = makeLib('mylib', ['fn1']);
      registry.addLibrary(lib);
      expect(registry.getLibrary('mylib')).toBe(lib);
    });

    it('returns undefined for missing library', () => {
      expect(registry.getLibrary('nope')).toBeUndefined();
    });

    it('hasLibrary returns true after add', () => {
      registry.addLibrary(makeLib('mylib', ['fn1']));
      expect(registry.hasLibrary('mylib')).toBe(true);
      expect(registry.hasLibrary('other')).toBe(false);
    });
  });

  describe('deleteLibrary', () => {
    it('removes library and its functions', () => {
      registry.addLibrary(makeLib('mylib', ['fn1', 'fn2']));
      expect(registry.deleteLibrary('mylib')).toBe(true);
      expect(registry.hasLibrary('mylib')).toBe(false);
      expect(registry.hasFunction('fn1')).toBe(false);
      expect(registry.hasFunction('fn2')).toBe(false);
    });

    it('returns false for non-existent library', () => {
      expect(registry.deleteLibrary('nope')).toBe(false);
    });
  });

  describe('getFunction / hasFunction', () => {
    it('looks up function by name across libraries', () => {
      registry.addLibrary(makeLib('lib1', ['fn1']));
      registry.addLibrary(makeLib('lib2', ['fn2']));

      const entry = registry.getFunction('fn1');
      expect(entry).toBeDefined();
      expect(entry?.lib.name).toBe('lib1');
      expect(entry?.func.name).toBe('fn1');
    });

    it('returns undefined for missing function', () => {
      expect(registry.getFunction('nope')).toBeUndefined();
    });

    it('hasFunction returns correct values', () => {
      registry.addLibrary(makeLib('lib1', ['fn1']));
      expect(registry.hasFunction('fn1')).toBe(true);
      expect(registry.hasFunction('fn2')).toBe(false);
    });
  });

  describe('listLibraries', () => {
    it('returns all libraries when no pattern', () => {
      registry.addLibrary(makeLib('alpha', ['fn1']));
      registry.addLibrary(makeLib('beta', ['fn2']));
      expect(registry.listLibraries()).toHaveLength(2);
    });

    it('filters by glob pattern', () => {
      registry.addLibrary(makeLib('mylib', ['fn1']));
      registry.addLibrary(makeLib('other', ['fn2']));
      const result = registry.listLibraries('my*');
      expect(result).toHaveLength(1);
      expect(result[0]?.name).toBe('mylib');
    });

    it('supports ? wildcard', () => {
      registry.addLibrary(makeLib('ab', ['fn1']));
      registry.addLibrary(makeLib('ac', ['fn2']));
      registry.addLibrary(makeLib('abc', ['fn3']));
      const result = registry.listLibraries('a?');
      expect(result).toHaveLength(2);
    });

    it('escapes regex special characters in pattern', () => {
      registry.addLibrary(makeLib('a.b', ['fn1']));
      registry.addLibrary(makeLib('axb', ['fn2']));
      const result = registry.listLibraries('a.b');
      expect(result).toHaveLength(1);
      expect(result[0]?.name).toBe('a.b');
    });

    it('returns empty array when nothing matches', () => {
      registry.addLibrary(makeLib('mylib', ['fn1']));
      expect(registry.listLibraries('zzz*')).toHaveLength(0);
    });
  });

  describe('flush', () => {
    it('removes all libraries and functions', () => {
      registry.addLibrary(makeLib('lib1', ['fn1']));
      registry.addLibrary(makeLib('lib2', ['fn2']));
      registry.flush();
      expect(registry.libraryCount).toBe(0);
      expect(registry.functionCount).toBe(0);
      expect(registry.hasFunction('fn1')).toBe(false);
    });
  });

  describe('counts', () => {
    it('tracks library and function counts', () => {
      expect(registry.libraryCount).toBe(0);
      expect(registry.functionCount).toBe(0);

      registry.addLibrary(makeLib('lib1', ['fn1', 'fn2']));
      expect(registry.libraryCount).toBe(1);
      expect(registry.functionCount).toBe(2);

      registry.addLibrary(makeLib('lib2', ['fn3']));
      expect(registry.libraryCount).toBe(2);
      expect(registry.functionCount).toBe(3);

      registry.deleteLibrary('lib1');
      expect(registry.libraryCount).toBe(1);
      expect(registry.functionCount).toBe(1);
    });
  });
});
