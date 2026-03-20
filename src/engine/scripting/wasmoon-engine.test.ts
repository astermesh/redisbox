import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { WasmoonEngine } from './wasmoon-engine.ts';
import { LuaScriptError } from './lua-engine.ts';

describe('WasmoonEngine', () => {
  let engine: WasmoonEngine;

  beforeEach(async () => {
    engine = await WasmoonEngine.create();
  });

  afterEach(() => {
    if (!engine.closed) {
      engine.close();
    }
  });

  // --- Initialization ---

  describe('create', () => {
    it('initializes a Lua VM', () => {
      expect(engine).toBeDefined();
      expect(engine.closed).toBe(false);
    });

    it('can create multiple independent instances', async () => {
      const engine2 = await WasmoonEngine.create();
      try {
        engine.setGlobal('x', 1);
        engine2.setGlobal('x', 2);
        expect(engine.getGlobal('x')).toBe(1);
        expect(engine2.getGlobal('x')).toBe(2);
      } finally {
        engine2.close();
      }
    });
  });

  // --- Basic execution ---

  describe('execute', () => {
    it('executes basic Lua code returning a number', async () => {
      const result = await engine.execute('return 42');
      expect(result.values).toEqual([42]);
    });

    it('executes Lua code returning a string', async () => {
      const result = await engine.execute('return "hello"');
      expect(result.values).toEqual(['hello']);
    });

    it('executes Lua code returning a boolean', async () => {
      const result = await engine.execute('return true');
      expect(result.values).toEqual([true]);
    });

    it('executes Lua code returning nil (empty values)', async () => {
      const result = await engine.execute('return nil');
      expect(result.values).toEqual([null]);
    });

    it('returns empty values for statements without return', async () => {
      const result = await engine.execute('local x = 1');
      expect(result.values).toEqual([]);
    });

    it('executes arithmetic expressions', async () => {
      const result = await engine.execute('return 2 + 3 * 4');
      expect(result.values).toEqual([14]);
    });

    it('executes string concatenation', async () => {
      const result = await engine.execute('return "foo" .. "bar"');
      expect(result.values).toEqual(['foobar']);
    });

    it('executes multi-statement scripts', async () => {
      const result = await engine.execute(`
        local a = 10
        local b = 20
        return a + b
      `);
      expect(result.values).toEqual([30]);
    });

    it('executes table creation and access', async () => {
      const result = await engine.execute(`
        local t = {1, 2, 3}
        return #t
      `);
      expect(result.values).toEqual([3]);
    });
  });

  // --- Standard libraries ---

  describe('standard libraries', () => {
    it('has math library available', async () => {
      const result = await engine.execute('return math.floor(3.7)');
      expect(result.values).toEqual([3]);
    });

    it('has string library available', async () => {
      const result = await engine.execute('return string.upper("hello")');
      expect(result.values).toEqual(['HELLO']);
    });

    it('has table library available', async () => {
      const result = await engine.execute(`
        local t = {3, 1, 2}
        table.sort(t)
        return t[1]
      `);
      expect(result.values).toEqual([1]);
    });

    it('has Lua 5.1 unpack (not table.unpack)', async () => {
      const result = await engine.execute(`
        local t = {10, 20, 30}
        local a, b, c = unpack(t)
        return a + b + c
      `);
      expect(result.values).toEqual([60]);
    });

    it('has coroutine library available', async () => {
      const result = await engine.execute('return type(coroutine.create)');
      expect(result.values).toEqual(['function']);
    });

    it('has tostring and tonumber', async () => {
      const result = await engine.execute('return tonumber("42")');
      expect(result.values).toEqual([42]);
    });

    it('has pcall for error handling', async () => {
      const result = await engine.execute(`
        local ok, err = pcall(function() error("boom") end)
        return ok
      `);
      expect(result.values).toEqual([false]);
    });

    it('has type function', async () => {
      const result = await engine.execute('return type(42)');
      expect(result.values).toEqual(['number']);
    });
  });

  // --- Error handling ---

  describe('error handling', () => {
    it('throws LuaScriptError on syntax error', async () => {
      await expect(engine.execute('invalid $$$ lua')).rejects.toThrow(
        LuaScriptError
      );
    });

    it('throws LuaScriptError on runtime error', async () => {
      await expect(engine.execute('error("boom")')).rejects.toThrow(
        LuaScriptError
      );
    });

    it('includes error message in LuaScriptError', async () => {
      await expect(engine.execute('error("test error")')).rejects.toThrow(
        /test error/
      );
    });

    it('throws on undefined variable access in function call', async () => {
      await expect(engine.execute('return foo()')).rejects.toThrow(
        LuaScriptError
      );
    });
  });

  // --- Globals ---

  describe('setGlobal / getGlobal', () => {
    it('sets and gets a number global', () => {
      engine.setGlobal('mynum', 42);
      expect(engine.getGlobal('mynum')).toBe(42);
    });

    it('sets and gets a string global', () => {
      engine.setGlobal('mystr', 'hello');
      expect(engine.getGlobal('mystr')).toBe('hello');
    });

    it('sets and gets a boolean global', () => {
      engine.setGlobal('mybool', true);
      expect(engine.getGlobal('mybool')).toBe(true);
    });

    it('global is accessible from Lua script', async () => {
      engine.setGlobal('x', 100);
      const result = await engine.execute('return x + 1');
      expect(result.values).toEqual([101]);
    });

    it('Lua script can modify globals visible to JS', async () => {
      await engine.execute('myvar = 999');
      expect(engine.getGlobal('myvar')).toBe(999);
    });

    it('sets a function global callable from Lua', async () => {
      engine.setGlobal('double', (n: number) => n * 2);
      const result = await engine.execute('return double(21)');
      expect(result.values).toEqual([42]);
    });
  });

  // --- Lifecycle ---

  describe('close', () => {
    it('marks engine as closed', () => {
      expect(engine.closed).toBe(false);
      engine.close();
      expect(engine.closed).toBe(true);
    });

    it('close is idempotent', () => {
      engine.close();
      engine.close(); // should not throw
      expect(engine.closed).toBe(true);
    });

    it('execute throws after close', async () => {
      engine.close();
      await expect(engine.execute('return 1')).rejects.toThrow(LuaScriptError);
      await expect(engine.execute('return 1')).rejects.toThrow(/closed/);
    });

    it('setGlobal throws after close', () => {
      engine.close();
      expect(() => engine.setGlobal('x', 1)).toThrow(LuaScriptError);
    });

    it('getGlobal throws after close', () => {
      engine.close();
      expect(() => engine.getGlobal('x')).toThrow(LuaScriptError);
    });
  });
});
