import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { WasmoonEngine } from '../wasmoon-engine.ts';
import { applySandbox } from './sandbox.ts';

describe('sandbox', () => {
  let engine: WasmoonEngine;

  beforeEach(async () => {
    engine = await WasmoonEngine.create();
    await applySandbox(engine);
  });

  afterEach(() => {
    if (!engine.closed) {
      engine.close();
    }
  });

  describe('removed globals', () => {
    it('removes loadfile', async () => {
      const result = await engine.execute('return type(loadfile)');
      expect(result.values).toEqual(['nil']);
    });

    it('removes dofile', async () => {
      const result = await engine.execute('return type(dofile)');
      expect(result.values).toEqual(['nil']);
    });

    it('removes require', async () => {
      const result = await engine.execute('return type(require)');
      expect(result.values).toEqual(['nil']);
    });

    it('removes print', async () => {
      const result = await engine.execute('return type(print)');
      expect(result.values).toEqual(['nil']);
    });

    it('removes io library', async () => {
      const result = await engine.execute('return type(io)');
      expect(result.values).toEqual(['nil']);
    });

    it('removes os library', async () => {
      const result = await engine.execute('return type(os)');
      expect(result.values).toEqual(['nil']);
    });

    it('removes debug library', async () => {
      const result = await engine.execute('return type(debug)');
      expect(result.values).toEqual(['nil']);
    });

    it('removes package library', async () => {
      const result = await engine.execute('return type(package)');
      expect(result.values).toEqual(['nil']);
    });

    it('removes newproxy', async () => {
      const result = await engine.execute('return type(newproxy)');
      expect(result.values).toEqual(['nil']);
    });

    it('removes module', async () => {
      const result = await engine.execute('return type(module)');
      expect(result.values).toEqual(['nil']);
    });

    it('removes setfenv', async () => {
      const result = await engine.execute('return type(setfenv)');
      expect(result.values).toEqual(['nil']);
    });

    it('removes getfenv', async () => {
      const result = await engine.execute('return type(getfenv)');
      expect(result.values).toEqual(['nil']);
    });
  });

  describe('preserved globals', () => {
    it('keeps string library', async () => {
      const result = await engine.execute('return string.upper("abc")');
      expect(result.values).toEqual(['ABC']);
    });

    it('keeps table library', async () => {
      const result = await engine.execute(
        'local t = {3,1,2}; table.sort(t); return t[1]'
      );
      expect(result.values).toEqual([1]);
    });

    it('keeps math library', async () => {
      const result = await engine.execute('return math.floor(3.7)');
      expect(result.values).toEqual([3]);
    });

    it('keeps coroutine library', async () => {
      const result = await engine.execute('return type(coroutine.create)');
      expect(result.values).toEqual(['function']);
    });

    it('keeps tostring', async () => {
      const result = await engine.execute('return tostring(42)');
      expect(result.values).toEqual(['42']);
    });

    it('keeps tonumber', async () => {
      const result = await engine.execute('return tonumber("42")');
      expect(result.values).toEqual([42]);
    });

    it('keeps pcall', async () => {
      const result = await engine.execute('return type(pcall)');
      expect(result.values).toEqual(['function']);
    });

    it('keeps xpcall', async () => {
      const result = await engine.execute('return type(xpcall)');
      expect(result.values).toEqual(['function']);
    });

    it('keeps error', async () => {
      const result = await engine.execute('return type(error)');
      expect(result.values).toEqual(['function']);
    });

    it('keeps type', async () => {
      const result = await engine.execute('return type(type)');
      expect(result.values).toEqual(['function']);
    });

    it('keeps select', async () => {
      const result = await engine.execute('return select("#", 1, 2, 3)');
      expect(result.values).toEqual([3]);
    });

    it('keeps unpack', async () => {
      const result = await engine.execute(
        'local a,b = unpack({10,20}); return a + b'
      );
      expect(result.values).toEqual([30]);
    });

    it('keeps pairs', async () => {
      const result = await engine.execute('return type(pairs)');
      expect(result.values).toEqual(['function']);
    });

    it('keeps ipairs', async () => {
      const result = await engine.execute('return type(ipairs)');
      expect(result.values).toEqual(['function']);
    });

    it('keeps next', async () => {
      const result = await engine.execute('return type(next)');
      expect(result.values).toEqual(['function']);
    });

    it('keeps rawget/rawset/rawequal', async () => {
      const result = await engine.execute(
        'return type(rawget) .. type(rawset) .. type(rawequal)'
      );
      expect(result.values).toEqual(['functionfunctionfunction']);
    });

    it('keeps setmetatable/getmetatable', async () => {
      const result = await engine.execute(
        'return type(setmetatable) .. type(getmetatable)'
      );
      expect(result.values).toEqual(['functionfunction']);
    });

    it('keeps assert', async () => {
      const result = await engine.execute('return type(assert)');
      expect(result.values).toEqual(['function']);
    });

    it('keeps collectgarbage', async () => {
      const result = await engine.execute('return type(collectgarbage)');
      expect(result.values).toEqual(['function']);
    });

    it('keeps loadstring', async () => {
      const result = await engine.execute('return type(loadstring)');
      expect(result.values).toEqual(['function']);
    });

    it('keeps _VERSION', async () => {
      const result = await engine.execute('return _VERSION');
      expect(result.values).toEqual(['Lua 5.1']);
    });

    it('keeps gcinfo', async () => {
      const result = await engine.execute('return type(gcinfo)');
      expect(result.values).toEqual(['function']);
    });
  });

  describe('read-only globals', () => {
    it('prevents creating new globals', async () => {
      await expect(engine.execute('newglobal = 42')).rejects.toThrow(
        /Script attempted to create global variable/
      );
    });

    it('prevents creating new globals with different types', async () => {
      await expect(engine.execute('myfunc = function() end')).rejects.toThrow(
        /Script attempted to create global variable/
      );
    });

    it('allows local variables', async () => {
      const result = await engine.execute('local x = 42; return x');
      expect(result.values).toEqual([42]);
    });

    it('allows modifying existing globals like KEYS and ARGV', async () => {
      // KEYS and ARGV are set per-eval, must be writable
      const result = await engine.execute('KEYS = {1,2,3}; return #KEYS');
      expect(result.values).toEqual([3]);
    });
  });
});
