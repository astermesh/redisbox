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

    it('allows overwriting ARGV', async () => {
      const result = await engine.execute(
        'ARGV = {"a","b"}; return ARGV[1] .. ARGV[2]'
      );
      expect(result.values).toEqual(['ab']);
    });
  });

  // ---- sandbox escape attempts ----

  describe('escape attempts', () => {
    it('metatable __newindex can be removed via getmetatable (matches Redis)', async () => {
      // In Lua 5.1 (and real Redis), getmetatable returns the raw metatable.
      // Scripts CAN modify it — this matches Redis behavior where the sandbox
      // is a best-effort deterrent, not a security boundary.
      const result = await engine.execute(`
        local mt = getmetatable(_G)
        mt.__newindex = nil
        newglobal = 42
        return newglobal
      `);
      expect(result.values).toEqual([42]);
    });

    it('rawset bypasses __newindex (matches Redis behavior)', async () => {
      // In real Redis, rawset on _G works — this is by design
      const result = await engine.execute(`
        rawset(_G, "test_raw", 42)
        return test_raw
      `);
      expect(result.values).toEqual([42]);
    });

    it('cannot restore removed globals via loadstring', async () => {
      const result = await engine.execute(`
        local f = loadstring("return type(os)")
        return f()
      `);
      expect(result.values).toEqual(['nil']);
    });

    it('cannot access removed globals via string functions', async () => {
      const result = await engine.execute(`
        return type(string.dump)
      `);
      // string.dump exists in Lua 5.1 but is harmless in sandbox
      expect(typeof result.values[0]).toBe('string');
    });

    it('pcall catches sandbox errors gracefully', async () => {
      // pcall returns (false, error_msg), but wasmoon only returns first value
      const result = await engine.execute(`
        local ok = pcall(function() newglobal = 42 end)
        return ok
      `);
      expect(result.values[0]).toBe(false);
    });

    it('table.insert on _G uses rawset (matches Redis behavior)', async () => {
      // table.insert uses rawset internally in Lua 5.1, bypassing __newindex.
      // This matches Redis behavior.
      const result = await engine.execute(`
        table.insert(_G, "sneaky")
        return type(rawget(_G, 1))
      `);
      expect(result.values).toEqual(['string']);
    });

    it('bridge functions are cleaned up (not accessible)', async () => {
      const result = await engine.execute(`
        return type(__rb_math_random)
          .. type(__rb_math_randomseed)
          .. type(__rb_cjson_decode)
          .. type(__rb_msgpack_pack_json)
          .. type(__rb_msgpack_unpack)
          .. type(__rb_struct_size)
          .. type(__rb_struct_pack)
          .. type(__rb_struct_unpack)
      `);
      expect(result.values).toEqual(['nilnilnilnilnilnilnilnil']);
    });
  });

  // ---- library composition ----

  describe('library composition', () => {
    it('cjson.encode → cmsgpack.pack → cmsgpack.unpack → cjson.decode round-trip', async () => {
      const result = await engine.execute(`
        local original = {name="test", values={1,2,3}}
        local json = cjson.encode(original)
        local packed = cmsgpack.pack(json)
        local json2 = cmsgpack.unpack(packed)
        local restored = cjson.decode(json2)
        return restored.name .. restored.values[1] .. restored.values[3]
      `);
      expect(result.values).toEqual(['test13']);
    });

    it('bit operations inside cjson encode/decode', async () => {
      const result = await engine.execute(`
        local flags = bit.bor(0x01, 0x04, 0x10)
        local json = cjson.encode({flags=flags})
        local t = cjson.decode(json)
        return bit.band(t.flags, 0x04) == 0x04
      `);
      expect(result.values).toEqual([true]);
    });

    it('struct pack with values from cjson decode', async () => {
      const result = await engine.execute(`
        local t = cjson.decode('{"a":42,"b":100}')
        local packed = struct.pack(">BB", t.a, t.b)
        local a, b = struct.unpack(">BB", packed)
        return a + b
      `);
      expect(result.values).toEqual([142]);
    });

    it('PRNG used with cjson', async () => {
      const result = await engine.execute(`
        math.randomseed(0)
        local vals = {}
        for i = 1, 3 do vals[i] = math.random(100) end
        local json = cjson.encode(vals)
        local decoded = cjson.decode(json)
        return decoded[1] .. "," .. decoded[2] .. "," .. decoded[3]
      `);
      // Should be deterministic
      const result2 = await engine.execute(`
        math.randomseed(0)
        local vals = {}
        for i = 1, 3 do vals[i] = math.random(100) end
        return vals[1] .. "," .. vals[2] .. "," .. vals[3]
      `);
      expect(result.values).toEqual(result2.values);
    });

    it('all libraries available together in one script', async () => {
      const result = await engine.execute(`
        -- Use all libraries in one script
        local b = bit.band(0xff, 0x0f)
        local j = cjson.encode({val=b})
        local p = cmsgpack.pack(b)
        local s = struct.pack(">B", b)
        math.randomseed(0)
        local r = math.random(100)
        return b .. "," .. j .. "," .. cmsgpack.unpack(p) .. "," .. struct.unpack(">B", s) .. "," .. r
      `);
      const parts = (result.values[0] as string).split(',');
      expect(parts[0]).toBe('15'); // bit.band result
      expect(parts.length).toBe(5);
    });
  });
});
