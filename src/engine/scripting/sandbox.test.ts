import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { WasmoonEngine } from './wasmoon-engine.ts';
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

  // --- Removed globals ---

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

  // --- Preserved globals ---

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

  // --- Read-only _G ---

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

  // --- bit library ---

  describe('bit library', () => {
    it('is available', async () => {
      const result = await engine.execute('return type(bit)');
      expect(result.values).toEqual(['table']);
    });

    it('bit.tobit normalizes to 32-bit signed integer', async () => {
      const result = await engine.execute('return bit.tobit(0xffffffff)');
      expect(result.values).toEqual([-1]);
    });

    it('bit.tobit wraps large numbers', async () => {
      const result = await engine.execute('return bit.tobit(0x100000000)');
      expect(result.values).toEqual([0]);
    });

    it('bit.tohex converts to hex string', async () => {
      const result = await engine.execute('return bit.tohex(255)');
      expect(result.values).toEqual(['000000ff']);
    });

    it('bit.tohex with specified digits', async () => {
      const result = await engine.execute('return bit.tohex(255, 4)');
      expect(result.values).toEqual(['00ff']);
    });

    it('bit.tohex negative digits gives uppercase', async () => {
      const result = await engine.execute('return bit.tohex(255, -4)');
      expect(result.values).toEqual(['00FF']);
    });

    it('bit.bnot performs bitwise NOT', async () => {
      const result = await engine.execute('return bit.bnot(0)');
      expect(result.values).toEqual([-1]);
    });

    it('bit.band performs bitwise AND', async () => {
      const result = await engine.execute('return bit.band(0xff, 0x0f)');
      expect(result.values).toEqual([0x0f]);
    });

    it('bit.band with multiple arguments', async () => {
      const result = await engine.execute('return bit.band(0xff, 0x3f, 0x0f)');
      expect(result.values).toEqual([0x0f]);
    });

    it('bit.bor performs bitwise OR', async () => {
      const result = await engine.execute('return bit.bor(0xf0, 0x0f)');
      expect(result.values).toEqual([0xff]);
    });

    it('bit.bor with multiple arguments', async () => {
      const result = await engine.execute('return bit.bor(0x01, 0x02, 0x04)');
      expect(result.values).toEqual([0x07]);
    });

    it('bit.bxor performs bitwise XOR', async () => {
      const result = await engine.execute('return bit.bxor(0xff, 0x0f)');
      expect(result.values).toEqual([0xf0]);
    });

    it('bit.lshift shifts left', async () => {
      const result = await engine.execute('return bit.lshift(1, 8)');
      expect(result.values).toEqual([256]);
    });

    it('bit.rshift shifts right (logical)', async () => {
      const result = await engine.execute('return bit.rshift(256, 8)');
      expect(result.values).toEqual([1]);
    });

    it('bit.rshift is logical (fills with zeros)', async () => {
      const result = await engine.execute('return bit.rshift(-1, 28)');
      expect(result.values).toEqual([0x0f]);
    });

    it('bit.arshift shifts right (arithmetic)', async () => {
      const result = await engine.execute('return bit.arshift(-256, 8)');
      expect(result.values).toEqual([-1]);
    });

    it('bit.rol rotates left', async () => {
      const result = await engine.execute('return bit.rol(0x80000000, 1)');
      expect(result.values).toEqual([1]);
    });

    it('bit.ror rotates right', async () => {
      const result = await engine.execute('return bit.ror(1, 1)');
      // 1 rotated right by 1 = 0x80000000 which is -2147483648 as signed
      const result2 = await engine.execute('return bit.tobit(0x80000000)');
      expect(result.values).toEqual([result2.values[0]]);
    });

    it('bit.bswap reverses bytes', async () => {
      const result = await engine.execute('return bit.bswap(0x01020304)');
      expect(result.values).toEqual([0x04030201]);
    });
  });

  // --- cjson library ---

  describe('cjson library', () => {
    it('is available', async () => {
      const result = await engine.execute('return type(cjson)');
      expect(result.values).toEqual(['table']);
    });

    it('cjson.encode encodes a number', async () => {
      const result = await engine.execute('return cjson.encode(42)');
      expect(result.values).toEqual(['42']);
    });

    it('cjson.encode encodes a string', async () => {
      const result = await engine.execute('return cjson.encode("hello")');
      expect(result.values).toEqual(['"hello"']);
    });

    it('cjson.encode encodes boolean true', async () => {
      const result = await engine.execute('return cjson.encode(true)');
      expect(result.values).toEqual(['true']);
    });

    it('cjson.encode encodes boolean false', async () => {
      const result = await engine.execute('return cjson.encode(false)');
      expect(result.values).toEqual(['false']);
    });

    it('cjson.encode encodes an array table', async () => {
      const result = await engine.execute('return cjson.encode({1,2,3})');
      expect(result.values).toEqual(['[1,2,3]']);
    });

    it('cjson.encode encodes an object table', async () => {
      const result = await engine.execute(
        'return cjson.encode({name="test", value=42})'
      );
      const parsed = JSON.parse(result.values[0] as string);
      expect(parsed).toEqual({ name: 'test', value: 42 });
    });

    it('cjson.encode encodes nested tables', async () => {
      const result = await engine.execute(
        'return cjson.encode({arr={1,2}, obj={a="b"}})'
      );
      const parsed = JSON.parse(result.values[0] as string);
      expect(parsed).toEqual({ arr: [1, 2], obj: { a: 'b' } });
    });

    it('cjson.encode encodes cjson.null as null', async () => {
      const result = await engine.execute('return cjson.encode(cjson.null)');
      expect(result.values).toEqual(['null']);
    });

    it('cjson.decode decodes a number', async () => {
      const result = await engine.execute('return cjson.decode("42")');
      expect(result.values).toEqual([42]);
    });

    it('cjson.decode decodes a string', async () => {
      const result = await engine.execute('return cjson.decode(\'"hello"\')');
      expect(result.values).toEqual(['hello']);
    });

    it('cjson.decode decodes boolean true', async () => {
      const result = await engine.execute('return cjson.decode("true")');
      expect(result.values).toEqual([true]);
    });

    it('cjson.decode decodes boolean false', async () => {
      const result = await engine.execute('return cjson.decode("false")');
      expect(result.values).toEqual([false]);
    });

    it('cjson.decode decodes null to cjson.null', async () => {
      const result = await engine.execute(
        'return cjson.decode("null") == cjson.null'
      );
      expect(result.values).toEqual([true]);
    });

    it('cjson.decode decodes an array', async () => {
      const result = await engine.execute(
        'local t = cjson.decode("[1,2,3]"); return t[1] + t[2] + t[3]'
      );
      expect(result.values).toEqual([6]);
    });

    it('cjson.decode decodes an object', async () => {
      const result = await engine.execute(
        'local t = cjson.decode(\'{"a":"b","c":1}\'); return t.a .. t.c'
      );
      expect(result.values).toEqual(['b1']);
    });

    it('cjson.decode errors on invalid JSON', async () => {
      await expect(
        engine.execute('return cjson.decode("invalid")')
      ).rejects.toThrow();
    });

    it('round-trips a complex table', async () => {
      const result = await engine.execute(`
        local t = {name="redis", version=7, features={"scripting","pub/sub"}}
        local json = cjson.encode(t)
        local t2 = cjson.decode(json)
        return t2.name .. t2.version .. t2.features[1]
      `);
      expect(result.values).toEqual(['redis7scripting']);
    });

    it('cjson.encode encodes special characters in strings', async () => {
      const result = await engine.execute(
        'return cjson.encode("hello\\nworld")'
      );
      const parsed = JSON.parse(result.values[0] as string);
      expect(parsed).toBe('hello\nworld');
    });

    it('cjson.encode encodes empty table as object', async () => {
      const result = await engine.execute('return cjson.encode({})');
      expect(result.values).toEqual(['{}']);
    });
  });

  // --- cmsgpack library ---

  describe('cmsgpack library', () => {
    it('is available', async () => {
      const result = await engine.execute('return type(cmsgpack)');
      expect(result.values).toEqual(['table']);
    });

    it('round-trips an integer', async () => {
      const result = await engine.execute(
        'return cmsgpack.unpack(cmsgpack.pack(42))'
      );
      expect(result.values).toEqual([42]);
    });

    it('round-trips a string', async () => {
      const result = await engine.execute(
        'return cmsgpack.unpack(cmsgpack.pack("hello"))'
      );
      expect(result.values).toEqual(['hello']);
    });

    it('round-trips boolean true', async () => {
      const result = await engine.execute(
        'return cmsgpack.unpack(cmsgpack.pack(true))'
      );
      expect(result.values).toEqual([true]);
    });

    it('round-trips boolean false', async () => {
      const result = await engine.execute(
        'return cmsgpack.unpack(cmsgpack.pack(false))'
      );
      expect(result.values).toEqual([false]);
    });

    it('round-trips an array', async () => {
      const result = await engine.execute(`
        local packed = cmsgpack.pack({1, 2, 3})
        local t = cmsgpack.unpack(packed)
        return t[1] + t[2] + t[3]
      `);
      expect(result.values).toEqual([6]);
    });

    it('round-trips a map', async () => {
      const result = await engine.execute(`
        local packed = cmsgpack.pack({a="b", c=1})
        local t = cmsgpack.unpack(packed)
        return t.a .. t.c
      `);
      expect(result.values).toEqual(['b1']);
    });

    it('round-trips nil as false', async () => {
      // In Redis's cmsgpack, nil maps to msgpack nil which comes back as false
      const result = await engine.execute(`
        local packed = cmsgpack.pack(nil)
        return cmsgpack.unpack(packed)
      `);
      expect(result.values).toEqual([false]);
    });

    it('round-trips negative numbers', async () => {
      const result = await engine.execute(
        'return cmsgpack.unpack(cmsgpack.pack(-100))'
      );
      expect(result.values).toEqual([-100]);
    });

    it('round-trips floating point numbers', async () => {
      const result = await engine.execute(
        'return cmsgpack.unpack(cmsgpack.pack(3.14))'
      );
      expect(result.values[0]).toBeCloseTo(3.14);
    });

    it('round-trips nested tables', async () => {
      const result = await engine.execute(`
        local t = {arr={1,2}, obj={x="y"}}
        local packed = cmsgpack.pack(t)
        local t2 = cmsgpack.unpack(packed)
        return t2.arr[1] + t2.arr[2] .. t2.obj.x
      `);
      expect(result.values).toEqual(['3y']);
    });
  });

  // --- struct library ---

  describe('struct library', () => {
    it('is available', async () => {
      const result = await engine.execute('return type(struct)');
      expect(result.values).toEqual(['table']);
    });

    it('struct.size returns correct size for format', async () => {
      // "I" = unsigned int (4 bytes), "B" = unsigned byte (1 byte)
      const result = await engine.execute('return struct.size("BB")');
      expect(result.values).toEqual([2]);
    });

    it('round-trips unsigned bytes', async () => {
      const result = await engine.execute(`
        local packed = struct.pack("BB", 65, 66)
        local a, b = struct.unpack("BB", packed)
        return a + b
      `);
      expect(result.values).toEqual([131]);
    });

    it('round-trips unsigned 16-bit integer', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">H", 1000)
        return struct.unpack(">H", packed)
      `);
      expect(result.values).toEqual([1000]);
    });

    it('round-trips signed 32-bit integer', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">i", -12345)
        return struct.unpack(">i", packed)
      `);
      expect(result.values).toEqual([-12345]);
    });

    it('round-trips a zero-terminated string', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">s", "hello")
        return struct.unpack(">s", packed)
      `);
      // The s format packs a zero-terminated string (matching Redis struct library)
      expect(result.values).toEqual(['hello']);
    });

    it('packs and unpacks little-endian', async () => {
      const result = await engine.execute(`
        local packed = struct.pack("<H", 0x0102)
        return string.byte(packed, 1), string.byte(packed, 2)
      `);
      // Little-endian: low byte first
      expect(result.values).toEqual([0x02]);
    });

    it('packs and unpacks big-endian', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">H", 0x0102)
        return string.byte(packed, 1)
      `);
      // Big-endian: high byte first
      expect(result.values).toEqual([0x01]);
    });

    it('struct.unpack returns offset as last value', async () => {
      const result = await engine.execute(`
        local packed = struct.pack("BB", 1, 2)
        local a, b, pos = struct.unpack("BB", packed)
        return pos
      `);
      // After reading 2 bytes, position should be 3 (1-based)
      expect(result.values).toEqual([3]);
    });

    it('round-trips float', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">f", 3.14)
        local val = struct.unpack(">f", packed)
        return math.floor(val * 100)
      `);
      expect(result.values).toEqual([314]);
    });

    it('round-trips double', async () => {
      const result = await engine.execute(`
        local packed = struct.pack(">d", 3.14159265358979)
        local val = struct.unpack(">d", packed)
        return math.floor(val * 1000000)
      `);
      expect(result.values).toEqual([3141592]);
    });
  });

  // --- PRNG (math.random / math.randomseed) ---

  describe('PRNG (redisLrand48)', () => {
    it('math.random returns a number', async () => {
      const result = await engine.execute('return type(math.random())');
      expect(result.values).toEqual(['number']);
    });

    it('math.random() returns value in [0,1)', async () => {
      const result = await engine.execute(`
        local ok = true
        for i = 1, 100 do
          local v = math.random()
          if v < 0 or v >= 1 then ok = false end
        end
        return ok
      `);
      expect(result.values).toEqual([true]);
    });

    it('math.random(n) returns value in [1,n]', async () => {
      const result = await engine.execute(`
        local ok = true
        for i = 1, 100 do
          local v = math.random(10)
          if v < 1 or v > 10 then ok = false end
        end
        return ok
      `);
      expect(result.values).toEqual([true]);
    });

    it('math.random(m,n) returns value in [m,n]', async () => {
      const result = await engine.execute(`
        local ok = true
        for i = 1, 100 do
          local v = math.random(5, 15)
          if v < 5 or v > 15 then ok = false end
        end
        return ok
      `);
      expect(result.values).toEqual([true]);
    });

    it('math.randomseed resets PRNG state', async () => {
      const result = await engine.execute(`
        math.randomseed(42)
        local a = math.random()
        math.randomseed(42)
        local b = math.random()
        return a == b
      `);
      expect(result.values).toEqual([true]);
    });

    it('produces deterministic sequence with default state', async () => {
      // Two separate engine instances with same init should produce same sequence
      const engine2 = await WasmoonEngine.create();
      await applySandbox(engine2);

      try {
        const result1 = await engine.execute(`
          math.randomseed(0)
          local vals = {}
          for i = 1, 5 do vals[i] = math.random(1000000) end
          return vals[1] .. "," .. vals[2] .. "," .. vals[3] .. "," .. vals[4] .. "," .. vals[5]
        `);
        const result2 = await engine2.execute(`
          math.randomseed(0)
          local vals = {}
          for i = 1, 5 do vals[i] = math.random(1000000) end
          return vals[1] .. "," .. vals[2] .. "," .. vals[3] .. "," .. vals[4] .. "," .. vals[5]
        `);
        expect(result1.values).toEqual(result2.values);
      } finally {
        engine2.close();
      }
    });

    it('produces Redis-compatible sequence with seed 0', async () => {
      // These values are verified against real Redis 7.x
      // redis> EVAL "math.randomseed(0); return math.random(1000000)" 0
      // The redisLrand48 with seed=0 has initial state {0x330E, 0x0000, 0x0000}
      // First call to lrand48 advances state and returns upper 31 bits
      const result = await engine.execute(`
        math.randomseed(0)
        local v1 = math.random(1000000)
        local v2 = math.random(1000000)
        local v3 = math.random(1000000)
        return v1 .. "," .. v2 .. "," .. v3
      `);
      // Verify against known Redis output
      // With seed 0: state = 0x00000000330E
      // After 1st advance: state = (0x5DEECE66D * 0x330E + 0xB) & 0xFFFFFFFFFFFF
      // = 0x130E94CC8B2 -> lrand48 result = state >> 17 = 0x130E94CC8B2 >> 17
      // Redis math.random(n) = 1 + floor(lrand48_result * n / (2^31))
      expect(result.values[0]).toBe('170829,749902,96372');
    });

    it('produces Redis-compatible sequence with default per-EVAL state', async () => {
      // Redis calls redisSrand48(0) before every EVAL for deterministic replication.
      // applySandbox initializes with srand48(0), so the default state matches.
      // This test verifies the sequence WITHOUT any explicit randomseed call.
      const result = await engine.execute(`
        local v1 = math.random(1000000)
        local v2 = math.random(1000000)
        local v3 = math.random(1000000)
        return v1 .. "," .. v2 .. "," .. v3
      `);
      // Same as seed=0 — matches Redis EVAL behavior
      expect(result.values[0]).toBe('170829,749902,96372');
    });
  });
});
