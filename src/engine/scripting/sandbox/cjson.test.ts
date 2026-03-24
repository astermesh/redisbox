import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { WasmoonEngine } from '../wasmoon-engine.ts';
import { applySandbox } from './sandbox.ts';

describe('cjson library', () => {
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
    const result = await engine.execute('return cjson.encode("hello\\nworld")');
    const parsed = JSON.parse(result.values[0] as string);
    expect(parsed).toBe('hello\nworld');
  });

  it('cjson.encode encodes empty table as object', async () => {
    const result = await engine.execute('return cjson.encode({})');
    expect(result.values).toEqual(['{}']);
  });

  // ---- encode edge cases ----

  describe('encode edge cases', () => {
    it('encodes NaN as null', async () => {
      const result = await engine.execute('return cjson.encode(0/0)');
      expect(result.values).toEqual(['null']);
    });

    it('encodes Infinity as null', async () => {
      const result = await engine.execute('return cjson.encode(math.huge)');
      expect(result.values).toEqual(['null']);
    });

    it('encodes -Infinity as null', async () => {
      const result = await engine.execute('return cjson.encode(-math.huge)');
      expect(result.values).toEqual(['null']);
    });

    it('encodes zero as integer', async () => {
      const result = await engine.execute('return cjson.encode(0)');
      expect(result.values).toEqual(['0']);
    });

    it('encodes max int32 as integer', async () => {
      const result = await engine.execute('return cjson.encode(2147483647)');
      expect(result.values).toEqual(['2147483647']);
    });

    it('encodes min int32 as integer', async () => {
      const result = await engine.execute('return cjson.encode(-2147483648)');
      expect(result.values).toEqual(['-2147483648']);
    });

    it('encodes empty string', async () => {
      const result = await engine.execute('return cjson.encode("")');
      expect(result.values).toEqual(['""']);
    });

    it('encodes string with backslash', async () => {
      const result = await engine.execute(
        'return cjson.encode("back\\\\slash")'
      );
      const parsed = JSON.parse(result.values[0] as string);
      expect(parsed).toBe('back\\slash');
    });

    it('encodes string with quotes', async () => {
      const result = await engine.execute(
        'return cjson.encode("say \\"hi\\"")'
      );
      const parsed = JSON.parse(result.values[0] as string);
      expect(parsed).toBe('say "hi"');
    });

    it('encodes string with tab', async () => {
      const result = await engine.execute('return cjson.encode("a\\tb")');
      const parsed = JSON.parse(result.values[0] as string);
      expect(parsed).toBe('a\tb');
    });

    it('encodes string with carriage return', async () => {
      const result = await engine.execute('return cjson.encode("a\\rb")');
      const parsed = JSON.parse(result.values[0] as string);
      expect(parsed).toBe('a\rb');
    });

    it('encodes table with sparse keys as object', async () => {
      // {[1]=1, [3]=3} has count=2 but max=3, so is_array returns false
      const result = await engine.execute(
        'return cjson.encode({[1]=1, [3]=3})'
      );
      const parsed = JSON.parse(result.values[0] as string);
      expect(parsed).toEqual({ '1': 1, '3': 3 });
    });

    it('encodes table with zero key as object', async () => {
      const result = await engine.execute(
        'return cjson.encode({[0]="zero", [1]="one"})'
      );
      const parsed = JSON.parse(result.values[0] as string);
      expect(parsed['0']).toBe('zero');
    });

    it('encodes table with negative key as object', async () => {
      const result = await engine.execute(
        'return cjson.encode({[-1]="neg", [1]="pos"})'
      );
      const parsed = JSON.parse(result.values[0] as string);
      expect(parsed['-1']).toBe('neg');
    });

    it('encodes deeply nested table', async () => {
      const result = await engine.execute(`
        local t = {a = {b = {c = {d = {e = 42}}}}}
        return cjson.encode(t)
      `);
      const parsed = JSON.parse(result.values[0] as string);
      expect(parsed.a.b.c.d.e).toBe(42);
    });

    it('errors on encoding a function', async () => {
      await expect(
        engine.execute('return cjson.encode(tostring)')
      ).rejects.toThrow();
    });
  });

  // ---- decode edge cases ----

  describe('decode edge cases', () => {
    it('decodes zero', async () => {
      const result = await engine.execute('return cjson.decode("0")');
      expect(result.values).toEqual([0]);
    });

    it('decodes negative number', async () => {
      const result = await engine.execute('return cjson.decode("-42")');
      expect(result.values).toEqual([-42]);
    });

    it('decodes scientific notation', async () => {
      // Use a value that fits in 32-bit integer range (wasmoon truncates large numbers)
      const result = await engine.execute('return cjson.decode("1e5")');
      expect(result.values).toEqual([1e5]);
    });

    it('decodes very small number', async () => {
      const result = await engine.execute('return cjson.decode("0.001")');
      expect(result.values[0]).toBeCloseTo(0.001);
    });

    it('decodes empty string value', async () => {
      const result = await engine.execute('return cjson.decode(\'""\')');
      expect(result.values).toEqual(['']);
    });

    it('decodes string with escape sequences', async () => {
      const result = await engine.execute(
        'return cjson.decode(\'"hello\\\\nworld"\')'
      );
      // The JSON string "hello\nworld" has a literal newline
      expect(typeof result.values[0]).toBe('string');
    });

    it('decodes empty array', async () => {
      const result = await engine.execute(`
        local t = cjson.decode("[]")
        return type(t)
      `);
      expect(result.values).toEqual(['table']);
    });

    it('decodes empty object', async () => {
      const result = await engine.execute(`
        local t = cjson.decode("{}")
        return type(t)
      `);
      expect(result.values).toEqual(['table']);
    });

    it('decodes nested arrays', async () => {
      const result = await engine.execute(`
        local t = cjson.decode("[[1,2],[3,4]]")
        return t[1][1] + t[2][2]
      `);
      expect(result.values).toEqual([5]);
    });

    it('decodes nested objects', async () => {
      const result = await engine.execute(`
        local t = cjson.decode('{"a":{"b":{"c":99}}}')
        return t.a.b.c
      `);
      expect(result.values).toEqual([99]);
    });

    it('decodes array with null preserving cjson.null', async () => {
      const result = await engine.execute(`
        local t = cjson.decode("[1,null,3]")
        return t[2] == cjson.null
      `);
      expect(result.values).toEqual([true]);
    });

    it('errors on trailing comma', async () => {
      await expect(
        engine.execute('return cjson.decode("[1,2,]")')
      ).rejects.toThrow();
    });

    it('errors on single quotes', async () => {
      await expect(
        engine.execute('return cjson.decode("{\'a\':1}")')
      ).rejects.toThrow();
    });
  });

  // ---- cjson.null identity ----

  describe('cjson.null', () => {
    it('has string representation "null"', async () => {
      const result = await engine.execute('return tostring(cjson.null)');
      expect(result.values).toEqual(['null']);
    });

    it('is a table type', async () => {
      const result = await engine.execute('return type(cjson.null)');
      expect(result.values).toEqual(['table']);
    });

    it('cjson.null == cjson.null is true', async () => {
      const result = await engine.execute('return cjson.null == cjson.null');
      expect(result.values).toEqual([true]);
    });

    it('cjson.null ~= nil', async () => {
      const result = await engine.execute('return cjson.null ~= nil');
      expect(result.values).toEqual([true]);
    });
  });
});
