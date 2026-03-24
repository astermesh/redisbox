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
});
