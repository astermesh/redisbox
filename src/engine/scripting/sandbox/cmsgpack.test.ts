import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { WasmoonEngine } from '../wasmoon-engine.ts';
import { applySandbox } from './sandbox.ts';

describe('cmsgpack library', () => {
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
