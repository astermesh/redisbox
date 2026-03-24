import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { WasmoonEngine } from '../wasmoon-engine.ts';
import { applySandbox } from './sandbox.ts';

describe('bit library', () => {
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
