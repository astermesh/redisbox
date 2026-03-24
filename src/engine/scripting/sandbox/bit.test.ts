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

  // ---- tobit edge cases ----

  describe('tobit edge cases', () => {
    it('tobit(-1) returns -1', async () => {
      const result = await engine.execute('return bit.tobit(-1)');
      expect(result.values).toEqual([-1]);
    });

    it('tobit(0) returns 0', async () => {
      const result = await engine.execute('return bit.tobit(0)');
      expect(result.values).toEqual([0]);
    });

    it('tobit(2147483647) returns INT32_MAX', async () => {
      const result = await engine.execute('return bit.tobit(2147483647)');
      expect(result.values).toEqual([2147483647]);
    });

    it('tobit(2147483648) returns INT32_MIN', async () => {
      const result = await engine.execute('return bit.tobit(2147483648)');
      expect(result.values).toEqual([-2147483648]);
    });
  });

  // ---- identity and algebraic properties ----

  describe('algebraic properties', () => {
    it('bnot(bnot(x)) == x', async () => {
      const result = await engine.execute(
        'return bit.bnot(bit.bnot(42)) == 42'
      );
      expect(result.values).toEqual([true]);
    });

    it('bnot(-1) returns 0', async () => {
      const result = await engine.execute('return bit.bnot(-1)');
      expect(result.values).toEqual([0]);
    });

    it('band(x, 0) returns 0', async () => {
      const result = await engine.execute('return bit.band(0xdeadbeef, 0)');
      expect(result.values).toEqual([0]);
    });

    it('band(x, -1) returns x (identity)', async () => {
      const result = await engine.execute('return bit.band(42, -1)');
      expect(result.values).toEqual([42]);
    });

    it('band(x, x) returns x', async () => {
      const result = await engine.execute('return bit.band(42, 42)');
      expect(result.values).toEqual([42]);
    });

    it('bor(x, 0) returns x (identity)', async () => {
      const result = await engine.execute('return bit.bor(42, 0)');
      expect(result.values).toEqual([42]);
    });

    it('bor(x, -1) returns -1 (all bits set)', async () => {
      const result = await engine.execute('return bit.bor(42, -1)');
      expect(result.values).toEqual([-1]);
    });

    it('bxor(x, x) returns 0', async () => {
      const result = await engine.execute('return bit.bxor(42, 42)');
      expect(result.values).toEqual([0]);
    });

    it('bxor(x, 0) returns x (identity)', async () => {
      const result = await engine.execute('return bit.bxor(42, 0)');
      expect(result.values).toEqual([42]);
    });
  });

  // ---- shift edge cases ----

  describe('shift edge cases', () => {
    it('lshift by 0 is identity', async () => {
      const result = await engine.execute('return bit.lshift(42, 0)');
      expect(result.values).toEqual([42]);
    });

    it('rshift by 0 is identity', async () => {
      const result = await engine.execute('return bit.rshift(42, 0)');
      expect(result.values).toEqual([42]);
    });

    it('arshift by 0 is identity', async () => {
      const result = await engine.execute('return bit.arshift(-42, 0)');
      expect(result.values).toEqual([-42]);
    });

    it('lshift by 31', async () => {
      const result = await engine.execute('return bit.lshift(1, 31)');
      expect(result.values).toEqual([-2147483648]); // 0x80000000 as signed
    });

    it('rshift by 31', async () => {
      const result = await engine.execute('return bit.rshift(-1, 31)');
      expect(result.values).toEqual([1]);
    });

    it('arshift by 31 on negative preserves sign', async () => {
      const result = await engine.execute('return bit.arshift(-1, 31)');
      expect(result.values).toEqual([-1]);
    });

    it('arshift by 31 on positive returns 0', async () => {
      const result = await engine.execute('return bit.arshift(1, 31)');
      expect(result.values).toEqual([0]);
    });
  });

  // ---- rotate edge cases ----

  describe('rotate edge cases', () => {
    it('rol by 0 is identity', async () => {
      const result = await engine.execute('return bit.rol(42, 0)');
      expect(result.values).toEqual([42]);
    });

    it('ror by 0 is identity', async () => {
      const result = await engine.execute('return bit.ror(42, 0)');
      expect(result.values).toEqual([42]);
    });

    it('rol by 32 is identity', async () => {
      const result = await engine.execute('return bit.rol(42, 32)');
      expect(result.values).toEqual([42]);
    });

    it('ror by 32 is identity', async () => {
      const result = await engine.execute('return bit.ror(42, 32)');
      expect(result.values).toEqual([42]);
    });

    it('rol(x, n) == ror(x, 32-n)', async () => {
      const result = await engine.execute(
        'return bit.rol(0x12345678, 5) == bit.ror(0x12345678, 27)'
      );
      expect(result.values).toEqual([true]);
    });
  });

  // ---- bswap edge cases ----

  describe('bswap edge cases', () => {
    it('bswap(0) returns 0', async () => {
      const result = await engine.execute('return bit.bswap(0)');
      expect(result.values).toEqual([0]);
    });

    it('bswap(-1) returns -1 (all bytes 0xFF)', async () => {
      const result = await engine.execute('return bit.bswap(-1)');
      expect(result.values).toEqual([-1]);
    });

    it('bswap(bswap(x)) == x', async () => {
      const result = await engine.execute(
        'return bit.bswap(bit.bswap(0x12345678)) == 0x12345678'
      );
      expect(result.values).toEqual([true]);
    });
  });

  // ---- tohex edge cases ----

  describe('tohex edge cases', () => {
    it('tohex(0) returns "00000000"', async () => {
      const result = await engine.execute('return bit.tohex(0)');
      expect(result.values).toEqual(['00000000']);
    });

    it('tohex(-1) returns "ffffffff"', async () => {
      const result = await engine.execute('return bit.tohex(-1)');
      expect(result.values).toEqual(['ffffffff']);
    });

    it('tohex with 1 digit', async () => {
      const result = await engine.execute('return bit.tohex(15, 1)');
      expect(result.values).toEqual(['f']);
    });
  });
});
