import { describe, it, expect } from 'vitest';
import { murmurHash64A, hllPatLen } from './hash.ts';
import { stringToBytes } from './encoding.ts';
import { HLL_REGISTERS, HLL_Q } from './encoding.ts';

describe('murmurHash64A', () => {
  it('returns a bigint', () => {
    const data = stringToBytes('hello');
    const result = murmurHash64A(data);
    expect(typeof result).toBe('bigint');
  });

  it('returns consistent hash for same input', () => {
    const data = stringToBytes('test');
    expect(murmurHash64A(data)).toBe(murmurHash64A(data));
  });

  it('returns different hashes for different inputs', () => {
    const h1 = murmurHash64A(stringToBytes('foo'));
    const h2 = murmurHash64A(stringToBytes('bar'));
    expect(h1).not.toBe(h2);
  });

  it('handles empty input', () => {
    const data = new Uint8Array(0);
    const result = murmurHash64A(data);
    expect(typeof result).toBe('bigint');
    // Empty input should still produce a valid hash
    expect(result).toBeGreaterThanOrEqual(0n);
  });

  it('handles single byte input', () => {
    const data = new Uint8Array([0x41]); // 'A'
    const result = murmurHash64A(data);
    expect(typeof result).toBe('bigint');
    expect(result).toBeGreaterThanOrEqual(0n);
  });

  it('produces 64-bit values (fits in unsigned 64-bit range)', () => {
    const inputs = ['a', 'hello world', 'testing123', ''];
    for (const input of inputs) {
      const hash = murmurHash64A(stringToBytes(input));
      expect(hash).toBeGreaterThanOrEqual(0n);
      expect(hash).toBeLessThanOrEqual(0xffffffffffffffffn);
    }
  });

  it('handles input exactly 8 bytes (one full block, no tail)', () => {
    const data = stringToBytes('12345678');
    const result = murmurHash64A(data);
    expect(typeof result).toBe('bigint');
    expect(result).toBeGreaterThanOrEqual(0n);
  });

  it('handles input of 16 bytes (two full blocks, no tail)', () => {
    const data = stringToBytes('1234567890abcdef');
    const result = murmurHash64A(data);
    expect(typeof result).toBe('bigint');
    expect(result).toBeGreaterThanOrEqual(0n);
  });

  it('handles all tail lengths (1 through 7 remaining bytes)', () => {
    // Inputs of length 9..15 to test tail cases 1..7
    for (let len = 9; len <= 15; len++) {
      const input = 'x'.repeat(len);
      const data = stringToBytes(input);
      const result = murmurHash64A(data);
      expect(typeof result).toBe('bigint');
      expect(result).toBeGreaterThanOrEqual(0n);
      expect(result).toBeLessThanOrEqual(0xffffffffffffffffn);
    }
  });

  it('is sensitive to byte order', () => {
    const h1 = murmurHash64A(new Uint8Array([1, 2]));
    const h2 = murmurHash64A(new Uint8Array([2, 1]));
    expect(h1).not.toBe(h2);
  });

  it('handles binary data with zero bytes', () => {
    const data = new Uint8Array([0, 0, 0, 0]);
    const result = murmurHash64A(data);
    expect(typeof result).toBe('bigint');
    expect(result).toBeGreaterThanOrEqual(0n);
  });

  it('handles high byte values (0xff)', () => {
    const data = new Uint8Array([0xff, 0xff, 0xff, 0xff, 0xff]);
    const result = murmurHash64A(data);
    expect(typeof result).toBe('bigint');
    expect(result).toBeGreaterThanOrEqual(0n);
  });

  it('produces good distribution (no obvious collisions for simple inputs)', () => {
    const hashes = new Set<bigint>();
    for (let i = 0; i < 1000; i++) {
      hashes.add(murmurHash64A(stringToBytes(`key${i}`)));
    }
    // All 1000 inputs should produce unique hashes
    expect(hashes.size).toBe(1000);
  });
});

describe('hllPatLen', () => {
  it('returns a tuple of [index, count]', () => {
    const result = hllPatLen('hello');
    expect(Array.isArray(result)).toBe(true);
    expect(result).toHaveLength(2);
  });

  it('index is within valid register range [0, 16383]', () => {
    const inputs = ['a', 'b', 'hello', 'world', '', '12345', 'test'];
    for (const input of inputs) {
      const [index] = hllPatLen(input);
      expect(index).toBeGreaterThanOrEqual(0);
      expect(index).toBeLessThan(HLL_REGISTERS);
    }
  });

  it('count (run length) is at least 1', () => {
    const inputs = ['a', 'b', 'hello', 'world', '', '12345'];
    for (const input of inputs) {
      const [, count] = hllPatLen(input);
      expect(count).toBeGreaterThanOrEqual(1);
    }
  });

  it('count is at most HLL_Q + 1 (51)', () => {
    // The sentinel bit at position HLL_Q guarantees max count is HLL_Q + 1
    const inputs = ['a', 'b', 'hello', 'world', 'foo', 'bar'];
    for (const input of inputs) {
      const [, count] = hllPatLen(input);
      expect(count).toBeLessThanOrEqual(HLL_Q + 1);
    }
  });

  it('returns consistent results for same input', () => {
    const r1 = hllPatLen('test');
    const r2 = hllPatLen('test');
    expect(r1).toEqual(r2);
  });

  it('handles empty string', () => {
    const [index, count] = hllPatLen('');
    expect(index).toBeGreaterThanOrEqual(0);
    expect(index).toBeLessThan(HLL_REGISTERS);
    expect(count).toBeGreaterThanOrEqual(1);
    expect(count).toBeLessThanOrEqual(HLL_Q + 1);
  });

  it('distributes elements across many registers', () => {
    const indices = new Set<number>();
    for (let i = 0; i < 10000; i++) {
      const [index] = hllPatLen(`element${i}`);
      indices.add(index);
    }
    // With 10000 elements across 16384 registers, we expect good spread
    expect(indices.size).toBeGreaterThan(5000);
  });

  it('most run lengths are small (statistical property)', () => {
    let totalCount = 0;
    const n = 10000;
    for (let i = 0; i < n; i++) {
      const [, count] = hllPatLen(`item${i}`);
      totalCount += count;
    }
    // Average run length should be around 2 (geometric distribution)
    const avg = totalCount / n;
    expect(avg).toBeGreaterThan(1);
    expect(avg).toBeLessThan(5);
  });

  it('handles long strings', () => {
    const longStr = 'a'.repeat(10000);
    const [index, count] = hllPatLen(longStr);
    expect(index).toBeGreaterThanOrEqual(0);
    expect(index).toBeLessThan(HLL_REGISTERS);
    expect(count).toBeGreaterThanOrEqual(1);
  });

  it('handles strings with special characters', () => {
    const specials = ['\x00', '\xff', '\n', '\t', ' ', '!@#$%^&*()'];
    for (const s of specials) {
      const [index, count] = hllPatLen(s);
      expect(index).toBeGreaterThanOrEqual(0);
      expect(index).toBeLessThan(HLL_REGISTERS);
      expect(count).toBeGreaterThanOrEqual(1);
    }
  });
});
