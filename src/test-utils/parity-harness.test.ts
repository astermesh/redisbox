/**
 * Unit tests for parity harness comparison utilities.
 *
 * These tests verify the comparison logic in isolation, without
 * requiring any network connections or Redis instances.
 */

import { describe, it, expect } from 'vitest';
import type { RespValue } from '../resp/types.ts';
import {
  normalizeResp,
  assertRespEqual,
  assertRespUnordered,
  assertRespStructure,
  formatResp,
} from './parity-harness.ts';

// ============================================================================
// normalizeResp
// ============================================================================

describe('normalizeResp', () => {
  it('normalizes simple string', () => {
    const resp: RespValue = { type: 'simple', value: 'OK' };
    expect(normalizeResp(resp)).toEqual({ type: 'simple', value: 'OK' });
  });

  it('normalizes error', () => {
    const resp: RespValue = { type: 'error', value: 'ERR unknown command' };
    expect(normalizeResp(resp)).toEqual({
      type: 'error',
      value: 'ERR unknown command',
    });
  });

  it('normalizes integer', () => {
    const resp: RespValue = { type: 'integer', value: 42 };
    expect(normalizeResp(resp)).toEqual({ type: 'integer', value: 42 });
  });

  it('normalizes bigint integer to number', () => {
    const resp: RespValue = { type: 'integer', value: BigInt(100) };
    expect(normalizeResp(resp)).toEqual({ type: 'integer', value: 100 });
  });

  it('normalizes bulk string', () => {
    const resp: RespValue = { type: 'bulk', value: Buffer.from('hello') };
    expect(normalizeResp(resp)).toEqual({ type: 'bulk', value: 'hello' });
  });

  it('normalizes null bulk string', () => {
    const resp: RespValue = { type: 'bulk', value: null };
    expect(normalizeResp(resp)).toEqual({ type: 'bulk', value: null });
  });

  it('normalizes empty array', () => {
    const resp: RespValue = { type: 'array', value: [] };
    expect(normalizeResp(resp)).toEqual({ type: 'array', value: [] });
  });

  it('normalizes null array', () => {
    const resp: RespValue = { type: 'array', value: null };
    expect(normalizeResp(resp)).toEqual({ type: 'array', value: null });
  });

  it('normalizes nested array', () => {
    const resp: RespValue = {
      type: 'array',
      value: [
        { type: 'bulk', value: Buffer.from('key') },
        { type: 'integer', value: 1 },
      ],
    };
    expect(normalizeResp(resp)).toEqual({
      type: 'array',
      value: [
        { type: 'bulk', value: 'key' },
        { type: 'integer', value: 1 },
      ],
    });
  });
});

// ============================================================================
// assertRespEqual
// ============================================================================

describe('assertRespEqual', () => {
  it('passes for identical simple strings', () => {
    const a: RespValue = { type: 'simple', value: 'OK' };
    const b: RespValue = { type: 'simple', value: 'OK' };
    expect(() => assertRespEqual(a, b)).not.toThrow();
  });

  it('passes for identical integers', () => {
    const a: RespValue = { type: 'integer', value: 42 };
    const b: RespValue = { type: 'integer', value: 42 };
    expect(() => assertRespEqual(a, b)).not.toThrow();
  });

  it('passes for identical bulk strings', () => {
    const a: RespValue = { type: 'bulk', value: Buffer.from('hello') };
    const b: RespValue = { type: 'bulk', value: Buffer.from('hello') };
    expect(() => assertRespEqual(a, b)).not.toThrow();
  });

  it('passes for null bulk strings', () => {
    const a: RespValue = { type: 'bulk', value: null };
    const b: RespValue = { type: 'bulk', value: null };
    expect(() => assertRespEqual(a, b)).not.toThrow();
  });

  it('passes for identical errors', () => {
    const a: RespValue = { type: 'error', value: 'ERR syntax error' };
    const b: RespValue = { type: 'error', value: 'ERR syntax error' };
    expect(() => assertRespEqual(a, b)).not.toThrow();
  });

  it('fails for different types', () => {
    const a: RespValue = { type: 'simple', value: 'OK' };
    const b: RespValue = { type: 'integer', value: 1 };
    expect(() => assertRespEqual(a, b)).toThrow('RESP mismatch');
  });

  it('fails for different values', () => {
    const a: RespValue = { type: 'bulk', value: Buffer.from('foo') };
    const b: RespValue = { type: 'bulk', value: Buffer.from('bar') };
    expect(() => assertRespEqual(a, b)).toThrow('RESP mismatch');
  });

  it('fails for null vs non-null bulk', () => {
    const a: RespValue = { type: 'bulk', value: null };
    const b: RespValue = { type: 'bulk', value: Buffer.from('val') };
    expect(() => assertRespEqual(a, b)).toThrow('RESP mismatch');
  });

  it('includes context in error message', () => {
    const a: RespValue = { type: 'integer', value: 1 };
    const b: RespValue = { type: 'integer', value: 2 };
    expect(() => assertRespEqual(a, b, 'SET key value')).toThrow(
      '[SET key value]'
    );
  });

  it('passes for identical arrays', () => {
    const a: RespValue = {
      type: 'array',
      value: [
        { type: 'bulk', value: Buffer.from('a') },
        { type: 'bulk', value: Buffer.from('b') },
      ],
    };
    const b: RespValue = {
      type: 'array',
      value: [
        { type: 'bulk', value: Buffer.from('a') },
        { type: 'bulk', value: Buffer.from('b') },
      ],
    };
    expect(() => assertRespEqual(a, b)).not.toThrow();
  });

  it('fails for arrays with different order', () => {
    const a: RespValue = {
      type: 'array',
      value: [
        { type: 'bulk', value: Buffer.from('a') },
        { type: 'bulk', value: Buffer.from('b') },
      ],
    };
    const b: RespValue = {
      type: 'array',
      value: [
        { type: 'bulk', value: Buffer.from('b') },
        { type: 'bulk', value: Buffer.from('a') },
      ],
    };
    expect(() => assertRespEqual(a, b)).toThrow('RESP mismatch');
  });
});

// ============================================================================
// assertRespUnordered
// ============================================================================

describe('assertRespUnordered', () => {
  it('passes for identical arrays', () => {
    const a: RespValue = {
      type: 'array',
      value: [
        { type: 'bulk', value: Buffer.from('a') },
        { type: 'bulk', value: Buffer.from('b') },
      ],
    };
    const b: RespValue = {
      type: 'array',
      value: [
        { type: 'bulk', value: Buffer.from('a') },
        { type: 'bulk', value: Buffer.from('b') },
      ],
    };
    expect(() => assertRespUnordered(a, b)).not.toThrow();
  });

  it('passes for arrays with different order', () => {
    const a: RespValue = {
      type: 'array',
      value: [
        { type: 'bulk', value: Buffer.from('c') },
        { type: 'bulk', value: Buffer.from('a') },
        { type: 'bulk', value: Buffer.from('b') },
      ],
    };
    const b: RespValue = {
      type: 'array',
      value: [
        { type: 'bulk', value: Buffer.from('a') },
        { type: 'bulk', value: Buffer.from('b') },
        { type: 'bulk', value: Buffer.from('c') },
      ],
    };
    expect(() => assertRespUnordered(a, b)).not.toThrow();
  });

  it('fails for arrays with different elements', () => {
    const a: RespValue = {
      type: 'array',
      value: [
        { type: 'bulk', value: Buffer.from('a') },
        { type: 'bulk', value: Buffer.from('b') },
      ],
    };
    const b: RespValue = {
      type: 'array',
      value: [
        { type: 'bulk', value: Buffer.from('a') },
        { type: 'bulk', value: Buffer.from('c') },
      ],
    };
    expect(() => assertRespUnordered(a, b)).toThrow('unordered mismatch');
  });

  it('fails for arrays with different lengths', () => {
    const a: RespValue = {
      type: 'array',
      value: [{ type: 'bulk', value: Buffer.from('a') }],
    };
    const b: RespValue = {
      type: 'array',
      value: [
        { type: 'bulk', value: Buffer.from('a') },
        { type: 'bulk', value: Buffer.from('b') },
      ],
    };
    expect(() => assertRespUnordered(a, b)).toThrow('array length mismatch');
  });

  it('passes for null arrays', () => {
    const a: RespValue = { type: 'array', value: null };
    const b: RespValue = { type: 'array', value: null };
    expect(() => assertRespUnordered(a, b)).not.toThrow();
  });

  it('fails when one is null and other is not', () => {
    const a: RespValue = { type: 'array', value: null };
    const b: RespValue = { type: 'array', value: [] };
    expect(() => assertRespUnordered(a, b)).toThrow('one array is null');
  });

  it('falls back to exact match for non-array types', () => {
    const a: RespValue = { type: 'integer', value: 5 };
    const b: RespValue = { type: 'integer', value: 5 };
    expect(() => assertRespUnordered(a, b)).not.toThrow();
  });

  it('fails for different types', () => {
    const a: RespValue = { type: 'simple', value: 'OK' };
    const b: RespValue = { type: 'integer', value: 1 };
    expect(() => assertRespUnordered(a, b)).toThrow('type mismatch');
  });

  it('handles empty arrays', () => {
    const a: RespValue = { type: 'array', value: [] };
    const b: RespValue = { type: 'array', value: [] };
    expect(() => assertRespUnordered(a, b)).not.toThrow();
  });

  it('handles mixed-type array elements in different order', () => {
    const a: RespValue = {
      type: 'array',
      value: [
        { type: 'integer', value: 1 },
        { type: 'bulk', value: Buffer.from('x') },
      ],
    };
    const b: RespValue = {
      type: 'array',
      value: [
        { type: 'bulk', value: Buffer.from('x') },
        { type: 'integer', value: 1 },
      ],
    };
    expect(() => assertRespUnordered(a, b)).not.toThrow();
  });
});

// ============================================================================
// assertRespStructure
// ============================================================================

describe('assertRespStructure', () => {
  it('passes for same type and structure', () => {
    const a: RespValue = { type: 'bulk', value: Buffer.from('foo') };
    const b: RespValue = { type: 'bulk', value: Buffer.from('bar') };
    expect(() => assertRespStructure(a, b)).not.toThrow();
  });

  it('passes for different integer values', () => {
    const a: RespValue = { type: 'integer', value: 1 };
    const b: RespValue = { type: 'integer', value: 999 };
    expect(() => assertRespStructure(a, b)).not.toThrow();
  });

  it('fails for different types', () => {
    const a: RespValue = { type: 'bulk', value: Buffer.from('foo') };
    const b: RespValue = { type: 'integer', value: 1 };
    expect(() => assertRespStructure(a, b)).toThrow('type mismatch');
  });

  it('passes for arrays with same element types but different values', () => {
    const a: RespValue = {
      type: 'array',
      value: [
        { type: 'bulk', value: Buffer.from('key1') },
        { type: 'bulk', value: Buffer.from('key2') },
      ],
    };
    const b: RespValue = {
      type: 'array',
      value: [
        { type: 'bulk', value: Buffer.from('other1') },
        { type: 'bulk', value: Buffer.from('other2') },
      ],
    };
    expect(() => assertRespStructure(a, b)).not.toThrow();
  });

  it('fails for arrays with different lengths', () => {
    const a: RespValue = {
      type: 'array',
      value: [{ type: 'bulk', value: Buffer.from('x') }],
    };
    const b: RespValue = {
      type: 'array',
      value: [
        { type: 'bulk', value: Buffer.from('x') },
        { type: 'bulk', value: Buffer.from('y') },
      ],
    };
    expect(() => assertRespStructure(a, b)).toThrow('array length mismatch');
  });

  it('fails for arrays with different element types', () => {
    const a: RespValue = {
      type: 'array',
      value: [{ type: 'bulk', value: Buffer.from('x') }],
    };
    const b: RespValue = {
      type: 'array',
      value: [{ type: 'integer', value: 1 }],
    };
    expect(() => assertRespStructure(a, b)).toThrow('type mismatch');
  });

  it('passes for null arrays', () => {
    const a: RespValue = { type: 'array', value: null };
    const b: RespValue = { type: 'array', value: null };
    expect(() => assertRespStructure(a, b)).not.toThrow();
  });

  it('fails for null vs non-null array', () => {
    const a: RespValue = { type: 'array', value: null };
    const b: RespValue = { type: 'array', value: [] };
    expect(() => assertRespStructure(a, b)).toThrow('one array is null');
  });

  it('passes for nested arrays with matching structure', () => {
    const a: RespValue = {
      type: 'array',
      value: [
        {
          type: 'array',
          value: [
            { type: 'bulk', value: Buffer.from('a') },
            { type: 'integer', value: 1 },
          ],
        },
      ],
    };
    const b: RespValue = {
      type: 'array',
      value: [
        {
          type: 'array',
          value: [
            { type: 'bulk', value: Buffer.from('x') },
            { type: 'integer', value: 99 },
          ],
        },
      ],
    };
    expect(() => assertRespStructure(a, b)).not.toThrow();
  });
});

// ============================================================================
// formatResp
// ============================================================================

describe('formatResp', () => {
  it('formats simple string', () => {
    expect(formatResp({ type: 'simple', value: 'OK' })).toBe('+OK');
  });

  it('formats error', () => {
    expect(formatResp({ type: 'error', value: 'ERR bad' })).toBe('-ERR bad');
  });

  it('formats integer', () => {
    expect(formatResp({ type: 'integer', value: 42 })).toBe(':42');
  });

  it('formats bulk string', () => {
    expect(formatResp({ type: 'bulk', value: Buffer.from('hi') })).toBe('"hi"');
  });

  it('formats null bulk string', () => {
    expect(formatResp({ type: 'bulk', value: null })).toBe('(nil)');
  });

  it('formats null array', () => {
    expect(formatResp({ type: 'array', value: null })).toBe('(nil array)');
  });

  it('formats empty array', () => {
    expect(formatResp({ type: 'array', value: [] })).toBe('(empty array)');
  });

  it('formats array with elements', () => {
    const resp: RespValue = {
      type: 'array',
      value: [
        { type: 'bulk', value: Buffer.from('a') },
        { type: 'integer', value: 1 },
      ],
    };
    expect(formatResp(resp)).toBe('["a", :1]');
  });
});
