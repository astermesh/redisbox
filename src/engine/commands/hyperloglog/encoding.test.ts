import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import type { Database } from '../../database.ts';
import type { CommandContext } from '../../types.ts';
import { ConfigStore } from '../../../config-store.ts';
import { pfadd, pfdebug } from './hyperloglog.ts';

function createDb(time = 1000): {
  db: Database;
  engine: RedisEngine;
  ctx: CommandContext;
  setTime: (t: number) => void;
} {
  let now = time;
  const clock = () => now;
  const engine = new RedisEngine({ clock, rng: () => 0.5 });
  const db = engine.db(0);
  const config = new ConfigStore();
  return {
    db,
    engine,
    ctx: { db, engine, config },
    setTime: (t: number) => {
      now = t;
    },
  };
}

// --- Sparse/Dense transition ---

describe('Sparse/Dense transition', () => {
  it('starts in sparse encoding', () => {
    const { ctx } = createDb();
    pfadd(ctx, ['mykey', 'a']);
    const result = pfdebug(ctx, ['ENCODING', 'mykey']);
    expect(result).toEqual({ kind: 'status', value: 'sparse' });
  });

  it('transitions to dense with many unique elements', () => {
    const { ctx } = createDb();
    const elements = [];
    for (let i = 0; i < 2000; i++) {
      elements.push(`element${i}`);
    }
    pfadd(ctx, ['mykey', ...elements]);
    const result = pfdebug(ctx, ['ENCODING', 'mykey']);
    expect(result).toEqual({ kind: 'status', value: 'dense' });
  });
});

// --- Hash function and register assignment ---

describe('Hash function and register assignment', () => {
  it('GETREG shows consistent register assignments for same elements', () => {
    const { ctx } = createDb();
    pfadd(ctx, ['k1', 'a', 'b', 'c']);
    pfadd(ctx, ['k2', 'a', 'b', 'c']);
    const r1 = pfdebug(ctx, ['GETREG', 'k1']);
    const r2 = pfdebug(ctx, ['GETREG', 'k2']);
    expect(r1).toEqual(r2);
  });

  it('same element always maps to same register', () => {
    const { ctx } = createDb();
    // Add 'a' to two separate HLLs — should set same register
    pfadd(ctx, ['k1', 'a']);
    pfadd(ctx, ['k2', 'a']);
    const r1 = pfdebug(ctx, ['GETREG', 'k1']);
    const r2 = pfdebug(ctx, ['GETREG', 'k2']);
    expect(r1.kind).toBe('array');
    expect(r2.kind).toBe('array');
    if (r1.kind === 'array' && r2.kind === 'array') {
      // Find the non-zero register — should be the same index and value
      for (let i = 0; i < r1.value.length; i++) {
        expect(r1.value[i]).toEqual(r2.value[i]);
      }
    }
  });

  it('different elements can map to different registers', () => {
    const { ctx } = createDb();
    // With enough elements, multiple distinct registers should be set
    const elements = [];
    for (let i = 0; i < 100; i++) {
      elements.push(`elem${i}`);
    }
    pfadd(ctx, ['mykey', ...elements]);
    const result = pfdebug(ctx, ['GETREG', 'mykey']);
    expect(result.kind).toBe('array');
    if (result.kind === 'array') {
      const nonZero = result.value.filter(
        (r) => r.kind === 'integer' && (r as { value: number }).value > 0
      );
      // With 100 elements, we expect many distinct registers
      expect(nonZero.length).toBeGreaterThan(50);
    }
  });

  it('register values are valid (1 to 51)', () => {
    const { ctx } = createDb();
    const elements = [];
    for (let i = 0; i < 1000; i++) {
      elements.push(`val${i}`);
    }
    pfadd(ctx, ['mykey', ...elements]);
    const result = pfdebug(ctx, ['GETREG', 'mykey']);
    expect(result.kind).toBe('array');
    if (result.kind === 'array') {
      for (const r of result.value) {
        if (r.kind === 'integer') {
          const v = r.value as number;
          // Register values: 0 (empty) or 1-51 (HLL_Q+1)
          expect(v).toBeGreaterThanOrEqual(0);
          expect(v).toBeLessThanOrEqual(51);
        }
      }
    }
  });

  it('GETREG returns exactly 16384 registers', () => {
    const { ctx } = createDb();
    pfadd(ctx, ['mykey', 'x']);
    const result = pfdebug(ctx, ['GETREG', 'mykey']);
    expect(result.kind).toBe('array');
    if (result.kind === 'array') {
      expect(result.value.length).toBe(16384);
    }
  });

  it('GETREG converts sparse to dense in-place (Redis behavior)', () => {
    const { ctx } = createDb();
    pfadd(ctx, ['mykey', 'a', 'b', 'c']);
    // Key starts as sparse
    expect(pfdebug(ctx, ['ENCODING', 'mykey'])).toEqual({
      kind: 'status',
      value: 'sparse',
    });
    // GETREG converts to dense as a side effect
    const regs = pfdebug(ctx, ['GETREG', 'mykey']);
    expect(regs.kind).toBe('array');
    expect(pfdebug(ctx, ['ENCODING', 'mykey'])).toEqual({
      kind: 'status',
      value: 'dense',
    });
  });
});

// --- PFDEBUG DECODE format ---

describe('PFDEBUG DECODE', () => {
  it('empty HLL decodes to single XZERO covering 16384 registers', () => {
    const { ctx } = createDb();
    pfadd(ctx, ['mykey']);
    const result = pfdebug(ctx, ['DECODE', 'mykey']);
    expect(result.kind).toBe('bulk');
    if (result.kind === 'bulk') {
      // An empty HLL has one XZERO opcode covering all 16384 registers
      expect(result.value).toBe('Z:16384');
    }
  });

  it('DECODE contains z:, Z:, and v: opcodes', () => {
    const { ctx } = createDb();
    pfadd(ctx, ['mykey', 'a', 'b', 'c', 'd']);
    const result = pfdebug(ctx, ['DECODE', 'mykey']);
    expect(result.kind).toBe('bulk');
    if (result.kind === 'bulk' && result.value) {
      // Should contain at least one value opcode for the added elements
      expect(result.value).toMatch(/v:\d+,\d+/);
      // Should contain zero-run opcodes for empty regions
      expect(result.value).toMatch(/[zZ]:\d+/);
    }
  });

  it('DECODE returns error for dense-encoded HLL', () => {
    const { ctx } = createDb();
    pfadd(ctx, ['mykey', 'a']);
    pfdebug(ctx, ['TODENSE', 'mykey']);
    const result = pfdebug(ctx, ['DECODE', 'mykey']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'HLL encoding is not sparse',
    });
  });

  it('DECODE register count sums to 16384', () => {
    const { ctx } = createDb();
    pfadd(ctx, ['mykey', 'hello', 'world', 'foo', 'bar']);
    const result = pfdebug(ctx, ['DECODE', 'mykey']);
    expect(result.kind).toBe('bulk');
    if (result.kind === 'bulk' && result.value) {
      const parts = result.value.split(' ');
      let total = 0;
      for (const part of parts) {
        if (part.startsWith('z:') || part.startsWith('Z:')) {
          total += parseInt(part.split(':')[1] ?? '0', 10);
        } else if (part.startsWith('v:')) {
          const runLen = parseInt(part.split(',')[1] ?? '0', 10);
          total += runLen;
        }
      }
      expect(total).toBe(16384);
    }
  });
});
