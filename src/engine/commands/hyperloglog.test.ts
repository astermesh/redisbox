import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Database } from '../database.ts';
import type { CommandContext } from '../types.ts';
import { ConfigStore } from '../../config-store.ts';
import * as hll from './hyperloglog.ts';

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

// --- PFADD ---

describe('PFADD', () => {
  it('creates a new HLL key and returns 1', () => {
    const { ctx } = createDb();
    const result = hll.pfadd(ctx, ['mykey', 'a', 'b', 'c']);
    expect(result).toEqual({ kind: 'integer', value: 1 });
  });

  it('returns 0 when adding duplicate elements', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'a', 'b', 'c']);
    const result = hll.pfadd(ctx, ['mykey', 'a', 'b', 'c']);
    expect(result).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns 1 when at least one element is new', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'a', 'b']);
    const result = hll.pfadd(ctx, ['mykey', 'b', 'c']);
    expect(result).toEqual({ kind: 'integer', value: 1 });
  });

  it('creates empty HLL with no elements (returns 1 for new key)', () => {
    const { ctx } = createDb();
    const result = hll.pfadd(ctx, ['mykey']);
    expect(result).toEqual({ kind: 'integer', value: 1 });
  });

  it('returns 0 when adding no elements to existing key', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'a']);
    const result = hll.pfadd(ctx, ['mykey']);
    expect(result).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns WRONGTYPE error for non-string key', () => {
    const { ctx, db } = createDb();
    db.set('mykey', 'list', 'quicklist', []);
    const result = hll.pfadd(ctx, ['mykey', 'a']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns error for corrupted HLL (non-HLL string)', () => {
    const { ctx, db } = createDb();
    db.set('mykey', 'string', 'raw', 'notanhll');
    const result = hll.pfadd(ctx, ['mykey', 'a']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Key is not a valid HyperLogLog string value.',
    });
  });

  it('works correctly after sparse-to-dense promotion', () => {
    const { ctx } = createDb();
    // Force promotion to dense
    const elements = [];
    for (let i = 0; i < 2000; i++) {
      elements.push(`e${i}`);
    }
    hll.pfadd(ctx, ['mykey', ...elements]);
    const enc = hll.pfdebug(ctx, ['ENCODING', 'mykey']);
    expect(enc).toEqual({ kind: 'bulk', value: 'dense' });
    // Adding more elements to dense should still work
    const result = hll.pfadd(ctx, ['mykey', 'newelem']);
    expect(result.kind).toBe('integer');
    const count = hll.pfcount(ctx, ['mykey']) as {
      kind: string;
      value: number;
    };
    expect(count.value).toBeGreaterThanOrEqual(1800);
  });

  it('handles empty string elements', () => {
    const { ctx } = createDb();
    const result = hll.pfadd(ctx, ['mykey', '']);
    expect(result).toEqual({ kind: 'integer', value: 1 });
    const count = hll.pfcount(ctx, ['mykey']) as {
      kind: string;
      value: number;
    };
    expect(count.kind).toBe('integer');
    expect(count.value).toBeGreaterThanOrEqual(1);
  });
});

// --- PFCOUNT ---

describe('PFCOUNT', () => {
  it('returns 0 for non-existent key', () => {
    const { ctx } = createDb();
    const result = hll.pfcount(ctx, ['mykey']);
    expect(result).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns approximate cardinality for a single key', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'a', 'b', 'c', 'd', 'e']);
    const result = hll.pfcount(ctx, ['mykey']) as {
      kind: string;
      value: number;
    };
    // HLL is probabilistic — allow some tolerance
    expect(result.kind).toBe('integer');
    expect(result.value).toBeGreaterThanOrEqual(3);
    expect(result.value).toBeLessThanOrEqual(8);
  });

  it('returns consistent results (caching)', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'a', 'b', 'c']);
    const r1 = hll.pfcount(ctx, ['mykey']);
    const r2 = hll.pfcount(ctx, ['mykey']);
    expect(r1).toEqual(r2);
  });

  it('counts multiple keys (union cardinality)', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['key1', 'a', 'b', 'c']);
    hll.pfadd(ctx, ['key2', 'c', 'd', 'e']);
    const result = hll.pfcount(ctx, ['key1', 'key2']) as {
      kind: string;
      value: number;
    };
    expect(result.kind).toBe('integer');
    // Union of {a,b,c} and {c,d,e} = {a,b,c,d,e} = 5
    expect(result.value).toBeGreaterThanOrEqual(3);
    expect(result.value).toBeLessThanOrEqual(8);
  });

  it('skips non-existent keys in multi-key mode', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['key1', 'a', 'b', 'c']);
    const result = hll.pfcount(ctx, ['key1', 'nonexistent']);
    const single = hll.pfcount(ctx, ['key1']);
    expect(result).toEqual(single);
  });

  it('returns WRONGTYPE for non-string key', () => {
    const { ctx, db } = createDb();
    db.set('mykey', 'list', 'quicklist', []);
    const result = hll.pfcount(ctx, ['mykey']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns error for invalid HLL string', () => {
    const { ctx, db } = createDb();
    db.set('mykey', 'string', 'raw', 'notanhll');
    const result = hll.pfcount(ctx, ['mykey']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Key is not a valid HyperLogLog string value.',
    });
  });

  it('returns 0 when all keys are non-existent (multi-key)', () => {
    const { ctx } = createDb();
    const result = hll.pfcount(ctx, ['a', 'b', 'c']);
    expect(result).toEqual({ kind: 'integer', value: 0 });
  });

  it('TYPE returns string for HLL keys', () => {
    const { ctx, db } = createDb();
    hll.pfadd(ctx, ['mykey', 'a']);
    const entry = db.get('mykey');
    expect(entry?.type).toBe('string');
  });

  it('OBJECT ENCODING returns raw for HLL keys', () => {
    const { ctx, db } = createDb();
    hll.pfadd(ctx, ['mykey', 'a']);
    const entry = db.get('mykey');
    expect(entry?.encoding).toBe('raw');
  });

  it('single-key PFCOUNT caches result (modifies key on read)', () => {
    const { ctx, db } = createDb();
    hll.pfadd(ctx, ['mykey', 'a', 'b', 'c']);
    const valueBefore = (db.get('mykey')?.value as string).slice();
    hll.pfcount(ctx, ['mykey']);
    const valueAfter = db.get('mykey')?.value as string;
    // The raw value should change because PFCOUNT writes the cached cardinality
    expect(valueAfter).not.toBe(valueBefore);
  });

  it('multi-key PFCOUNT does NOT modify source keys', () => {
    const { ctx, db } = createDb();
    hll.pfadd(ctx, ['key1', 'a', 'b']);
    hll.pfadd(ctx, ['key2', 'c', 'd']);
    // Force cache by reading each key individually first
    hll.pfcount(ctx, ['key1']);
    hll.pfcount(ctx, ['key2']);
    const val1Before = db.get('key1')?.value as string;
    const val2Before = db.get('key2')?.value as string;
    hll.pfcount(ctx, ['key1', 'key2']);
    const val1After = db.get('key1')?.value as string;
    const val2After = db.get('key2')?.value as string;
    expect(val1After).toBe(val1Before);
    expect(val2After).toBe(val2Before);
  });

  it('PFADD after PFCOUNT invalidates cache and updates count', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'a', 'b', 'c']);
    const count1 = hll.pfcount(ctx, ['mykey']) as {
      kind: string;
      value: number;
    };
    // Add many more distinct elements
    for (let i = 0; i < 50; i++) {
      hll.pfadd(ctx, ['mykey', `new${i}`]);
    }
    const count2 = hll.pfcount(ctx, ['mykey']) as {
      kind: string;
      value: number;
    };
    expect(count2.value).toBeGreaterThan(count1.value);
  });
});

// --- PFMERGE ---

describe('PFMERGE', () => {
  it('merges two HLLs into destination', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['key1', 'a', 'b', 'c']);
    hll.pfadd(ctx, ['key2', 'c', 'd', 'e']);
    const result = hll.pfmerge(ctx, ['dest', 'key1', 'key2']);
    expect(result).toEqual({ kind: 'status', value: 'OK' });

    const count = hll.pfcount(ctx, ['dest']) as { kind: string; value: number };
    expect(count.kind).toBe('integer');
    expect(count.value).toBeGreaterThanOrEqual(3);
    expect(count.value).toBeLessThanOrEqual(8);
  });

  it('creates empty HLL if all sources are missing', () => {
    const { ctx } = createDb();
    const result = hll.pfmerge(ctx, ['dest', 'nonexistent']);
    expect(result).toEqual({ kind: 'status', value: 'OK' });
    const count = hll.pfcount(ctx, ['dest']);
    expect(count).toEqual({ kind: 'integer', value: 0 });
  });

  it('overwrites existing destination', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['dest', 'x', 'y', 'z']);
    hll.pfadd(ctx, ['src', 'a']);
    hll.pfmerge(ctx, ['dest', 'src']);
    // dest should now contain union of old dest and src
    const count = hll.pfcount(ctx, ['dest']) as { kind: string; value: number };
    expect(count.kind).toBe('integer');
    expect(count.value).toBeGreaterThanOrEqual(2);
  });

  it('returns WRONGTYPE for non-string source', () => {
    const { ctx, db } = createDb();
    db.set('src', 'list', 'quicklist', []);
    const result = hll.pfmerge(ctx, ['dest', 'src']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns WRONGTYPE for non-string destination', () => {
    const { ctx, db } = createDb();
    db.set('dest', 'list', 'quicklist', []);
    hll.pfadd(ctx, ['src', 'a']);
    const result = hll.pfmerge(ctx, ['dest', 'src']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('creates empty HLL with dest-only (no sources)', () => {
    const { ctx } = createDb();
    const result = hll.pfmerge(ctx, ['dest']);
    expect(result).toEqual({ kind: 'status', value: 'OK' });
    const count = hll.pfcount(ctx, ['dest']);
    expect(count).toEqual({ kind: 'integer', value: 0 });
  });

  it('result has type string and encoding raw', () => {
    const { ctx, db } = createDb();
    hll.pfadd(ctx, ['src', 'a', 'b']);
    hll.pfmerge(ctx, ['dest', 'src']);
    const entry = db.get('dest');
    expect(entry?.type).toBe('string');
    expect(entry?.encoding).toBe('raw');
  });

  it('merged result is superset of all sources', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['k1', 'a', 'b']);
    hll.pfadd(ctx, ['k2', 'c', 'd']);
    hll.pfadd(ctx, ['k3', 'e', 'f']);
    hll.pfmerge(ctx, ['dest', 'k1', 'k2', 'k3']);
    const merged = hll.pfcount(ctx, ['dest']) as {
      kind: string;
      value: number;
    };
    const union = hll.pfcount(ctx, ['k1', 'k2', 'k3']) as {
      kind: string;
      value: number;
    };
    expect(merged.value).toBe(union.value);
  });

  it('can merge with self as source', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'a', 'b', 'c']);
    const countBefore = hll.pfcount(ctx, ['mykey']);
    hll.pfmerge(ctx, ['mykey', 'mykey']);
    const countAfter = hll.pfcount(ctx, ['mykey']);
    expect(countBefore).toEqual(countAfter);
  });
});

// --- PFDEBUG ---

describe('PFDEBUG', () => {
  it('GETREG returns register values', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'a']);
    const result = hll.pfdebug(ctx, ['GETREG', 'mykey']);
    expect(result.kind).toBe('array');
    if (result.kind === 'array') {
      expect(result.value.length).toBe(16384);
      // At least one register should be non-zero after adding an element
      const nonZero = result.value.filter(
        (r) => r.kind === 'integer' && (r as { value: number }).value > 0
      );
      expect(nonZero.length).toBeGreaterThanOrEqual(1);
    }
  });

  it('DECODE returns sparse representation', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'a']);
    const result = hll.pfdebug(ctx, ['DECODE', 'mykey']);
    expect(result.kind).toBe('bulk');
  });

  it('returns error for non-existent key', () => {
    const { ctx } = createDb();
    const result = hll.pfdebug(ctx, ['GETREG', 'nonexistent']);
    expect(result.kind).toBe('error');
  });

  it('ENCODING returns sparse for new HLL', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'a']);
    const result = hll.pfdebug(ctx, ['ENCODING', 'mykey']);
    expect(result).toEqual({ kind: 'bulk', value: 'sparse' });
  });

  it('ENCODING returns dense after promotion', () => {
    const { ctx } = createDb();
    const elements = [];
    for (let i = 0; i < 2000; i++) {
      elements.push(`element${i}`);
    }
    hll.pfadd(ctx, ['mykey', ...elements]);
    const result = hll.pfdebug(ctx, ['ENCODING', 'mykey']);
    expect(result).toEqual({ kind: 'bulk', value: 'dense' });
  });

  it('TODENSE converts sparse to dense and returns 1', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'a']);
    expect(hll.pfdebug(ctx, ['ENCODING', 'mykey'])).toEqual({
      kind: 'bulk',
      value: 'sparse',
    });

    const result = hll.pfdebug(ctx, ['TODENSE', 'mykey']);
    expect(result).toEqual({ kind: 'integer', value: 1 });

    expect(hll.pfdebug(ctx, ['ENCODING', 'mykey'])).toEqual({
      kind: 'bulk',
      value: 'dense',
    });
  });

  it('TODENSE returns 0 for already dense HLL', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'a']);
    hll.pfdebug(ctx, ['TODENSE', 'mykey']);
    const result = hll.pfdebug(ctx, ['TODENSE', 'mykey']);
    expect(result).toEqual({ kind: 'integer', value: 0 });
  });

  it('TODENSE preserves cardinality', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'a', 'b', 'c', 'd', 'e']);
    const countBefore = hll.pfcount(ctx, ['mykey']);
    hll.pfdebug(ctx, ['TODENSE', 'mykey']);
    const countAfter = hll.pfcount(ctx, ['mykey']);
    expect(countBefore).toEqual(countAfter);
  });

  it('returns error for unknown subcommand', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'a']);
    const result = hll.pfdebug(ctx, ['INVALID', 'mykey']);
    expect(result.kind).toBe('error');
  });
});

// --- PFSELFTEST ---

describe('PFSELFTEST', () => {
  it('returns OK (validates headers, hash, registers, and cardinality)', () => {
    const { ctx } = createDb();
    const result = hll.pfselftest(ctx);
    expect(result).toEqual({ kind: 'status', value: 'OK' });
  }, 30000);
});

// --- Sparse/Dense transition ---

describe('Sparse/Dense transition', () => {
  it('starts in sparse encoding', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'a']);
    const result = hll.pfdebug(ctx, ['ENCODING', 'mykey']);
    expect(result).toEqual({ kind: 'bulk', value: 'sparse' });
  });

  it('transitions to dense with many unique elements', () => {
    const { ctx } = createDb();
    const elements = [];
    for (let i = 0; i < 2000; i++) {
      elements.push(`element${i}`);
    }
    hll.pfadd(ctx, ['mykey', ...elements]);
    const result = hll.pfdebug(ctx, ['ENCODING', 'mykey']);
    expect(result).toEqual({ kind: 'bulk', value: 'dense' });
  });
});

// --- Hash function and register assignment ---

describe('Hash function and register assignment', () => {
  it('GETREG shows consistent register assignments for same elements', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['k1', 'a', 'b', 'c']);
    hll.pfadd(ctx, ['k2', 'a', 'b', 'c']);
    const r1 = hll.pfdebug(ctx, ['GETREG', 'k1']);
    const r2 = hll.pfdebug(ctx, ['GETREG', 'k2']);
    expect(r1).toEqual(r2);
  });

  it('same element always maps to same register', () => {
    const { ctx } = createDb();
    // Add 'a' to two separate HLLs — should set same register
    hll.pfadd(ctx, ['k1', 'a']);
    hll.pfadd(ctx, ['k2', 'a']);
    const r1 = hll.pfdebug(ctx, ['GETREG', 'k1']);
    const r2 = hll.pfdebug(ctx, ['GETREG', 'k2']);
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
    hll.pfadd(ctx, ['mykey', ...elements]);
    const result = hll.pfdebug(ctx, ['GETREG', 'mykey']);
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
    hll.pfadd(ctx, ['mykey', ...elements]);
    const result = hll.pfdebug(ctx, ['GETREG', 'mykey']);
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
    hll.pfadd(ctx, ['mykey', 'x']);
    const result = hll.pfdebug(ctx, ['GETREG', 'mykey']);
    expect(result.kind).toBe('array');
    if (result.kind === 'array') {
      expect(result.value.length).toBe(16384);
    }
  });

  it('GETREG works for both sparse and dense', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['sparse', 'a', 'b', 'c']);
    const sparseRegs = hll.pfdebug(ctx, ['GETREG', 'sparse']);

    // Convert to dense and check registers are identical
    hll.pfdebug(ctx, ['TODENSE', 'sparse']);
    const denseRegs = hll.pfdebug(ctx, ['GETREG', 'sparse']);

    expect(sparseRegs).toEqual(denseRegs);
  });
});

// --- PFDEBUG DECODE format ---

describe('PFDEBUG DECODE', () => {
  it('empty HLL decodes to single XZERO covering 16384 registers', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey']);
    const result = hll.pfdebug(ctx, ['DECODE', 'mykey']);
    expect(result.kind).toBe('bulk');
    if (result.kind === 'bulk') {
      // An empty HLL has one XZERO opcode covering all 16384 registers
      expect(result.value).toBe('Z:16384');
    }
  });

  it('DECODE contains z:, Z:, and v: opcodes', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'a', 'b', 'c', 'd']);
    const result = hll.pfdebug(ctx, ['DECODE', 'mykey']);
    expect(result.kind).toBe('bulk');
    if (result.kind === 'bulk' && result.value) {
      // Should contain at least one value opcode for the added elements
      expect(result.value).toMatch(/v:\d+,\d+/);
      // Should contain zero-run opcodes for empty regions
      expect(result.value).toMatch(/[zZ]:\d+/);
    }
  });

  it('DECODE returns "dense" for dense-encoded HLL', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'a']);
    hll.pfdebug(ctx, ['TODENSE', 'mykey']);
    const result = hll.pfdebug(ctx, ['DECODE', 'mykey']);
    expect(result).toEqual({ kind: 'bulk', value: 'dense' });
  });

  it('DECODE register count sums to 16384', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'hello', 'world', 'foo', 'bar']);
    const result = hll.pfdebug(ctx, ['DECODE', 'mykey']);
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

// --- Cardinality accuracy ---

describe('Cardinality accuracy', () => {
  it('estimates very small cardinalities (10 elements)', () => {
    const { ctx } = createDb();
    for (let i = 0; i < 10; i++) {
      hll.pfadd(ctx, ['mykey', `elem${i}`]);
    }
    const result = hll.pfcount(ctx, ['mykey']) as {
      kind: string;
      value: number;
    };
    expect(result.kind).toBe('integer');
    // For 10 elements, allow generous tolerance
    expect(result.value).toBeGreaterThanOrEqual(7);
    expect(result.value).toBeLessThanOrEqual(14);
  });

  it('estimates small cardinalities reasonably (100 elements)', () => {
    const { ctx } = createDb();
    for (let i = 0; i < 100; i++) {
      hll.pfadd(ctx, ['mykey', `elem${i}`]);
    }
    const result = hll.pfcount(ctx, ['mykey']) as {
      kind: string;
      value: number;
    };
    expect(result.kind).toBe('integer');
    // Allow ~10% error for 100 elements
    expect(result.value).toBeGreaterThanOrEqual(85);
    expect(result.value).toBeLessThanOrEqual(115);
  });

  it('estimates medium cardinalities reasonably (1K elements)', () => {
    const { ctx } = createDb();
    for (let i = 0; i < 1000; i++) {
      hll.pfadd(ctx, ['mykey', `elem${i}`]);
    }
    const result = hll.pfcount(ctx, ['mykey']) as {
      kind: string;
      value: number;
    };
    expect(result.kind).toBe('integer');
    // Allow ~5% error for 1000 elements
    expect(result.value).toBeGreaterThanOrEqual(900);
    expect(result.value).toBeLessThanOrEqual(1100);
  });

  it('estimates large cardinalities (10K) within 2x standard error', () => {
    const { ctx } = createDb();
    const N = 10000;
    for (let i = 0; i < N; i++) {
      hll.pfadd(ctx, ['mykey', `item${i}`]);
    }
    const result = hll.pfcount(ctx, ['mykey']) as {
      kind: string;
      value: number;
    };
    expect(result.kind).toBe('integer');
    // 2x standard error ≈ 1.62% → allow ~5% margin
    expect(result.value).toBeGreaterThanOrEqual(N * 0.95);
    expect(result.value).toBeLessThanOrEqual(N * 1.05);
  });

  it('estimates 100K cardinality within 2x standard error', () => {
    const { ctx } = createDb();
    const N = 100000;
    const batch: string[] = ['mykey'];
    for (let i = 0; i < N; i++) {
      batch.push(`e${i}`);
    }
    hll.pfadd(ctx, batch);
    const result = hll.pfcount(ctx, ['mykey']) as {
      kind: string;
      value: number;
    };
    expect(result.kind).toBe('integer');
    // 2x standard error ≈ 1.62%
    const stdError2x = N * 2 * (1.04 / Math.sqrt(16384));
    expect(result.value).toBeGreaterThanOrEqual(N - stdError2x);
    expect(result.value).toBeLessThanOrEqual(N + stdError2x);
  }, 30000);

  it('estimates 1M cardinality within 3x standard error', () => {
    const { ctx } = createDb();
    const N = 1000000;
    // Add in batches for efficiency
    const batchSize = 10000;
    for (let start = 0; start < N; start += batchSize) {
      const batch: string[] = ['mykey'];
      const end = Math.min(start + batchSize, N);
      for (let i = start; i < end; i++) {
        batch.push(`m${i}`);
      }
      hll.pfadd(ctx, batch);
    }
    const result = hll.pfcount(ctx, ['mykey']) as {
      kind: string;
      value: number;
    };
    expect(result.kind).toBe('integer');
    // 3x standard error ≈ 2.43% — accounts for variance at high cardinalities
    const stdError3x = N * 3 * (1.04 / Math.sqrt(16384));
    expect(result.value).toBeGreaterThanOrEqual(N - stdError3x);
    expect(result.value).toBeLessThanOrEqual(N + stdError3x);
  }, 120000);

  it('handles non-ASCII elements correctly', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'café', 'über', 'naïve']);
    const result = hll.pfcount(ctx, ['mykey']) as {
      kind: string;
      value: number;
    };
    expect(result.kind).toBe('integer');
    expect(result.value).toBeGreaterThanOrEqual(1);
    expect(result.value).toBeLessThanOrEqual(6);
  });
});
