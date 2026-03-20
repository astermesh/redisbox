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

  it('returns error for unknown subcommand', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'a']);
    const result = hll.pfdebug(ctx, ['INVALID', 'mykey']);
    expect(result.kind).toBe('error');
  });
});

// --- PFSELFTEST ---

describe('PFSELFTEST', () => {
  it('returns OK', () => {
    const { ctx } = createDb();
    const result = hll.pfselftest(ctx);
    expect(result).toEqual({ kind: 'status', value: 'OK' });
  });
});

// --- Sparse/Dense transition ---

describe('Sparse/Dense transition', () => {
  it('starts in sparse encoding', () => {
    const { ctx } = createDb();
    hll.pfadd(ctx, ['mykey', 'a']);
    const result = hll.pfdebug(ctx, ['DECODE', 'mykey']);
    // For sparse, DECODE returns a string description
    expect(result.kind).toBe('bulk');
  });

  it('transitions to dense with many unique elements', () => {
    const { ctx } = createDb();
    // Add enough elements to force sparse-to-dense transition
    const elements = [];
    for (let i = 0; i < 2000; i++) {
      elements.push(`element${i}`);
    }
    hll.pfadd(ctx, ['mykey', ...elements]);
    // After many elements, should be dense
    const result = hll.pfdebug(ctx, ['DECODE', 'mykey']);
    if (result.kind === 'bulk') {
      expect((result as { value: string }).value).toContain('dense');
    }
  });
});

// --- Cardinality accuracy ---

describe('Cardinality accuracy', () => {
  it('estimates small cardinalities reasonably', () => {
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

  it('estimates medium cardinalities reasonably', () => {
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
});
