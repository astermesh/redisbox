import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Database } from '../database.ts';
import * as cmd from './memory.ts';

function createDb(time = 1000): {
  db: Database;
  engine: RedisEngine;
  setTime: (t: number) => void;
} {
  let now = time;
  const engine = new RedisEngine({
    clock: () => now,
    rng: () => 0.5,
  });
  return {
    db: engine.db(0),
    engine,
    setTime: (t: number) => {
      now = t;
    },
  };
}

describe('MEMORY USAGE', () => {
  it('returns null for non-existent key', () => {
    const { db } = createDb();
    expect(cmd.memoryUsage(db, ['missing'])).toEqual({
      kind: 'bulk',
      value: null,
    });
  });

  it('returns positive integer for string key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello world');
    const reply = cmd.memoryUsage(db, ['k']);
    expect(reply.kind).toBe('integer');
    expect((reply as { value: number }).value).toBeGreaterThan(0);
  });

  it('returns positive integer for int-encoded string', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '42');
    const reply = cmd.memoryUsage(db, ['k']);
    expect(reply.kind).toBe('integer');
    expect((reply as { value: number }).value).toBeGreaterThan(0);
  });

  it('returns positive integer for hash key', () => {
    const { db } = createDb();
    const hash = new Map([
      ['f1', 'v1'],
      ['f2', 'v2'],
    ]);
    db.set('h', 'hash', 'listpack', hash);
    const reply = cmd.memoryUsage(db, ['h']);
    expect(reply.kind).toBe('integer');
    expect((reply as { value: number }).value).toBeGreaterThan(0);
  });

  it('returns positive integer for list key', () => {
    const { db } = createDb();
    db.set('l', 'list', 'listpack', ['a', 'b', 'c']);
    const reply = cmd.memoryUsage(db, ['l']);
    expect(reply.kind).toBe('integer');
    expect((reply as { value: number }).value).toBeGreaterThan(0);
  });

  it('returns positive integer for set key', () => {
    const { db } = createDb();
    db.set('s', 'set', 'hashtable', new Set(['a', 'b']));
    const reply = cmd.memoryUsage(db, ['s']);
    expect(reply.kind).toBe('integer');
    expect((reply as { value: number }).value).toBeGreaterThan(0);
  });

  it('returns positive integer for zset key', () => {
    const { db } = createDb();
    db.set(
      'z',
      'zset',
      'skiplist',
      new Map([
        ['a', 1],
        ['b', 2],
      ])
    );
    const reply = cmd.memoryUsage(db, ['z']);
    expect(reply.kind).toBe('integer');
    expect((reply as { value: number }).value).toBeGreaterThan(0);
  });

  it('includes expiry overhead when key has TTL', () => {
    const { db } = createDb();
    db.set('k1', 'string', 'raw', 'val');
    db.set('k2', 'string', 'raw', 'val');
    db.setExpiry('k2', 9999);

    const r1 = cmd.memoryUsage(db, ['k1']);
    const r2 = cmd.memoryUsage(db, ['k2']);
    expect((r2 as { value: number }).value).toBeGreaterThan(
      (r1 as { value: number }).value
    );
  });

  it('accepts SAMPLES option', () => {
    const { db } = createDb();
    const hash = new Map<string, string>();
    for (let i = 0; i < 100; i++) hash.set(`f${i}`, `v${i}`);
    db.set('h', 'hash', 'hashtable', hash);

    const r1 = cmd.memoryUsage(db, ['h']);
    const r2 = cmd.memoryUsage(db, ['h', 'SAMPLES', '0']);
    const r3 = cmd.memoryUsage(db, ['h', 'SAMPLES', '5']);

    expect(r1.kind).toBe('integer');
    expect(r2.kind).toBe('integer');
    expect(r3.kind).toBe('integer');
  });

  it('returns error for invalid SAMPLES', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    const reply = cmd.memoryUsage(db, ['k', 'SAMPLES', 'abc']);
    expect(reply.kind).toBe('error');
  });

  it('returns error for negative SAMPLES', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    const reply = cmd.memoryUsage(db, ['k', 'SAMPLES', '-1']);
    expect(reply.kind).toBe('error');
  });

  it('returns syntax error for wrong option name', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    const reply = cmd.memoryUsage(db, ['k', 'NOTSAMPLES', '5']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('returns arity error for too many args', () => {
    const { db } = createDb();
    const reply = cmd.memoryUsage(db, ['k', 'SAMPLES', '5', 'extra']);
    expect(reply.kind).toBe('error');
  });
});

describe('MEMORY DOCTOR', () => {
  it('returns bulk string', () => {
    const reply = cmd.memoryDoctor();
    expect(reply.kind).toBe('bulk');
    expect((reply as { value: string }).value).toContain('no memory problems');
  });
});

describe('MEMORY MALLOC-STATS', () => {
  it('returns bulk string', () => {
    const reply = cmd.memoryMallocStats();
    expect(reply.kind).toBe('bulk');
  });
});

describe('MEMORY PURGE', () => {
  it('returns OK', () => {
    expect(cmd.memoryPurge()).toEqual({ kind: 'status', value: 'OK' });
  });
});

describe('MEMORY STATS', () => {
  it('returns array with memory info', () => {
    const { engine } = createDb();
    const reply = cmd.memoryStats(engine);
    expect(reply.kind).toBe('array');
    const arr = (reply as { value: unknown[] }).value;
    expect(arr.length).toBeGreaterThan(0);
  });

  it('includes keys.count of 0 for empty db', () => {
    const { engine } = createDb();
    const reply = cmd.memoryStats(engine);
    const arr = (reply as { value: { kind: string; value: unknown }[] }).value;
    const idx = arr.findIndex(
      (r) => r.kind === 'bulk' && r.value === 'keys.count'
    );
    expect(idx).toBeGreaterThanOrEqual(0);
    expect(arr[idx + 1]).toEqual({ kind: 'integer', value: 0 });
  });

  it('reflects key count after adding keys', () => {
    const { db, engine } = createDb();
    db.set('a', 'string', 'raw', 'v');
    db.set('b', 'string', 'raw', 'v');
    const reply = cmd.memoryStats(engine);
    const arr = (reply as { value: { kind: string; value: unknown }[] }).value;
    const idx = arr.findIndex(
      (r) => r.kind === 'bulk' && r.value === 'keys.count'
    );
    expect(arr[idx + 1]).toEqual({ kind: 'integer', value: 2 });
  });
});

describe('MEMORY HELP', () => {
  it('returns array of help lines', () => {
    const reply = cmd.memoryHelp();
    expect(reply.kind).toBe('array');
    const arr = (reply as { value: unknown[] }).value;
    expect(arr.length).toBeGreaterThan(0);
  });
});

describe('MEMORY dispatcher', () => {
  it('dispatches USAGE subcommand', () => {
    const { db, engine } = createDb();
    db.set('k', 'string', 'raw', 'val');
    const reply = cmd.memory(db, engine, ['USAGE', 'k']);
    expect(reply.kind).toBe('integer');
  });

  it('dispatches case-insensitive subcommands', () => {
    const { db, engine } = createDb();
    expect(cmd.memory(db, engine, ['help']).kind).toBe('array');
    expect(cmd.memory(db, engine, ['HELP']).kind).toBe('array');
    expect(cmd.memory(db, engine, ['Help']).kind).toBe('array');
  });

  it('returns error for unknown subcommand', () => {
    const { db, engine } = createDb();
    const reply = cmd.memory(db, engine, ['UNKNOWN']);
    expect(reply.kind).toBe('error');
  });

  it('returns arity error for empty args', () => {
    const { db, engine } = createDb();
    const reply = cmd.memory(db, engine, []);
    expect(reply.kind).toBe('error');
  });
});

describe('engine.usedMemory', () => {
  it('returns 0 for empty engine', () => {
    const { engine } = createDb();
    expect(engine.usedMemory()).toBe(0);
  });

  it('increases after adding keys', () => {
    const { db, engine } = createDb();
    const before = engine.usedMemory();
    db.set('key1', 'string', 'raw', 'value1');
    const after = engine.usedMemory();
    expect(after).toBeGreaterThan(before);
  });

  it('decreases after deleting keys', () => {
    const { db, engine } = createDb();
    db.set('key1', 'string', 'raw', 'value1');
    const before = engine.usedMemory();
    db.delete('key1');
    const after = engine.usedMemory();
    expect(after).toBeLessThan(before);
  });

  it('sums across multiple databases', () => {
    const { engine } = createDb();
    engine.db(0).set('a', 'string', 'raw', 'v');
    const oneDb = engine.usedMemory();
    engine.db(1).set('b', 'string', 'raw', 'v');
    const twoDbs = engine.usedMemory();
    expect(twoDbs).toBeGreaterThan(oneDb);
  });
});
