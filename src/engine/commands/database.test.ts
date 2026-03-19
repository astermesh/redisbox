import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { CommandContext } from '../types.ts';
import { ClientState } from '../../server/client-state.ts';
import * as cmd from './database.ts';

function createEngine() {
  return new RedisEngine({ clock: () => 1000, rng: () => 0.5 });
}

interface TestContext extends CommandContext {
  client: ClientState;
}

function createCtx(engine?: RedisEngine, dbIndex = 0): TestContext {
  const e = engine ?? createEngine();
  const client = new ClientState(1, 0);
  client.dbIndex = dbIndex;
  return {
    db: e.db(dbIndex),
    engine: e,
    client,
  };
}

describe('SELECT', () => {
  it('selects database 0', () => {
    const ctx = createCtx();
    expect(cmd.select(ctx, ['0'])).toEqual({ kind: 'status', value: 'OK' });
    expect(ctx.client.dbIndex).toBe(0);
  });

  it('selects database 15', () => {
    const ctx = createCtx();
    expect(cmd.select(ctx, ['15'])).toEqual({ kind: 'status', value: 'OK' });
    expect(ctx.client.dbIndex).toBe(15);
  });

  it('switches db reference in context', () => {
    const engine = createEngine();
    const ctx = createCtx(engine, 0);
    engine.db(5).set('key', 'string', 'raw', 'val');
    cmd.select(ctx, ['5']);
    expect(ctx.db.has('key')).toBe(true);
  });

  it('returns error for index 16', () => {
    const ctx = createCtx();
    expect(cmd.select(ctx, ['16'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'DB index is out of range',
    });
  });

  it('returns error for negative index', () => {
    const ctx = createCtx();
    expect(cmd.select(ctx, ['-1'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'DB index is out of range',
    });
  });

  it('returns error for non-integer', () => {
    const ctx = createCtx();
    expect(cmd.select(ctx, ['abc'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns error for float', () => {
    const ctx = createCtx();
    expect(cmd.select(ctx, ['1.5'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('does not change dbIndex on error', () => {
    const ctx = createCtx();
    ctx.client.dbIndex = 3;
    cmd.select(ctx, ['99']);
    expect(ctx.client.dbIndex).toBe(3);
  });
});

describe('DBSIZE', () => {
  it('returns 0 for empty database', () => {
    const engine = createEngine();
    expect(cmd.dbsize(engine.db(0))).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns correct count', () => {
    const engine = createEngine();
    const db = engine.db(0);
    db.set('a', 'string', 'raw', '1');
    db.set('b', 'string', 'raw', '2');
    db.set('c', 'string', 'raw', '3');
    expect(cmd.dbsize(db)).toEqual({ kind: 'integer', value: 3 });
  });

  it('does not count expired keys (lazy expiration)', () => {
    let now = 1000;
    const engine = new RedisEngine({ clock: () => now, rng: () => 0.5 });
    const db = engine.db(0);
    db.set('a', 'string', 'raw', '1');
    db.set('b', 'string', 'raw', '2');
    db.setExpiry('b', 1500);
    now = 2000;
    // DBSIZE returns raw count; lazy expiration happens on access
    // Redis DBSIZE also returns raw count (includes not-yet-expired keys)
    // but after any access that triggers lazy expiry, count decreases
    db.has('b'); // triggers lazy expiry
    expect(cmd.dbsize(db)).toEqual({ kind: 'integer', value: 1 });
  });
});

describe('FLUSHDB', () => {
  it('clears all keys in current database', () => {
    const engine = createEngine();
    const db = engine.db(0);
    db.set('a', 'string', 'raw', '1');
    db.set('b', 'string', 'raw', '2');
    const ctx = createCtx(engine, 0);
    expect(cmd.flushdb(ctx, [])).toEqual({ kind: 'status', value: 'OK' });
    expect(db.size).toBe(0);
  });

  it('clears expiry index', () => {
    const engine = createEngine();
    const db = engine.db(0);
    db.set('a', 'string', 'raw', '1');
    db.setExpiry('a', 5000);
    const ctx = createCtx(engine, 0);
    cmd.flushdb(ctx, []);
    expect(db.expirySize).toBe(0);
  });

  it('does not affect other databases', () => {
    const engine = createEngine();
    engine.db(0).set('a', 'string', 'raw', '1');
    engine.db(1).set('b', 'string', 'raw', '2');
    const ctx = createCtx(engine, 0);
    cmd.flushdb(ctx, []);
    expect(engine.db(0).size).toBe(0);
    expect(engine.db(1).size).toBe(1);
  });

  it('accepts ASYNC flag', () => {
    const ctx = createCtx();
    ctx.db.set('a', 'string', 'raw', '1');
    expect(cmd.flushdb(ctx, ['ASYNC'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
    expect(ctx.db.size).toBe(0);
  });

  it('accepts SYNC flag', () => {
    const ctx = createCtx();
    ctx.db.set('a', 'string', 'raw', '1');
    expect(cmd.flushdb(ctx, ['SYNC'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
    expect(ctx.db.size).toBe(0);
  });

  it('accepts lowercase async flag', () => {
    const ctx = createCtx();
    ctx.db.set('a', 'string', 'raw', '1');
    expect(cmd.flushdb(ctx, ['async'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
  });

  it('returns wrong number of arguments error for too many args', () => {
    const ctx = createCtx();
    expect(cmd.flushdb(ctx, ['ASYNC', 'extra'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "wrong number of arguments for 'flushdb' command",
    });
  });

  it('returns error for invalid flag', () => {
    const ctx = createCtx();
    expect(cmd.flushdb(ctx, ['INVALID'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'FLUSHALL can call with no argument or a single argument ASYNC|SYNC',
    });
  });
});

describe('FLUSHALL', () => {
  it('clears all databases', () => {
    const engine = createEngine();
    engine.db(0).set('a', 'string', 'raw', '1');
    engine.db(1).set('b', 'string', 'raw', '2');
    engine.db(15).set('c', 'string', 'raw', '3');
    const ctx = createCtx(engine, 0);
    expect(cmd.flushall(ctx, [])).toEqual({ kind: 'status', value: 'OK' });
    expect(engine.db(0).size).toBe(0);
    expect(engine.db(1).size).toBe(0);
    expect(engine.db(15).size).toBe(0);
  });

  it('clears expiry indexes across all databases', () => {
    const engine = createEngine();
    engine.db(0).set('a', 'string', 'raw', '1');
    engine.db(0).setExpiry('a', 5000);
    engine.db(3).set('b', 'string', 'raw', '2');
    engine.db(3).setExpiry('b', 5000);
    const ctx = createCtx(engine, 0);
    cmd.flushall(ctx, []);
    expect(engine.db(0).expirySize).toBe(0);
    expect(engine.db(3).expirySize).toBe(0);
  });

  it('accepts ASYNC flag', () => {
    const engine = createEngine();
    engine.db(0).set('a', 'string', 'raw', '1');
    const ctx = createCtx(engine, 0);
    expect(cmd.flushall(ctx, ['ASYNC'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
  });

  it('accepts SYNC flag', () => {
    const engine = createEngine();
    const ctx = createCtx(engine, 0);
    expect(cmd.flushall(ctx, ['SYNC'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
  });

  it('returns wrong number of arguments error for too many args', () => {
    const ctx = createCtx();
    expect(cmd.flushall(ctx, ['ASYNC', 'extra'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "wrong number of arguments for 'flushall' command",
    });
  });

  it('returns error for invalid flag', () => {
    const ctx = createCtx();
    expect(cmd.flushall(ctx, ['BADARG'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'FLUSHALL can call with no argument or a single argument ASYNC|SYNC',
    });
  });
});

describe('SWAPDB', () => {
  it('swaps two databases', () => {
    const engine = createEngine();
    engine.db(0).set('key0', 'string', 'raw', 'val0');
    engine.db(1).set('key1', 'string', 'raw', 'val1');
    expect(cmd.swapdb(engine, ['0', '1'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
    expect(engine.db(0).has('key1')).toBe(true);
    expect(engine.db(0).has('key0')).toBe(false);
    expect(engine.db(1).has('key0')).toBe(true);
    expect(engine.db(1).has('key1')).toBe(false);
  });

  it('returns OK for same index (no-op)', () => {
    const engine = createEngine();
    engine.db(3).set('key', 'string', 'raw', 'val');
    expect(cmd.swapdb(engine, ['3', '3'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
    expect(engine.db(3).has('key')).toBe(true);
  });

  it('swaps expiry data', () => {
    const engine = createEngine();
    engine.db(0).set('a', 'string', 'raw', '1');
    engine.db(0).setExpiry('a', 5000);
    expect(cmd.swapdb(engine, ['0', '2'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
    expect(engine.db(2).has('a')).toBe(true);
    expect(engine.db(2).getExpiry('a')).toBe(5000);
    expect(engine.db(0).size).toBe(0);
  });

  it('returns error for non-integer index', () => {
    const engine = createEngine();
    expect(cmd.swapdb(engine, ['abc', '1'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns error for non-integer second index', () => {
    const engine = createEngine();
    expect(cmd.swapdb(engine, ['1', 'xyz'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns error for out of range index', () => {
    const engine = createEngine();
    expect(cmd.swapdb(engine, ['0', '16'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'invalid DB index',
    });
  });

  it('returns error for negative index', () => {
    const engine = createEngine();
    expect(cmd.swapdb(engine, ['-1', '0'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'invalid DB index',
    });
  });
});
