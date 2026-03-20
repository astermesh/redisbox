import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Database } from '../database.ts';
import * as cmd from './generic.ts';

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

describe('DEL', () => {
  it('deletes existing keys and returns count', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', '1');
    db.set('b', 'string', 'raw', '2');
    db.set('c', 'string', 'raw', '3');
    const reply = cmd.del(db, ['a', 'b', 'missing']);
    expect(reply).toEqual({ kind: 'integer', value: 2 });
    expect(db.has('a')).toBe(false);
    expect(db.has('b')).toBe(false);
    expect(db.has('c')).toBe(true);
  });

  it('returns 0 for no matching keys', () => {
    const { db } = createDb();
    expect(cmd.del(db, ['x', 'y'])).toEqual({ kind: 'integer', value: 0 });
  });
});

describe('UNLINK', () => {
  it('behaves identically to DEL', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', '1');
    expect(cmd.unlink(db, ['a'])).toEqual({ kind: 'integer', value: 1 });
    expect(db.has('a')).toBe(false);
  });
});

describe('EXISTS', () => {
  it('counts existing keys', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', '1');
    db.set('b', 'string', 'raw', '2');
    expect(cmd.exists(db, ['a', 'b', 'c'])).toEqual({
      kind: 'integer',
      value: 2,
    });
  });

  it('counts duplicate keys separately', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', '1');
    expect(cmd.exists(db, ['a', 'a'])).toEqual({ kind: 'integer', value: 2 });
  });
});

describe('TYPE', () => {
  it('returns type of existing key', () => {
    const { db } = createDb();
    db.set('k', 'list', 'quicklist', []);
    expect(cmd.type(db, ['k'])).toEqual({ kind: 'status', value: 'list' });
  });

  it('returns none for missing key', () => {
    const { db } = createDb();
    expect(cmd.type(db, ['missing'])).toEqual({
      kind: 'status',
      value: 'none',
    });
  });
});

describe('RENAME', () => {
  it('renames key', () => {
    const { db } = createDb();
    db.set('src', 'string', 'raw', 'val');
    expect(cmd.rename(db, ['src', 'dst'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
    expect(db.get('src')).toBeNull();
    expect(db.get('dst')?.value).toBe('val');
  });

  it('errors if source does not exist', () => {
    const { db } = createDb();
    const reply = cmd.rename(db, ['missing', 'dst']);
    expect(reply.kind).toBe('error');
  });

  it('handles src == dst', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'v');
    expect(cmd.rename(db, ['k', 'k'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
    expect(db.get('k')?.value).toBe('v');
  });

  it('preserves TTL of source', () => {
    const { db } = createDb();
    db.set('src', 'string', 'raw', 'v');
    db.setExpiry('src', 5000);
    cmd.rename(db, ['src', 'dst']);
    expect(db.getExpiry('dst')).toBe(5000);
  });

  it('overwrites destination', () => {
    const { db } = createDb();
    db.set('src', 'string', 'raw', 'new');
    db.set('dst', 'string', 'raw', 'old');
    cmd.rename(db, ['src', 'dst']);
    expect(db.get('dst')?.value).toBe('new');
  });
});

describe('RENAMENX', () => {
  it('renames when destination does not exist', () => {
    const { db } = createDb();
    db.set('src', 'string', 'raw', 'v');
    expect(cmd.renamenx(db, ['src', 'dst'])).toEqual({
      kind: 'integer',
      value: 1,
    });
  });

  it('returns 0 when destination exists', () => {
    const { db } = createDb();
    db.set('src', 'string', 'raw', 'v1');
    db.set('dst', 'string', 'raw', 'v2');
    expect(cmd.renamenx(db, ['src', 'dst'])).toEqual({
      kind: 'integer',
      value: 0,
    });
    expect(db.get('src')?.value).toBe('v1');
  });

  it('returns 0 when src == dst', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'v');
    expect(cmd.renamenx(db, ['k', 'k'])).toEqual({
      kind: 'integer',
      value: 0,
    });
  });

  it('errors if source does not exist', () => {
    const { db } = createDb();
    const reply = cmd.renamenx(db, ['missing', 'dst']);
    expect(reply.kind).toBe('error');
  });
});

describe('PERSIST', () => {
  it('removes expiry and returns 1', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'v');
    db.setExpiry('k', 5000);
    expect(cmd.persist(db, ['k'])).toEqual({ kind: 'integer', value: 1 });
    expect(db.getExpiry('k')).toBeUndefined();
  });

  it('returns 0 if key has no expiry', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'v');
    expect(cmd.persist(db, ['k'])).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns 0 if key does not exist', () => {
    const { db } = createDb();
    expect(cmd.persist(db, ['missing'])).toEqual({
      kind: 'integer',
      value: 0,
    });
  });
});

describe('RANDOMKEY', () => {
  it('returns null for empty db', () => {
    const { db } = createDb();
    expect(cmd.randomkey(db)).toEqual({ kind: 'bulk', value: null });
  });

  it('returns a key from the db', () => {
    const { db } = createDb();
    db.set('only', 'string', 'raw', 'v');
    expect(cmd.randomkey(db)).toEqual({ kind: 'bulk', value: 'only' });
  });
});

describe('TOUCH', () => {
  it('returns count of touched keys', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', '1');
    db.set('b', 'string', 'raw', '2');
    expect(cmd.touch(db, ['a', 'b', 'missing'])).toEqual({
      kind: 'integer',
      value: 2,
    });
  });
});

describe('COPY', () => {
  it('copies key to destination', () => {
    const { db, engine } = createDb();
    db.set('src', 'string', 'raw', 'hello');
    const reply = cmd.copy(engine, db, ['src', 'dst']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
    expect(db.get('dst')?.value).toBe('hello');
  });

  it('returns 0 if source does not exist', () => {
    const { db, engine } = createDb();
    expect(cmd.copy(engine, db, ['missing', 'dst'])).toEqual({
      kind: 'integer',
      value: 0,
    });
  });

  it('returns 0 if destination exists without REPLACE', () => {
    const { db, engine } = createDb();
    db.set('src', 'string', 'raw', 'new');
    db.set('dst', 'string', 'raw', 'old');
    expect(cmd.copy(engine, db, ['src', 'dst'])).toEqual({
      kind: 'integer',
      value: 0,
    });
    expect(db.get('dst')?.value).toBe('old');
  });

  it('overwrites destination with REPLACE', () => {
    const { db, engine } = createDb();
    db.set('src', 'string', 'raw', 'new');
    db.set('dst', 'string', 'raw', 'old');
    expect(cmd.copy(engine, db, ['src', 'dst', 'REPLACE'])).toEqual({
      kind: 'integer',
      value: 1,
    });
    expect(db.get('dst')?.value).toBe('new');
  });

  it('copies to different database with DB flag', () => {
    const { db, engine } = createDb();
    db.set('src', 'string', 'raw', 'hello');
    cmd.copy(engine, db, ['src', 'dst', 'DB', '1']);
    expect(engine.db(1).get('dst')?.value).toBe('hello');
  });

  it('copies TTL', () => {
    const { db, engine } = createDb();
    db.set('src', 'string', 'raw', 'v');
    db.setExpiry('src', 5000);
    cmd.copy(engine, db, ['src', 'dst']);
    expect(db.getExpiry('dst')).toBe(5000);
  });

  it('returns error for unknown flags', () => {
    const { db, engine } = createDb();
    db.set('src', 'string', 'raw', 'v');
    const reply = cmd.copy(engine, db, ['src', 'dst', 'BADOPTION']);
    expect(reply.kind).toBe('error');
    if (reply.kind === 'error') {
      expect(reply.message).toBe('syntax error');
    }
  });

  it('returns error when src and dst are the same key', () => {
    const { db, engine } = createDb();
    db.set('k', 'string', 'raw', 'v');
    const reply = cmd.copy(engine, db, ['k', 'k']);
    expect(reply.kind).toBe('error');
    if (reply.kind === 'error') {
      expect(reply.message).toContain('same');
    }
  });

  it('clears destination TTL when source has no TTL (REPLACE)', () => {
    const { db, engine } = createDb();
    db.set('src', 'string', 'raw', 'new');
    db.set('dst', 'string', 'raw', 'old');
    db.setExpiry('dst', 9000);
    cmd.copy(engine, db, ['src', 'dst', 'REPLACE']);
    expect(db.get('dst')?.value).toBe('new');
    expect(db.getExpiry('dst')).toBeUndefined();
  });
});

describe('OBJECT', () => {
  it('ENCODING returns encoding of key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'embstr', 'hi');
    expect(cmd.objectEncoding(db, ['k'])).toEqual({
      kind: 'bulk',
      value: 'embstr',
    });
  });

  it('ENCODING returns null for missing key', () => {
    const { db } = createDb();
    expect(cmd.objectEncoding(db, ['missing'])).toEqual({
      kind: 'bulk',
      value: null,
    });
  });

  it('REFCOUNT always returns 1', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'v');
    expect(cmd.objectRefcount(db, ['k'])).toEqual({
      kind: 'integer',
      value: 1,
    });
  });

  it('REFCOUNT returns null for missing key', () => {
    const { db } = createDb();
    expect(cmd.objectRefcount(db, ['missing'])).toEqual({
      kind: 'bulk',
      value: null,
    });
  });

  it('IDLETIME returns seconds since last access', () => {
    const { db } = createDb(1000);
    db.set('k', 'string', 'raw', 'v');
    const reply = cmd.objectIdletimeWithClock(db, () => 6000, ['k']);
    expect(reply).toEqual({ kind: 'integer', value: 5 });
  });

  it('FREQ returns frequency counter', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'v');
    expect(cmd.objectFreq(db, ['k'])).toEqual({ kind: 'integer', value: 0 });
  });

  it('HELP returns array of help strings', () => {
    const reply = cmd.objectHelp();
    expect(reply.kind).toBe('array');
  });

  it('object dispatches subcommands', () => {
    const { db } = createDb(1000);
    db.set('k', 'string', 'raw', 'v');
    expect(cmd.object(db, () => 1000, ['ENCODING', 'k']).kind).toBe('bulk');
    expect(cmd.object(db, () => 1000, ['REFCOUNT', 'k']).kind).toBe('integer');
    expect(cmd.object(db, () => 1000, ['IDLETIME', 'k']).kind).toBe('integer');
    expect(cmd.object(db, () => 1000, ['FREQ', 'k']).kind).toBe('integer');
    expect(cmd.object(db, () => 1000, ['HELP']).kind).toBe('array');
  });

  it('object returns error for unknown subcommand', () => {
    const { db } = createDb();
    const reply = cmd.object(db, () => 1000, ['BADCMD', 'k']);
    expect(reply.kind).toBe('error');
  });

  it('object returns error for wrong arg count', () => {
    const { db } = createDb();
    expect(cmd.object(db, () => 1000, []).kind).toBe('error');
    expect(cmd.object(db, () => 1000, ['ENCODING']).kind).toBe('error');
    expect(cmd.object(db, () => 1000, ['ENCODING', 'a', 'b']).kind).toBe(
      'error'
    );
  });
});

describe('DUMP / RESTORE stubs', () => {
  it('DUMP returns error', () => {
    expect(cmd.dump().kind).toBe('error');
  });
  it('RESTORE returns error', () => {
    expect(cmd.restore().kind).toBe('error');
  });
});
