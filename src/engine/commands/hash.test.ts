import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import * as hash from './hash.ts';

function createDb(): { db: Database; engine: RedisEngine } {
  const engine = new RedisEngine({ clock: () => 1000, rng: () => 0.5 });
  return { db: engine.db(0), engine };
}

function bulk(value: string | null): Reply {
  return { kind: 'bulk', value };
}

function integer(value: number): Reply {
  return { kind: 'integer', value };
}

function arr(...items: Reply[]): Reply {
  return { kind: 'array', value: items };
}

const NIL = bulk(null);
const OK: Reply = { kind: 'status', value: 'OK' };
const ZERO = integer(0);
const ONE = integer(1);
const EMPTY_ARRAY: Reply = { kind: 'array', value: [] };
const WRONGTYPE: Reply = {
  kind: 'error',
  prefix: 'WRONGTYPE',
  message: 'Operation against a key holding the wrong kind of value',
};

// --- HSET ---

describe('HSET', () => {
  it('creates hash with single field', () => {
    const { db } = createDb();
    expect(hash.hset(db, ['k', 'f1', 'v1'])).toEqual(ONE);
    expect(hash.hget(db, ['k', 'f1'])).toEqual(bulk('v1'));
  });

  it('creates hash with multiple fields', () => {
    const { db } = createDb();
    expect(hash.hset(db, ['k', 'f1', 'v1', 'f2', 'v2'])).toEqual(integer(2));
  });

  it('returns count of new fields only (not updated)', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    expect(hash.hset(db, ['k', 'f1', 'updated', 'f2', 'v2'])).toEqual(ONE);
  });

  it('updates existing field value', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    hash.hset(db, ['k', 'f1', 'v2']);
    expect(hash.hget(db, ['k', 'f1'])).toEqual(bulk('v2'));
  });

  it('returns WRONGTYPE for non-hash key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(hash.hset(db, ['k', 'f1', 'v1'])).toEqual(WRONGTYPE);
  });

  it('returns error for odd number of field-value args', () => {
    const { db } = createDb();
    const result = hash.hset(db, ['k', 'f1']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "wrong number of arguments for 'hset' command",
    });
  });
});

// --- HGET ---

describe('HGET', () => {
  it('returns value for existing field', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    expect(hash.hget(db, ['k', 'f1'])).toEqual(bulk('v1'));
  });

  it('returns nil for non-existing field', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    expect(hash.hget(db, ['k', 'f2'])).toEqual(NIL);
  });

  it('returns nil for non-existing key', () => {
    const { db } = createDb();
    expect(hash.hget(db, ['k', 'f1'])).toEqual(NIL);
  });

  it('returns WRONGTYPE for non-hash key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(hash.hget(db, ['k', 'f1'])).toEqual(WRONGTYPE);
  });
});

// --- HMSET ---

describe('HMSET', () => {
  it('sets multiple fields and returns OK', () => {
    const { db } = createDb();
    expect(hash.hmset(db, ['k', 'f1', 'v1', 'f2', 'v2'])).toEqual(OK);
    expect(hash.hget(db, ['k', 'f1'])).toEqual(bulk('v1'));
    expect(hash.hget(db, ['k', 'f2'])).toEqual(bulk('v2'));
  });

  it('overwrites existing fields', () => {
    const { db } = createDb();
    hash.hmset(db, ['k', 'f1', 'v1']);
    hash.hmset(db, ['k', 'f1', 'v2']);
    expect(hash.hget(db, ['k', 'f1'])).toEqual(bulk('v2'));
  });

  it('returns error for odd field-value args', () => {
    const { db } = createDb();
    const result = hash.hmset(db, ['k', 'f1']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "wrong number of arguments for 'hmset' command",
    });
  });

  it('returns WRONGTYPE for non-hash key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(hash.hmset(db, ['k', 'f1', 'v1'])).toEqual(WRONGTYPE);
  });
});

// --- HMGET ---

describe('HMGET', () => {
  it('returns values for existing fields', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1', 'f2', 'v2']);
    expect(hash.hmget(db, ['k', 'f1', 'f2'])).toEqual(
      arr(bulk('v1'), bulk('v2'))
    );
  });

  it('returns nil for missing fields', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    expect(hash.hmget(db, ['k', 'f1', 'f2'])).toEqual(arr(bulk('v1'), NIL));
  });

  it('returns all nils for non-existing key', () => {
    const { db } = createDb();
    expect(hash.hmget(db, ['k', 'f1', 'f2'])).toEqual(arr(NIL, NIL));
  });

  it('returns WRONGTYPE for non-hash key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(hash.hmget(db, ['k', 'f1'])).toEqual(WRONGTYPE);
  });
});

// --- HGETALL ---

describe('HGETALL', () => {
  it('returns all field-value pairs', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1', 'f2', 'v2']);
    const result = hash.hgetall(db, ['k']);
    expect(result).toEqual(arr(bulk('f1'), bulk('v1'), bulk('f2'), bulk('v2')));
  });

  it('returns empty array for non-existing key', () => {
    const { db } = createDb();
    expect(hash.hgetall(db, ['k'])).toEqual(EMPTY_ARRAY);
  });

  it('returns WRONGTYPE for non-hash key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(hash.hgetall(db, ['k'])).toEqual(WRONGTYPE);
  });
});

// --- HDEL ---

describe('HDEL', () => {
  it('deletes existing field', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1', 'f2', 'v2']);
    expect(hash.hdel(db, ['k', 'f1'])).toEqual(ONE);
    expect(hash.hget(db, ['k', 'f1'])).toEqual(NIL);
  });

  it('returns 0 for non-existing field', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    expect(hash.hdel(db, ['k', 'f2'])).toEqual(ZERO);
  });

  it('deletes multiple fields', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1', 'f2', 'v2', 'f3', 'v3']);
    expect(hash.hdel(db, ['k', 'f1', 'f2', 'f4'])).toEqual(integer(2));
  });

  it('deletes key when hash becomes empty', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    hash.hdel(db, ['k', 'f1']);
    expect(db.has('k')).toBe(false);
  });

  it('returns 0 for non-existing key', () => {
    const { db } = createDb();
    expect(hash.hdel(db, ['k', 'f1'])).toEqual(ZERO);
  });

  it('returns WRONGTYPE for non-hash key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(hash.hdel(db, ['k', 'f1'])).toEqual(WRONGTYPE);
  });
});

// --- HEXISTS ---

describe('HEXISTS', () => {
  it('returns 1 for existing field', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    expect(hash.hexists(db, ['k', 'f1'])).toEqual(ONE);
  });

  it('returns 0 for non-existing field', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    expect(hash.hexists(db, ['k', 'f2'])).toEqual(ZERO);
  });

  it('returns 0 for non-existing key', () => {
    const { db } = createDb();
    expect(hash.hexists(db, ['k', 'f1'])).toEqual(ZERO);
  });

  it('returns WRONGTYPE for non-hash key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(hash.hexists(db, ['k', 'f1'])).toEqual(WRONGTYPE);
  });
});

// --- HLEN ---

describe('HLEN', () => {
  it('returns number of fields', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1', 'f2', 'v2']);
    expect(hash.hlen(db, ['k'])).toEqual(integer(2));
  });

  it('returns 0 for non-existing key', () => {
    const { db } = createDb();
    expect(hash.hlen(db, ['k'])).toEqual(ZERO);
  });

  it('returns WRONGTYPE for non-hash key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(hash.hlen(db, ['k'])).toEqual(WRONGTYPE);
  });
});

// --- HKEYS ---

describe('HKEYS', () => {
  it('returns all field names', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1', 'f2', 'v2']);
    expect(hash.hkeys(db, ['k'])).toEqual(arr(bulk('f1'), bulk('f2')));
  });

  it('returns empty array for non-existing key', () => {
    const { db } = createDb();
    expect(hash.hkeys(db, ['k'])).toEqual(EMPTY_ARRAY);
  });

  it('returns WRONGTYPE for non-hash key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(hash.hkeys(db, ['k'])).toEqual(WRONGTYPE);
  });
});

// --- HVALS ---

describe('HVALS', () => {
  it('returns all values', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1', 'f2', 'v2']);
    expect(hash.hvals(db, ['k'])).toEqual(arr(bulk('v1'), bulk('v2')));
  });

  it('returns empty array for non-existing key', () => {
    const { db } = createDb();
    expect(hash.hvals(db, ['k'])).toEqual(EMPTY_ARRAY);
  });

  it('returns WRONGTYPE for non-hash key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(hash.hvals(db, ['k'])).toEqual(WRONGTYPE);
  });
});

// --- HSETNX ---

describe('HSETNX', () => {
  it('sets field when it does not exist', () => {
    const { db } = createDb();
    expect(hash.hsetnx(db, ['k', 'f1', 'v1'])).toEqual(ONE);
    expect(hash.hget(db, ['k', 'f1'])).toEqual(bulk('v1'));
  });

  it('does not overwrite existing field', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    expect(hash.hsetnx(db, ['k', 'f1', 'v2'])).toEqual(ZERO);
    expect(hash.hget(db, ['k', 'f1'])).toEqual(bulk('v1'));
  });

  it('creates hash if key does not exist', () => {
    const { db } = createDb();
    hash.hsetnx(db, ['k', 'f1', 'v1']);
    expect(hash.hlen(db, ['k'])).toEqual(ONE);
  });

  it('returns WRONGTYPE for non-hash key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(hash.hsetnx(db, ['k', 'f1', 'v1'])).toEqual(WRONGTYPE);
  });
});

// --- Encoding transitions ---

describe('encoding transitions', () => {
  it('uses listpack for small hashes', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    const entry = db.get('k');
    expect(entry?.encoding).toBe('listpack');
  });

  it('transitions to hashtable when exceeding entry count', () => {
    const { db } = createDb();
    // Default threshold is 128 entries
    const fields: string[] = ['k'];
    for (let i = 0; i <= 128; i++) {
      fields.push(`f${i}`, `v${i}`);
    }
    hash.hset(db, fields);
    const entry = db.get('k');
    expect(entry?.encoding).toBe('hashtable');
  });

  it('transitions to hashtable when field exceeds value size', () => {
    const { db } = createDb();
    const longValue = 'x'.repeat(65); // > 64 bytes
    hash.hset(db, ['k', 'f1', longValue]);
    const entry = db.get('k');
    expect(entry?.encoding).toBe('hashtable');
  });

  it('transitions to hashtable when field name exceeds size', () => {
    const { db } = createDb();
    const longField = 'f'.repeat(65);
    hash.hset(db, ['k', longField, 'v1']);
    const entry = db.get('k');
    expect(entry?.encoding).toBe('hashtable');
  });

  it('stays listpack at exact threshold', () => {
    const { db } = createDb();
    // Exactly 128 entries, each ≤64 bytes
    const fields: string[] = ['k'];
    for (let i = 0; i < 128; i++) {
      fields.push(`f${i}`, `v${i}`);
    }
    hash.hset(db, fields);
    const entry = db.get('k');
    expect(entry?.encoding).toBe('listpack');
  });

  it('stays listpack at exact value size threshold', () => {
    const { db } = createDb();
    const exactValue = 'x'.repeat(64); // exactly 64 bytes
    hash.hset(db, ['k', 'f1', exactValue]);
    const entry = db.get('k');
    expect(entry?.encoding).toBe('listpack');
  });

  it('transitions back to listpack after HDEL reduces size', () => {
    const { db } = createDb();
    // Create 129 entries (exceeds threshold)
    const fields: string[] = ['k'];
    for (let i = 0; i < 129; i++) {
      fields.push(`f${i}`, `v${i}`);
    }
    hash.hset(db, fields);
    expect(db.get('k')?.encoding).toBe('hashtable');

    // Delete one to go back to 128
    hash.hdel(db, ['k', 'f0']);
    expect(db.get('k')?.encoding).toBe('listpack');
  });
});

// --- HDEL cleans up field expiry ---

describe('HDEL field expiry cleanup', () => {
  it('removes field expiry when field is deleted', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1', 'f2', 'v2']);
    db.setFieldExpiry('k', 'f1', 2000);
    hash.hdel(db, ['k', 'f1']);
    expect(db.getFieldExpiry('k', 'f1')).toBeUndefined();
  });
});
