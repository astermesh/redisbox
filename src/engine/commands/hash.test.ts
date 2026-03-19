import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import * as hash from './hash.ts';

let rngValue = 0.5;
function createDb(): { db: Database; engine: RedisEngine } {
  rngValue = 0.5;
  const engine = new RedisEngine({ clock: () => 1000, rng: () => rngValue });
  return { db: engine.db(0), engine };
}

function bulk(value: string | null): Reply {
  return { kind: 'bulk', value };
}

function integer(value: number | bigint): Reply {
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

  it('stays hashtable after replacing long value with short one', () => {
    const { db } = createDb();
    const longValue = 'x'.repeat(65);
    hash.hset(db, ['k', 'f1', longValue]);
    expect(db.get('k')?.encoding).toBe('hashtable');

    // Replace with short value — encoding stays hashtable (Redis never demotes)
    hash.hset(db, ['k', 'f1', 'short']);
    expect(db.get('k')?.encoding).toBe('hashtable');
  });

  it('stays hashtable after HDEL reduces size below threshold', () => {
    const { db } = createDb();
    // Create 129 entries (exceeds threshold)
    const fields: string[] = ['k'];
    for (let i = 0; i < 129; i++) {
      fields.push(`f${i}`, `v${i}`);
    }
    hash.hset(db, fields);
    expect(db.get('k')?.encoding).toBe('hashtable');

    // Delete one to go back to 128 — encoding stays hashtable (Redis never demotes)
    hash.hdel(db, ['k', 'f0']);
    expect(db.get('k')?.encoding).toBe('hashtable');
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

// --- HINCRBY ---

describe('HINCRBY', () => {
  it('creates hash and field when key does not exist', () => {
    const { db } = createDb();
    expect(hash.hincrby(db, ['k', 'f1', '5'])).toEqual(integer(5));
    expect(hash.hget(db, ['k', 'f1'])).toEqual(bulk('5'));
  });

  it('creates field when field does not exist', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    expect(hash.hincrby(db, ['k', 'f2', '10'])).toEqual(integer(10));
  });

  it('increments existing integer field', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', '10']);
    expect(hash.hincrby(db, ['k', 'f1', '5'])).toEqual(integer(15));
    expect(hash.hget(db, ['k', 'f1'])).toEqual(bulk('15'));
  });

  it('decrements with negative increment', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', '10']);
    expect(hash.hincrby(db, ['k', 'f1', '-3'])).toEqual(integer(7));
  });

  it('returns error for non-integer field value', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'hello']);
    expect(hash.hincrby(db, ['k', 'f1', '1'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'hash value is not an integer',
    });
  });

  it('returns error for non-integer increment', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', '10']);
    expect(hash.hincrby(db, ['k', 'f1', 'abc'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns error for float increment', () => {
    const { db } = createDb();
    expect(hash.hincrby(db, ['k', 'f1', '1.5'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('handles overflow', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', '9223372036854775807']);
    expect(hash.hincrby(db, ['k', 'f1', '1'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'increment or decrement would overflow',
    });
  });

  it('handles underflow', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', '-9223372036854775808']);
    expect(hash.hincrby(db, ['k', 'f1', '-1'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'increment or decrement would overflow',
    });
  });

  it('returns WRONGTYPE for non-hash key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(hash.hincrby(db, ['k', 'f1', '1'])).toEqual(WRONGTYPE);
  });

  it('handles large 64-bit values', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', '9223372036854775800']);
    expect(hash.hincrby(db, ['k', 'f1', '6'])).toEqual(
      integer(9223372036854775806n)
    );
  });
});

// --- HINCRBYFLOAT ---

describe('HINCRBYFLOAT', () => {
  it('creates hash and field when key does not exist', () => {
    const { db } = createDb();
    expect(hash.hincrbyfloat(db, ['k', 'f1', '2.5'])).toEqual(bulk('2.5'));
    expect(hash.hget(db, ['k', 'f1'])).toEqual(bulk('2.5'));
  });

  it('creates field when field does not exist', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    expect(hash.hincrbyfloat(db, ['k', 'f2', '3.14'])).toEqual(bulk('3.14'));
  });

  it('increments existing float field', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', '10.5']);
    expect(hash.hincrbyfloat(db, ['k', 'f1', '0.1'])).toEqual(bulk('10.6'));
  });

  it('increments integer field with float', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', '10']);
    expect(hash.hincrbyfloat(db, ['k', 'f1', '1.5'])).toEqual(bulk('11.5'));
  });

  it('handles negative increment', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', '10']);
    expect(hash.hincrbyfloat(db, ['k', 'f1', '-5.5'])).toEqual(bulk('4.5'));
  });

  it('returns error for non-float field value', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'hello']);
    expect(hash.hincrbyfloat(db, ['k', 'f1', '1.0'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'hash value is not a valid float',
    });
  });

  it('returns error for non-float increment', () => {
    const { db } = createDb();
    expect(hash.hincrbyfloat(db, ['k', 'f1', 'abc'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not a valid float',
    });
  });

  it('returns error for inf increment', () => {
    const { db } = createDb();
    expect(hash.hincrbyfloat(db, ['k', 'f1', 'inf'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'increment would produce NaN or Infinity',
    });
  });

  it('returns error when result would be infinity', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', '1.7e308']);
    expect(hash.hincrbyfloat(db, ['k', 'f1', '1.7e308'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'increment would produce NaN or Infinity',
    });
  });

  it('formats result with zero correctly', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', '5']);
    expect(hash.hincrbyfloat(db, ['k', 'f1', '-5'])).toEqual(bulk('0'));
  });

  it('returns WRONGTYPE for non-hash key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(hash.hincrbyfloat(db, ['k', 'f1', '1.0'])).toEqual(WRONGTYPE);
  });
});

// --- HRANDFIELD ---

describe('HRANDFIELD', () => {
  it('returns nil for non-existing key (no count)', () => {
    const { db } = createDb();
    expect(hash.hrandfield(db, ['k'], () => 0.5)).toEqual(NIL);
  });

  it('returns a field name for existing key', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1', 'f2', 'v2']);
    const result = hash.hrandfield(db, ['k'], () => 0);
    expect(result).toEqual(bulk('f1'));
  });

  it('returns empty array for count=0', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    expect(hash.hrandfield(db, ['k', '0'], () => 0.5)).toEqual(EMPTY_ARRAY);
  });

  it('returns unique fields with positive count', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1', 'f2', 'v2', 'f3', 'v3']);
    const result = hash.hrandfield(db, ['k', '2'], () => 0.1);
    expect(result.kind).toBe('array');
    if (result.kind === 'array') {
      expect(result.value.length).toBe(2);
      // All unique
      const fields = result.value.map((r) =>
        r.kind === 'bulk' ? r.value : null
      );
      expect(new Set(fields).size).toBe(2);
    }
  });

  it('returns at most hash size with positive count > size', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1', 'f2', 'v2']);
    const result = hash.hrandfield(db, ['k', '10'], () => 0.1);
    expect(result.kind).toBe('array');
    if (result.kind === 'array') {
      expect(result.value.length).toBe(2);
    }
  });

  it('returns |count| fields with negative count (may duplicate)', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    const result = hash.hrandfield(db, ['k', '-5'], () => 0);
    expect(result.kind).toBe('array');
    if (result.kind === 'array') {
      expect(result.value.length).toBe(5);
      // All should be f1 since there's only one field
      for (const r of result.value) {
        expect(r).toEqual(bulk('f1'));
      }
    }
  });

  it('returns field-value pairs with WITHVALUES', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    const result = hash.hrandfield(db, ['k', '1', 'WITHVALUES'], () => 0);
    expect(result).toEqual(arr(bulk('f1'), bulk('v1')));
  });

  it('returns field-value pairs with negative count and WITHVALUES', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    const result = hash.hrandfield(db, ['k', '-2', 'WITHVALUES'], () => 0);
    expect(result).toEqual(arr(bulk('f1'), bulk('v1'), bulk('f1'), bulk('v1')));
  });

  it('returns empty array for non-existing key with count', () => {
    const { db } = createDb();
    expect(hash.hrandfield(db, ['k', '3'], () => 0.5)).toEqual(EMPTY_ARRAY);
  });

  it('returns WRONGTYPE for non-hash key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(hash.hrandfield(db, ['k'], () => 0.5)).toEqual(WRONGTYPE);
  });

  it('returns WRONGTYPE for non-hash key with count', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(hash.hrandfield(db, ['k', '1'], () => 0.5)).toEqual(WRONGTYPE);
  });

  it('is case-insensitive for WITHVALUES', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    const result = hash.hrandfield(db, ['k', '1', 'withvalues'], () => 0);
    expect(result).toEqual(arr(bulk('f1'), bulk('v1')));
  });

  it('returns error for invalid count', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    expect(hash.hrandfield(db, ['k', 'abc'], () => 0.5)).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns syntax error for extra arguments without WITHVALUES', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    expect(hash.hrandfield(db, ['k', '1', 'EXTRA'], () => 0.5)).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });
});

// --- HSCAN ---

describe('HSCAN', () => {
  it('scans all fields of a hash', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1', 'f2', 'v2']);
    const result = hash.hscan(db, ['k', '0']);
    expect(result).toEqual(
      arr(bulk('0'), arr(bulk('f1'), bulk('v1'), bulk('f2'), bulk('v2')))
    );
  });

  it('returns empty scan for non-existing key', () => {
    const { db } = createDb();
    expect(hash.hscan(db, ['k', '0'])).toEqual(arr(bulk('0'), EMPTY_ARRAY));
  });

  it('returns WRONGTYPE for non-hash key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(hash.hscan(db, ['k', '0'])).toEqual(WRONGTYPE);
  });

  it('filters fields with MATCH pattern', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'alpha', '1', 'beta', '2', 'abc', '3']);
    const result = hash.hscan(db, ['k', '0', 'MATCH', 'a*']);
    expect(result).toEqual(
      arr(bulk('0'), arr(bulk('alpha'), bulk('1'), bulk('abc'), bulk('3')))
    );
  });

  it('uses COUNT to limit batch size', () => {
    const { db } = createDb();
    // Create 20 fields
    const args = ['k'];
    for (let i = 0; i < 20; i++) {
      args.push(`f${String(i).padStart(2, '0')}`, `v${i}`);
    }
    hash.hset(db, args);

    // Scan with count=5
    const result1 = hash.hscan(db, ['k', '0', 'COUNT', '5']);
    expect(result1.kind).toBe('array');
    if (result1.kind === 'array') {
      const cursor1 = result1.value[0];
      expect(cursor1).not.toEqual(bulk('0')); // not done yet
      const items1 = result1.value[1];
      if (items1?.kind === 'array') {
        // Each field-value pair = 2 entries, so count=5 → up to 10 entries
        expect(items1.value.length).toBeLessThanOrEqual(10);
        expect(items1.value.length).toBeGreaterThan(0);
      }
    }
  });

  it('full iteration returns all fields', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1', 'f2', 'v2', 'f3', 'v3']);

    const allFields: string[] = [];
    let cursor = '0';
    let iterations = 0;
    do {
      const result = hash.hscan(db, ['k', cursor, 'COUNT', '1']);
      if (result.kind !== 'array') break;
      const cursorReply = result.value[0];
      if (cursorReply?.kind === 'bulk') cursor = cursorReply.value ?? '0';
      const items = result.value[1];
      if (items?.kind === 'array') {
        for (let i = 0; i < items.value.length; i += 2) {
          const field = items.value[i];
          if (field?.kind === 'bulk' && field.value !== null) {
            allFields.push(field.value);
          }
        }
      }
      iterations++;
    } while (cursor !== '0' && iterations < 100);

    expect(allFields.sort()).toEqual(['f1', 'f2', 'f3']);
  });

  it('returns error for invalid cursor', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    expect(hash.hscan(db, ['k', 'abc'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'invalid cursor',
    });
  });

  it('returns syntax error for unknown option', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    expect(hash.hscan(db, ['k', '0', 'INVALID'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('returns error for non-integer COUNT', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    expect(hash.hscan(db, ['k', '0', 'COUNT', 'abc'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('MATCH is case-insensitive option name', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1', 'g1', 'v2']);
    const result = hash.hscan(db, ['k', '0', 'match', 'f*']);
    expect(result).toEqual(arr(bulk('0'), arr(bulk('f1'), bulk('v1'))));
  });

  it('handles cursor beyond hash size', () => {
    const { db } = createDb();
    hash.hset(db, ['k', 'f1', 'v1']);
    const result = hash.hscan(db, ['k', '100']);
    expect(result).toEqual(arr(bulk('0'), EMPTY_ARRAY));
  });
});
