import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import type { Database } from '../../database.ts';
import { bitfield, bitfieldRo } from './bitfield.ts';

function createDb(time = 1000): {
  db: Database;
  engine: RedisEngine;
  clock: () => number;
  setTime: (t: number) => void;
} {
  let now = time;
  const clock = () => now;
  const engine = new RedisEngine({ clock, rng: () => 0.5 });
  return {
    db: engine.db(0),
    engine,
    clock,
    setTime: (t: number) => {
      now = t;
    },
  };
}

// --- BITFIELD ---

describe('BITFIELD', () => {
  it('GET returns 0 for non-existent key', () => {
    const { db } = createDb();
    expect(bitfield(db, ['mykey', 'GET', 'u8', '0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 0 }],
    });
  });

  it('SET stores and returns old value', () => {
    const { db } = createDb();
    // SET u8 at offset 0 to value 200, old value should be 0
    const result = bitfield(db, ['mykey', 'SET', 'u8', '0', '200']);
    expect(result).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 0 }],
    });
    // GET should now return 200
    expect(bitfield(db, ['mykey', 'GET', 'u8', '0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 200 }],
    });
  });

  it('INCRBY increments and returns new value', () => {
    const { db } = createDb();
    bitfield(db, ['mykey', 'SET', 'u8', '0', '100']);
    const result = bitfield(db, ['mykey', 'INCRBY', 'u8', '0', '50']);
    expect(result).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 150 }],
    });
  });

  it('supports signed types', () => {
    const { db } = createDb();
    bitfield(db, ['mykey', 'SET', 'i8', '0', '-10']);
    expect(bitfield(db, ['mykey', 'GET', 'i8', '0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: -10 }],
    });
  });

  it('supports hash offset (#N)', () => {
    const { db } = createDb();
    // #1 for u8 = offset 8
    bitfield(db, ['mykey', 'SET', 'u8', '#0', '10']);
    bitfield(db, ['mykey', 'SET', 'u8', '#1', '20']);
    expect(bitfield(db, ['mykey', 'GET', 'u8', '#0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 10 }],
    });
    expect(bitfield(db, ['mykey', 'GET', 'u8', '#1'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 20 }],
    });
  });

  it('OVERFLOW WRAP wraps unsigned values', () => {
    const { db } = createDb();
    bitfield(db, ['mykey', 'SET', 'u8', '0', '200']);
    const result = bitfield(db, [
      'mykey',
      'OVERFLOW',
      'WRAP',
      'INCRBY',
      'u8',
      '0',
      '100',
    ]);
    // 200 + 100 = 300, wrap to 300 % 256 = 44
    expect(result).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 44 }],
    });
  });

  it('OVERFLOW SAT saturates unsigned values', () => {
    const { db } = createDb();
    bitfield(db, ['mykey', 'SET', 'u8', '0', '200']);
    const result = bitfield(db, [
      'mykey',
      'OVERFLOW',
      'SAT',
      'INCRBY',
      'u8',
      '0',
      '100',
    ]);
    // 200 + 100 = 300, saturate to 255
    expect(result).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 255 }],
    });
  });

  it('OVERFLOW SAT saturates unsigned to 0 on underflow', () => {
    const { db } = createDb();
    bitfield(db, ['mykey', 'SET', 'u8', '0', '10']);
    const result = bitfield(db, [
      'mykey',
      'OVERFLOW',
      'SAT',
      'INCRBY',
      'u8',
      '0',
      '-20',
    ]);
    // 10 - 20 = -10, saturate to 0
    expect(result).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 0 }],
    });
  });

  it('OVERFLOW FAIL returns nil on overflow', () => {
    const { db } = createDb();
    bitfield(db, ['mykey', 'SET', 'u8', '0', '200']);
    const result = bitfield(db, [
      'mykey',
      'OVERFLOW',
      'FAIL',
      'INCRBY',
      'u8',
      '0',
      '100',
    ]);
    // 200 + 100 = 300, overflow -> nil
    expect(result).toEqual({
      kind: 'array',
      value: [{ kind: 'bulk', value: null }],
    });
    // Value should remain unchanged
    expect(bitfield(db, ['mykey', 'GET', 'u8', '0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 200 }],
    });
  });

  it('supports multiple operations in one call', () => {
    const { db } = createDb();
    const result = bitfield(db, [
      'mykey',
      'SET',
      'u8',
      '0',
      '100',
      'GET',
      'u8',
      '0',
      'INCRBY',
      'u8',
      '0',
      '10',
    ]);
    expect(result).toEqual({
      kind: 'array',
      value: [
        { kind: 'integer', value: 0 }, // SET returns old value (0)
        { kind: 'integer', value: 100 }, // GET returns current value
        { kind: 'integer', value: 110 }, // INCRBY returns new value
      ],
    });
  });

  it('returns error for invalid type', () => {
    const { db } = createDb();
    expect(bitfield(db, ['mykey', 'GET', 'x8', '0'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.',
    });
  });

  it('returns error for u64 type', () => {
    const { db } = createDb();
    expect(bitfield(db, ['mykey', 'GET', 'u64', '0'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.',
    });
  });

  it('allows i64 type', () => {
    const { db } = createDb();
    const result = bitfield(db, ['mykey', 'GET', 'i64', '0']);
    expect(result).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 0 }],
    });
  });

  it('returns WRONGTYPE for non-string key', () => {
    const { db } = createDb();
    db.set('mylist', 'list', 'quicklist', ['a']);
    expect(bitfield(db, ['mylist', 'GET', 'u8', '0'])).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('OVERFLOW mode applies only to subsequent operations', () => {
    const { db } = createDb();
    bitfield(db, ['mykey', 'SET', 'u8', '0', '200']);
    const result = bitfield(db, [
      'mykey',
      'INCRBY',
      'u8',
      '0',
      '100', // default WRAP -> 44
      'OVERFLOW',
      'SAT',
      'INCRBY',
      'u8',
      '0',
      '300', // SAT -> 255
    ]);
    expect(result).toEqual({
      kind: 'array',
      value: [
        { kind: 'integer', value: 44 }, // WRAP: 200+100=300 -> 44
        { kind: 'integer', value: 255 }, // SAT: 44+300=344 -> 255
      ],
    });
  });

  it('signed OVERFLOW WRAP wraps correctly', () => {
    const { db } = createDb();
    bitfield(db, ['mykey', 'SET', 'i8', '0', '120']);
    const result = bitfield(db, [
      'mykey',
      'OVERFLOW',
      'WRAP',
      'INCRBY',
      'i8',
      '0',
      '20',
    ]);
    // 120 + 20 = 140, i8 range is -128..127, wraps to 140-256 = -116
    expect(result).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: -116 }],
    });
  });

  it('signed OVERFLOW SAT saturates correctly', () => {
    const { db } = createDb();
    bitfield(db, ['mykey', 'SET', 'i8', '0', '120']);
    const result = bitfield(db, [
      'mykey',
      'OVERFLOW',
      'SAT',
      'INCRBY',
      'i8',
      '0',
      '20',
    ]);
    // 120 + 20 = 140 > 127, saturate to 127
    expect(result).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 127 }],
    });
  });

  it('returns empty array when no subcommands given', () => {
    const { db } = createDb();
    expect(bitfield(db, ['mykey'])).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('returns error for unknown subcommand', () => {
    const { db } = createDb();
    expect(bitfield(db, ['mykey', 'INVALID', 'u8', '0'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('SET with OVERFLOW FAIL rejects out-of-range values', () => {
    const { db } = createDb();
    const result = bitfield(db, [
      'mykey',
      'OVERFLOW',
      'FAIL',
      'SET',
      'u8',
      '0',
      '256',
    ]);
    expect(result).toEqual({
      kind: 'array',
      value: [{ kind: 'bulk', value: null }],
    });
  });

  it('SET with OVERFLOW WRAP wraps out-of-range values', () => {
    const { db } = createDb();
    const result = bitfield(db, [
      'mykey',
      'OVERFLOW',
      'WRAP',
      'SET',
      'u8',
      '0',
      '256',
    ]);
    // 256 % 256 = 0
    expect(result).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 0 }],
    });
    expect(bitfield(db, ['mykey', 'GET', 'u8', '0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 0 }],
    });
  });

  it('SET with OVERFLOW SAT clamps out-of-range values', () => {
    const { db } = createDb();
    const result = bitfield(db, [
      'mykey',
      'OVERFLOW',
      'SAT',
      'SET',
      'u8',
      '0',
      '300',
    ]);
    // Old value was 0, SET clamps to 255
    expect(result).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 0 }],
    });
    expect(bitfield(db, ['mykey', 'GET', 'u8', '0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 255 }],
    });
  });

  it('handles u1 type correctly', () => {
    const { db } = createDb();
    bitfield(db, ['mykey', 'SET', 'u1', '0', '1']);
    expect(bitfield(db, ['mykey', 'GET', 'u1', '0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 1 }],
    });
  });

  it('handles non-byte-aligned offset', () => {
    const { db } = createDb();
    // Set u8 at bit offset 4
    bitfield(db, ['mykey', 'SET', 'u8', '4', '255']);
    // Read u4 at offset 0 (first 4 bits) - should be 0x0F = 15
    // Actually, bit 4-11 are set to 11111111
    // bits 0-3 = 0000, bits 4-7 = 1111, bits 8-11 = 1111
    expect(bitfield(db, ['mykey', 'GET', 'u4', '0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 0 }],
    });
    expect(bitfield(db, ['mykey', 'GET', 'u4', '4'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 15 }],
    });
    expect(bitfield(db, ['mykey', 'GET', 'u4', '8'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 15 }],
    });
  });
});

// --- BITFIELD_RO ---

describe('BITFIELD_RO', () => {
  it('GET works like BITFIELD GET', () => {
    const { db } = createDb();
    bitfield(db, ['mykey', 'SET', 'u8', '0', '42']);
    expect(bitfieldRo(db, ['mykey', 'GET', 'u8', '0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 42 }],
    });
  });

  it('rejects SET subcommand', () => {
    const { db } = createDb();
    expect(bitfieldRo(db, ['mykey', 'SET', 'u8', '0', '42'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'BITFIELD_RO only supports the GET subcommand',
    });
  });

  it('rejects INCRBY subcommand', () => {
    const { db } = createDb();
    expect(bitfieldRo(db, ['mykey', 'INCRBY', 'u8', '0', '1'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'BITFIELD_RO only supports the GET subcommand',
    });
  });

  it('rejects OVERFLOW subcommand', () => {
    const { db } = createDb();
    expect(bitfieldRo(db, ['mykey', 'OVERFLOW', 'WRAP'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'BITFIELD_RO only supports the GET subcommand',
    });
  });

  it('returns 0 for non-existent key', () => {
    const { db } = createDb();
    expect(bitfieldRo(db, ['nokey', 'GET', 'u8', '0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 0 }],
    });
  });

  it('returns WRONGTYPE for non-string key', () => {
    const { db } = createDb();
    db.set('mylist', 'list', 'quicklist', ['a']);
    expect(bitfieldRo(db, ['mylist', 'GET', 'u8', '0'])).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('supports multiple GET operations', () => {
    const { db } = createDb();
    bitfield(db, ['mykey', 'SET', 'u8', '#0', '10', 'SET', 'u8', '#1', '20']);
    expect(
      bitfieldRo(db, ['mykey', 'GET', 'u8', '#0', 'GET', 'u8', '#1'])
    ).toEqual({
      kind: 'array',
      value: [
        { kind: 'integer', value: 10 },
        { kind: 'integer', value: 20 },
      ],
    });
  });

  it('returns empty array when no subcommands given', () => {
    const { db } = createDb();
    expect(bitfieldRo(db, ['mykey'])).toEqual({
      kind: 'array',
      value: [],
    });
  });
});
