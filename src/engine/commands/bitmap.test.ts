import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Database } from '../database.ts';
import * as bitmap from './bitmap.ts';
import * as str from './string.ts';

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

// --- SETBIT ---

describe('SETBIT', () => {
  it('sets a bit on a new key and returns 0 (old value)', () => {
    const { db } = createDb();
    expect(bitmap.setbit(db, ['mykey', '7', '1'])).toEqual({
      kind: 'integer',
      value: 0,
    });
  });

  it('returns the old bit value when overwriting', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['mykey', '7', '1']);
    expect(bitmap.setbit(db, ['mykey', '7', '0'])).toEqual({
      kind: 'integer',
      value: 1,
    });
  });

  it('auto-extends string with zero bytes', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['mykey', '23', '1']);
    // Byte 2 should have LSB set (bit 23 = byte 2, bit 7 within byte)
    expect(bitmap.getbit(db, ['mykey', '23'])).toEqual({
      kind: 'integer',
      value: 1,
    });
    // Earlier bits should be 0
    expect(bitmap.getbit(db, ['mykey', '0'])).toEqual({
      kind: 'integer',
      value: 0,
    });
  });

  it('sets MSB correctly (bit 0 = MSB of byte 0)', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['mykey', '0', '1']);
    // Byte 0 should be 0x80 = 128
    // Verify via getbit
    expect(bitmap.getbit(db, ['mykey', '0'])).toEqual({
      kind: 'integer',
      value: 1,
    });
    expect(bitmap.getbit(db, ['mykey', '7'])).toEqual({
      kind: 'integer',
      value: 0,
    });
  });

  it('returns WRONGTYPE for non-string key', () => {
    const { db } = createDb();
    db.set('mylist', 'list', 'quicklist', ['a']);
    expect(bitmap.setbit(db, ['mylist', '0', '1'])).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns error for invalid offset', () => {
    const { db } = createDb();
    expect(bitmap.setbit(db, ['mykey', 'abc', '1'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'bit offset is not an integer or out of range',
    });
  });

  it('returns error for negative offset', () => {
    const { db } = createDb();
    expect(bitmap.setbit(db, ['mykey', '-1', '1'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'bit offset is not an integer or out of range',
    });
  });

  it('returns error for invalid bit value', () => {
    const { db } = createDb();
    expect(bitmap.setbit(db, ['mykey', '0', '2'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'bit is not an integer or out of range',
    });
  });

  it('returns error for non-numeric bit value', () => {
    const { db } = createDb();
    expect(bitmap.setbit(db, ['mykey', '0', 'abc'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'bit is not an integer or out of range',
    });
  });

  it('works on existing string value', () => {
    const { db } = createDb();
    // 'a' = 0x61 = 01100001
    str.set(db, () => 1000, ['mykey', 'a']);
    // bit 0 of 'a' = 0 (MSB)
    expect(bitmap.setbit(db, ['mykey', '0', '1'])).toEqual({
      kind: 'integer',
      value: 0,
    });
    // now byte 0 = 0xE1 = 11100001
    expect(bitmap.getbit(db, ['mykey', '0'])).toEqual({
      kind: 'integer',
      value: 1,
    });
  });
});

// --- GETBIT ---

describe('GETBIT', () => {
  it('returns 0 for non-existent key', () => {
    const { db } = createDb();
    expect(bitmap.getbit(db, ['nokey', '0'])).toEqual({
      kind: 'integer',
      value: 0,
    });
  });

  it('returns 0 for offset beyond string length', () => {
    const { db } = createDb();
    str.set(db, () => 1000, ['mykey', 'a']);
    expect(bitmap.getbit(db, ['mykey', '100'])).toEqual({
      kind: 'integer',
      value: 0,
    });
  });

  it('reads bits of ASCII character correctly', () => {
    const { db } = createDb();
    // 'a' = 0x61 = 01100001
    str.set(db, () => 1000, ['mykey', 'a']);
    expect(bitmap.getbit(db, ['mykey', '0'])).toEqual({
      kind: 'integer',
      value: 0,
    });
    expect(bitmap.getbit(db, ['mykey', '1'])).toEqual({
      kind: 'integer',
      value: 1,
    });
    expect(bitmap.getbit(db, ['mykey', '2'])).toEqual({
      kind: 'integer',
      value: 1,
    });
    expect(bitmap.getbit(db, ['mykey', '7'])).toEqual({
      kind: 'integer',
      value: 1,
    });
  });

  it('returns WRONGTYPE for non-string key', () => {
    const { db } = createDb();
    db.set('mylist', 'list', 'quicklist', ['a']);
    expect(bitmap.getbit(db, ['mylist', '0'])).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns error for invalid offset', () => {
    const { db } = createDb();
    expect(bitmap.getbit(db, ['mykey', 'abc'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'bit offset is not an integer or out of range',
    });
  });

  it('returns error for negative offset', () => {
    const { db } = createDb();
    expect(bitmap.getbit(db, ['mykey', '-1'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'bit offset is not an integer or out of range',
    });
  });
});

// --- BITCOUNT ---

describe('BITCOUNT', () => {
  it('returns 0 for non-existent key', () => {
    const { db } = createDb();
    expect(bitmap.bitcount(db, ['nokey'])).toEqual({
      kind: 'integer',
      value: 0,
    });
  });

  it('counts all bits in a string', () => {
    const { db } = createDb();
    // Set some bits: bit 0, 7, 8, 15
    bitmap.setbit(db, ['mykey', '0', '1']);
    bitmap.setbit(db, ['mykey', '7', '1']);
    bitmap.setbit(db, ['mykey', '8', '1']);
    bitmap.setbit(db, ['mykey', '15', '1']);
    expect(bitmap.bitcount(db, ['mykey'])).toEqual({
      kind: 'integer',
      value: 4,
    });
  });

  it('counts bits with byte range', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['mykey', '0', '1']); // byte 0
    bitmap.setbit(db, ['mykey', '8', '1']); // byte 1
    bitmap.setbit(db, ['mykey', '16', '1']); // byte 2
    // Count only byte 1
    expect(bitmap.bitcount(db, ['mykey', '1', '1'])).toEqual({
      kind: 'integer',
      value: 1,
    });
  });

  it('supports negative byte range', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['mykey', '0', '1']); // byte 0
    bitmap.setbit(db, ['mykey', '8', '1']); // byte 1
    bitmap.setbit(db, ['mykey', '16', '1']); // byte 2
    // -1 = last byte (byte 2)
    expect(bitmap.bitcount(db, ['mykey', '-1', '-1'])).toEqual({
      kind: 'integer',
      value: 1,
    });
  });

  it('supports BIT range', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['mykey', '0', '1']);
    bitmap.setbit(db, ['mykey', '1', '1']);
    bitmap.setbit(db, ['mykey', '2', '1']);
    bitmap.setbit(db, ['mykey', '8', '1']);
    // Count bits 0-2 only
    expect(bitmap.bitcount(db, ['mykey', '0', '2', 'BIT'])).toEqual({
      kind: 'integer',
      value: 3,
    });
  });

  it('supports BYTE keyword explicitly', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['mykey', '0', '1']); // byte 0
    bitmap.setbit(db, ['mykey', '8', '1']); // byte 1
    expect(bitmap.bitcount(db, ['mykey', '0', '0', 'BYTE'])).toEqual({
      kind: 'integer',
      value: 1,
    });
  });

  it('returns 0 for empty range', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['mykey', '0', '1']);
    // start > end in byte mode
    expect(bitmap.bitcount(db, ['mykey', '5', '3'])).toEqual({
      kind: 'integer',
      value: 0,
    });
  });

  it('returns WRONGTYPE for non-string key', () => {
    const { db } = createDb();
    db.set('mylist', 'list', 'quicklist', ['a']);
    expect(bitmap.bitcount(db, ['mylist'])).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns error for invalid range arguments', () => {
    const { db } = createDb();
    str.set(db, () => 1000, ['mykey', 'a']);
    expect(bitmap.bitcount(db, ['mykey', 'abc', '1'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns error for wrong number of arguments with one range arg', () => {
    const { db } = createDb();
    str.set(db, () => 1000, ['mykey', 'a']);
    expect(bitmap.bitcount(db, ['mykey', '0'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('counts bits in foobar string', () => {
    const { db } = createDb();
    // "foobar" → f=01100110, o=01101111, o=01101111, b=01100010, a=01100001, r=01110010
    // f: 4 bits, o: 6, o: 6, b: 3, a: 3, r: 4 → total: 26
    str.set(db, () => 1000, ['mykey', 'foobar']);
    expect(bitmap.bitcount(db, ['mykey'])).toEqual({
      kind: 'integer',
      value: 26,
    });
  });

  it('counts bits in foobar with byte range 0 0', () => {
    const { db } = createDb();
    str.set(db, () => 1000, ['mykey', 'foobar']);
    // byte 0 = 'f' = 01100110 = 4 bits
    expect(bitmap.bitcount(db, ['mykey', '0', '0'])).toEqual({
      kind: 'integer',
      value: 4,
    });
  });

  it('counts bits in foobar with byte range 1 1', () => {
    const { db } = createDb();
    str.set(db, () => 1000, ['mykey', 'foobar']);
    // byte 1 = 'o' = 01101111 = 6 bits
    expect(bitmap.bitcount(db, ['mykey', '1', '1'])).toEqual({
      kind: 'integer',
      value: 6,
    });
  });
});

// --- BITPOS ---

describe('BITPOS', () => {
  it('returns -1 for bit=1 on non-existent key', () => {
    const { db } = createDb();
    expect(bitmap.bitpos(db, ['nokey', '1'])).toEqual({
      kind: 'integer',
      value: -1,
    });
  });

  it('returns 0 for bit=0 on non-existent key', () => {
    const { db } = createDb();
    expect(bitmap.bitpos(db, ['nokey', '0'])).toEqual({
      kind: 'integer',
      value: 0,
    });
  });

  it('finds first set bit', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['mykey', '5', '1']);
    expect(bitmap.bitpos(db, ['mykey', '1'])).toEqual({
      kind: 'integer',
      value: 5,
    });
  });

  it('finds first clear bit', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['mykey', '0', '1']);
    bitmap.setbit(db, ['mykey', '1', '1']);
    bitmap.setbit(db, ['mykey', '2', '1']);
    // first 0 bit is at position 3
    expect(bitmap.bitpos(db, ['mykey', '0'])).toEqual({
      kind: 'integer',
      value: 3,
    });
  });

  it('returns past-end position for bit=0 in all-ones string (no end given)', () => {
    const { db } = createDb();
    // Create a 1-byte string with all bits set
    bitmap.setbit(db, ['mykey', '0', '1']);
    bitmap.setbit(db, ['mykey', '1', '1']);
    bitmap.setbit(db, ['mykey', '2', '1']);
    bitmap.setbit(db, ['mykey', '3', '1']);
    bitmap.setbit(db, ['mykey', '4', '1']);
    bitmap.setbit(db, ['mykey', '5', '1']);
    bitmap.setbit(db, ['mykey', '6', '1']);
    bitmap.setbit(db, ['mykey', '7', '1']);
    // No end given → returns 8 (past end of 1-byte string)
    expect(bitmap.bitpos(db, ['mykey', '0'])).toEqual({
      kind: 'integer',
      value: 8,
    });
  });

  it('returns -1 for bit=0 in all-ones string (end given)', () => {
    const { db } = createDb();
    // Create 1-byte all-ones string
    for (let i = 0; i < 8; i++) {
      bitmap.setbit(db, ['mykey', String(i), '1']);
    }
    expect(bitmap.bitpos(db, ['mykey', '0', '0', '-1'])).toEqual({
      kind: 'integer',
      value: -1,
    });
  });

  it('searches from start byte', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['mykey', '0', '1']); // byte 0
    bitmap.setbit(db, ['mykey', '12', '1']); // byte 1
    // Start from byte 1
    expect(bitmap.bitpos(db, ['mykey', '1', '1'])).toEqual({
      kind: 'integer',
      value: 12,
    });
  });

  it('searches byte range', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['mykey', '0', '1']); // byte 0
    bitmap.setbit(db, ['mykey', '16', '1']); // byte 2
    // Search bytes 1-1, no set bit there
    expect(bitmap.bitpos(db, ['mykey', '1', '1', '1'])).toEqual({
      kind: 'integer',
      value: -1,
    });
  });

  it('supports BIT range', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['mykey', '3', '1']);
    bitmap.setbit(db, ['mykey', '10', '1']);
    // Search bits 5-15 for first 1
    expect(bitmap.bitpos(db, ['mykey', '1', '5', '15', 'BIT'])).toEqual({
      kind: 'integer',
      value: 10,
    });
  });

  it('returns error for invalid bit argument', () => {
    const { db } = createDb();
    expect(bitmap.bitpos(db, ['mykey', '2'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'The bit argument must be 1 or 0.',
    });
  });

  it('returns WRONGTYPE for non-string key', () => {
    const { db } = createDb();
    db.set('mylist', 'list', 'quicklist', ['a']);
    expect(bitmap.bitpos(db, ['mylist', '1'])).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns -1 for bit=1 in all-zeros string', () => {
    const { db } = createDb();
    // Create a 2-byte all-zeros string via setbit then clear
    bitmap.setbit(db, ['mykey', '15', '0']);
    expect(bitmap.bitpos(db, ['mykey', '1'])).toEqual({
      kind: 'integer',
      value: -1,
    });
  });

  it('handles negative start for byte range', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['mykey', '0', '1']); // byte 0
    bitmap.setbit(db, ['mykey', '16', '1']); // byte 2
    // Start from last byte (byte 2), find first 1
    expect(bitmap.bitpos(db, ['mykey', '1', '-1'])).toEqual({
      kind: 'integer',
      value: 16,
    });
  });
});

// --- BITOP ---

describe('BITOP', () => {
  it('performs AND operation', () => {
    const { db } = createDb();
    // key1: bit 0, 1 set → byte 0 = 11000000 = 0xC0
    bitmap.setbit(db, ['key1', '0', '1']);
    bitmap.setbit(db, ['key1', '1', '1']);
    // key2: bit 0, 2 set → byte 0 = 10100000 = 0xA0
    bitmap.setbit(db, ['key2', '0', '1']);
    bitmap.setbit(db, ['key2', '2', '1']);
    // AND → 10000000 = bit 0 only
    const result = bitmap.bitop(db, ['AND', 'dest', 'key1', 'key2']);
    expect(result).toEqual({ kind: 'integer', value: 1 });
    expect(bitmap.getbit(db, ['dest', '0'])).toEqual({
      kind: 'integer',
      value: 1,
    });
    expect(bitmap.getbit(db, ['dest', '1'])).toEqual({
      kind: 'integer',
      value: 0,
    });
    expect(bitmap.getbit(db, ['dest', '2'])).toEqual({
      kind: 'integer',
      value: 0,
    });
  });

  it('performs OR operation', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['key1', '0', '1']);
    bitmap.setbit(db, ['key1', '1', '1']);
    bitmap.setbit(db, ['key2', '0', '1']);
    bitmap.setbit(db, ['key2', '2', '1']);
    // OR → 11100000
    const result = bitmap.bitop(db, ['OR', 'dest', 'key1', 'key2']);
    expect(result).toEqual({ kind: 'integer', value: 1 });
    expect(bitmap.getbit(db, ['dest', '0'])).toEqual({
      kind: 'integer',
      value: 1,
    });
    expect(bitmap.getbit(db, ['dest', '1'])).toEqual({
      kind: 'integer',
      value: 1,
    });
    expect(bitmap.getbit(db, ['dest', '2'])).toEqual({
      kind: 'integer',
      value: 1,
    });
  });

  it('performs XOR operation', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['key1', '0', '1']);
    bitmap.setbit(db, ['key1', '1', '1']);
    bitmap.setbit(db, ['key2', '0', '1']);
    bitmap.setbit(db, ['key2', '2', '1']);
    // XOR → 01100000 (bit 1, 2)
    const result = bitmap.bitop(db, ['XOR', 'dest', 'key1', 'key2']);
    expect(result).toEqual({ kind: 'integer', value: 1 });
    expect(bitmap.getbit(db, ['dest', '0'])).toEqual({
      kind: 'integer',
      value: 0,
    });
    expect(bitmap.getbit(db, ['dest', '1'])).toEqual({
      kind: 'integer',
      value: 1,
    });
    expect(bitmap.getbit(db, ['dest', '2'])).toEqual({
      kind: 'integer',
      value: 1,
    });
  });

  it('performs NOT operation', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['key1', '0', '1']);
    bitmap.setbit(db, ['key1', '7', '1']);
    // byte 0 = 10000001, NOT = 01111110
    const result = bitmap.bitop(db, ['NOT', 'dest', 'key1']);
    expect(result).toEqual({ kind: 'integer', value: 1 });
    expect(bitmap.getbit(db, ['dest', '0'])).toEqual({
      kind: 'integer',
      value: 0,
    });
    expect(bitmap.getbit(db, ['dest', '1'])).toEqual({
      kind: 'integer',
      value: 1,
    });
    expect(bitmap.getbit(db, ['dest', '7'])).toEqual({
      kind: 'integer',
      value: 0,
    });
  });

  it('zero-pads shorter strings in AND/OR/XOR', () => {
    const { db } = createDb();
    // key1: 2 bytes (bits 0 and 15)
    bitmap.setbit(db, ['key1', '0', '1']);
    bitmap.setbit(db, ['key1', '15', '1']);
    // key2: 1 byte (bit 0)
    bitmap.setbit(db, ['key2', '0', '1']);
    // OR: result should be 2 bytes
    const result = bitmap.bitop(db, ['OR', 'dest', 'key1', 'key2']);
    expect(result).toEqual({ kind: 'integer', value: 2 });
    expect(bitmap.getbit(db, ['dest', '15'])).toEqual({
      kind: 'integer',
      value: 1,
    });
  });

  it('returns error for NOT with multiple keys', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['key1', '0', '1']);
    bitmap.setbit(db, ['key2', '0', '1']);
    expect(bitmap.bitop(db, ['NOT', 'dest', 'key1', 'key2'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'BITOP NOT requires one and only one key.',
    });
  });

  it('returns error for unknown operation', () => {
    const { db } = createDb();
    expect(bitmap.bitop(db, ['NAND', 'dest', 'key1'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('returns WRONGTYPE for non-string source key', () => {
    const { db } = createDb();
    db.set('mylist', 'list', 'quicklist', ['a']);
    expect(bitmap.bitop(db, ['AND', 'dest', 'mylist'])).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('handles non-existent source keys as empty strings', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['key1', '0', '1']);
    // AND with non-existent key → all zeros
    bitmap.bitop(db, ['AND', 'dest', 'key1', 'nokey']);
    expect(bitmap.getbit(db, ['dest', '0'])).toEqual({
      kind: 'integer',
      value: 0,
    });
  });

  it('deletes dest key when all sources are empty', () => {
    const { db } = createDb();
    // Pre-set dest to something
    bitmap.setbit(db, ['dest', '0', '1']);
    // AND with non-existent keys
    bitmap.bitop(db, ['AND', 'dest', 'nokey1', 'nokey2']);
    expect(db.has('dest')).toBe(false);
  });

  it('handles case-insensitive operation name', () => {
    const { db } = createDb();
    bitmap.setbit(db, ['key1', '0', '1']);
    const result = bitmap.bitop(db, ['and', 'dest', 'key1']);
    expect(result).toEqual({ kind: 'integer', value: 1 });
  });
});

// --- BITFIELD ---

describe('BITFIELD', () => {
  it('GET returns 0 for non-existent key', () => {
    const { db } = createDb();
    expect(bitmap.bitfield(db, ['mykey', 'GET', 'u8', '0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 0 }],
    });
  });

  it('SET stores and returns old value', () => {
    const { db } = createDb();
    // SET u8 at offset 0 to value 200, old value should be 0
    const result = bitmap.bitfield(db, ['mykey', 'SET', 'u8', '0', '200']);
    expect(result).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 0 }],
    });
    // GET should now return 200
    expect(bitmap.bitfield(db, ['mykey', 'GET', 'u8', '0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 200 }],
    });
  });

  it('INCRBY increments and returns new value', () => {
    const { db } = createDb();
    bitmap.bitfield(db, ['mykey', 'SET', 'u8', '0', '100']);
    const result = bitmap.bitfield(db, ['mykey', 'INCRBY', 'u8', '0', '50']);
    expect(result).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 150 }],
    });
  });

  it('supports signed types', () => {
    const { db } = createDb();
    bitmap.bitfield(db, ['mykey', 'SET', 'i8', '0', '-10']);
    expect(bitmap.bitfield(db, ['mykey', 'GET', 'i8', '0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: -10 }],
    });
  });

  it('supports hash offset (#N)', () => {
    const { db } = createDb();
    // #1 for u8 = offset 8
    bitmap.bitfield(db, ['mykey', 'SET', 'u8', '#0', '10']);
    bitmap.bitfield(db, ['mykey', 'SET', 'u8', '#1', '20']);
    expect(bitmap.bitfield(db, ['mykey', 'GET', 'u8', '#0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 10 }],
    });
    expect(bitmap.bitfield(db, ['mykey', 'GET', 'u8', '#1'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 20 }],
    });
  });

  it('OVERFLOW WRAP wraps unsigned values', () => {
    const { db } = createDb();
    bitmap.bitfield(db, ['mykey', 'SET', 'u8', '0', '200']);
    const result = bitmap.bitfield(db, [
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
    bitmap.bitfield(db, ['mykey', 'SET', 'u8', '0', '200']);
    const result = bitmap.bitfield(db, [
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
    bitmap.bitfield(db, ['mykey', 'SET', 'u8', '0', '10']);
    const result = bitmap.bitfield(db, [
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
    bitmap.bitfield(db, ['mykey', 'SET', 'u8', '0', '200']);
    const result = bitmap.bitfield(db, [
      'mykey',
      'OVERFLOW',
      'FAIL',
      'INCRBY',
      'u8',
      '0',
      '100',
    ]);
    // 200 + 100 = 300, overflow → nil
    expect(result).toEqual({
      kind: 'array',
      value: [{ kind: 'bulk', value: null }],
    });
    // Value should remain unchanged
    expect(bitmap.bitfield(db, ['mykey', 'GET', 'u8', '0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 200 }],
    });
  });

  it('supports multiple operations in one call', () => {
    const { db } = createDb();
    const result = bitmap.bitfield(db, [
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
    expect(bitmap.bitfield(db, ['mykey', 'GET', 'x8', '0'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.',
    });
  });

  it('returns error for u64 type', () => {
    const { db } = createDb();
    expect(bitmap.bitfield(db, ['mykey', 'GET', 'u64', '0'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.',
    });
  });

  it('allows i64 type', () => {
    const { db } = createDb();
    const result = bitmap.bitfield(db, ['mykey', 'GET', 'i64', '0']);
    expect(result).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 0 }],
    });
  });

  it('returns WRONGTYPE for non-string key', () => {
    const { db } = createDb();
    db.set('mylist', 'list', 'quicklist', ['a']);
    expect(bitmap.bitfield(db, ['mylist', 'GET', 'u8', '0'])).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('OVERFLOW mode applies only to subsequent operations', () => {
    const { db } = createDb();
    bitmap.bitfield(db, ['mykey', 'SET', 'u8', '0', '200']);
    const result = bitmap.bitfield(db, [
      'mykey',
      'INCRBY',
      'u8',
      '0',
      '100', // default WRAP → 44
      'OVERFLOW',
      'SAT',
      'INCRBY',
      'u8',
      '0',
      '300', // SAT → 255
    ]);
    expect(result).toEqual({
      kind: 'array',
      value: [
        { kind: 'integer', value: 44 }, // WRAP: 200+100=300 → 44
        { kind: 'integer', value: 255 }, // SAT: 44+300=344 → 255
      ],
    });
  });

  it('signed OVERFLOW WRAP wraps correctly', () => {
    const { db } = createDb();
    bitmap.bitfield(db, ['mykey', 'SET', 'i8', '0', '120']);
    const result = bitmap.bitfield(db, [
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
    bitmap.bitfield(db, ['mykey', 'SET', 'i8', '0', '120']);
    const result = bitmap.bitfield(db, [
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
    expect(bitmap.bitfield(db, ['mykey'])).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('returns error for unknown subcommand', () => {
    const { db } = createDb();
    expect(bitmap.bitfield(db, ['mykey', 'INVALID', 'u8', '0'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "Unknown BITFIELD subcommand 'INVALID'",
    });
  });

  it('SET with OVERFLOW FAIL rejects out-of-range values', () => {
    const { db } = createDb();
    const result = bitmap.bitfield(db, [
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
    const result = bitmap.bitfield(db, [
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
    expect(bitmap.bitfield(db, ['mykey', 'GET', 'u8', '0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 0 }],
    });
  });

  it('SET with OVERFLOW SAT clamps out-of-range values', () => {
    const { db } = createDb();
    const result = bitmap.bitfield(db, [
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
    expect(bitmap.bitfield(db, ['mykey', 'GET', 'u8', '0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 255 }],
    });
  });

  it('handles u1 type correctly', () => {
    const { db } = createDb();
    bitmap.bitfield(db, ['mykey', 'SET', 'u1', '0', '1']);
    expect(bitmap.bitfield(db, ['mykey', 'GET', 'u1', '0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 1 }],
    });
  });

  it('handles non-byte-aligned offset', () => {
    const { db } = createDb();
    // Set u8 at bit offset 4
    bitmap.bitfield(db, ['mykey', 'SET', 'u8', '4', '255']);
    // Read u4 at offset 0 (first 4 bits) - should be 0x0F = 15
    // Actually, bit 4-11 are set to 11111111
    // bits 0-3 = 0000, bits 4-7 = 1111, bits 8-11 = 1111
    expect(bitmap.bitfield(db, ['mykey', 'GET', 'u4', '0'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 0 }],
    });
    expect(bitmap.bitfield(db, ['mykey', 'GET', 'u4', '4'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 15 }],
    });
    expect(bitmap.bitfield(db, ['mykey', 'GET', 'u4', '8'])).toEqual({
      kind: 'array',
      value: [{ kind: 'integer', value: 15 }],
    });
  });
});
