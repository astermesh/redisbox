import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import {
  stringToBytes,
  bytesToString,
  byteAt,
  getStringBytes,
  setStringFromBytes,
  parseBitOffset,
  parseIntStrict,
  getBit,
  POPCOUNT_TABLE,
  BIT_OFFSET_ERR,
  BIT_VALUE_ERR,
  BIT_ARG_ERR,
  BITOP_NOT_ERR,
} from './bytes.ts';

function createDb(): Database {
  const engine = new RedisEngine({ clock: () => 1000, rng: () => 0.5 });
  return engine.db(0);
}

const WRONGTYPE: Reply = {
  kind: 'error',
  prefix: 'WRONGTYPE',
  message: 'Operation against a key holding the wrong kind of value',
};

const NOT_INTEGER_ERR: Reply = {
  kind: 'error',
  prefix: 'ERR',
  message: 'value is not an integer or out of range',
};

// --- Error constants ---

describe('error constants', () => {
  it('BIT_OFFSET_ERR has correct shape', () => {
    expect(BIT_OFFSET_ERR).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'bit offset is not an integer or out of range',
    });
  });

  it('BIT_VALUE_ERR has correct shape', () => {
    expect(BIT_VALUE_ERR).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'bit is not an integer or out of range',
    });
  });

  it('BIT_ARG_ERR has correct shape', () => {
    expect(BIT_ARG_ERR).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'The bit argument must be 1 or 0.',
    });
  });

  it('BITOP_NOT_ERR has correct shape', () => {
    expect(BITOP_NOT_ERR).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'BITOP NOT must be called with a single source key.',
    });
  });
});

// --- stringToBytes ---

describe('stringToBytes', () => {
  it('converts empty string to empty Uint8Array', () => {
    expect(stringToBytes('')).toEqual(new Uint8Array([]));
  });

  it('converts ASCII string to bytes', () => {
    expect(stringToBytes('abc')).toEqual(new Uint8Array([97, 98, 99]));
  });

  it('converts null bytes', () => {
    expect(stringToBytes('\x00\x00')).toEqual(new Uint8Array([0, 0]));
  });

  it('converts high byte values (Latin-1 range)', () => {
    expect(stringToBytes('\xff\x80')).toEqual(new Uint8Array([255, 128]));
  });

  it('masks characters to low 8 bits', () => {
    // charCodeAt for characters > 255 should be masked to low byte
    const s = String.fromCharCode(0x1ff); // 511 -> 0xff after mask
    expect(stringToBytes(s)).toEqual(new Uint8Array([0xff]));
  });

  it('handles all byte values 0-255', () => {
    let s = '';
    const expected = new Uint8Array(256);
    for (let i = 0; i < 256; i++) {
      s += String.fromCharCode(i);
      expected[i] = i;
    }
    expect(stringToBytes(s)).toEqual(expected);
  });
});

// --- bytesToString ---

describe('bytesToString', () => {
  it('converts empty Uint8Array to empty string', () => {
    expect(bytesToString(new Uint8Array([]))).toBe('');
  });

  it('converts ASCII bytes to string', () => {
    expect(bytesToString(new Uint8Array([97, 98, 99]))).toBe('abc');
  });

  it('converts null bytes', () => {
    expect(bytesToString(new Uint8Array([0, 0]))).toBe('\x00\x00');
  });

  it('converts high byte values', () => {
    expect(bytesToString(new Uint8Array([255, 128]))).toBe('\xff\x80');
  });

  it('handles large arrays (triggers chunking)', () => {
    // bytesToString uses 8192 chunk size, test with > 8192 bytes
    const size = 10000;
    const bytes = new Uint8Array(size);
    for (let i = 0; i < size; i++) bytes[i] = i & 0xff;
    const result = bytesToString(bytes);
    expect(result.length).toBe(size);
    for (let i = 0; i < size; i++) {
      expect(result.charCodeAt(i)).toBe(i & 0xff);
    }
  });

  it('roundtrips with stringToBytes for all byte values', () => {
    const bytes = new Uint8Array(256);
    for (let i = 0; i < 256; i++) bytes[i] = i;
    expect(stringToBytes(bytesToString(bytes))).toEqual(bytes);
  });
});

// --- byteAt ---

describe('byteAt', () => {
  it('returns byte at valid index', () => {
    const bytes = new Uint8Array([10, 20, 30]);
    expect(byteAt(bytes, 0)).toBe(10);
    expect(byteAt(bytes, 1)).toBe(20);
    expect(byteAt(bytes, 2)).toBe(30);
  });

  it('returns 0 for out-of-bounds index', () => {
    const bytes = new Uint8Array([10, 20]);
    expect(byteAt(bytes, 2)).toBe(0);
    expect(byteAt(bytes, 100)).toBe(0);
  });

  it('returns 0 for empty array', () => {
    expect(byteAt(new Uint8Array([]), 0)).toBe(0);
  });
});

// --- getStringBytes ---

describe('getStringBytes', () => {
  it('returns null bytes and null error for non-existent key', () => {
    const db = createDb();
    expect(getStringBytes(db, 'nokey')).toEqual({
      bytes: null,
      error: null,
    });
  });

  it('returns bytes for existing string key', () => {
    const db = createDb();
    db.set('k', 'string', 'raw', 'abc');
    const result = getStringBytes(db, 'k');
    expect(result.error).toBeNull();
    expect(result.bytes).toEqual(new Uint8Array([97, 98, 99]));
  });

  it('returns WRONGTYPE error for non-string key', () => {
    const db = createDb();
    db.set('k', 'list', 'linkedlist', []);
    expect(getStringBytes(db, 'k')).toEqual({
      bytes: null,
      error: WRONGTYPE,
    });
  });

  it('returns bytes for empty string', () => {
    const db = createDb();
    db.set('k', 'string', 'raw', '');
    const result = getStringBytes(db, 'k');
    expect(result.error).toBeNull();
    expect(result.bytes).toEqual(new Uint8Array([]));
  });
});

// --- setStringFromBytes ---

describe('setStringFromBytes', () => {
  it('sets a string key from bytes', () => {
    const db = createDb();
    setStringFromBytes(db, 'k', new Uint8Array([104, 105]));
    const entry = db.get('k');
    expect(entry).not.toBeNull();
    expect(entry?.type).toBe('string');
    expect(entry?.value).toBe('hi');
  });

  it('sets empty string from empty bytes', () => {
    const db = createDb();
    setStringFromBytes(db, 'k', new Uint8Array([]));
    const entry = db.get('k');
    expect(entry?.value).toBe('');
  });

  it('overwrites existing key', () => {
    const db = createDb();
    db.set('k', 'string', 'raw', 'old');
    setStringFromBytes(db, 'k', new Uint8Array([110, 101, 119]));
    expect(db.get('k')?.value).toBe('new');
  });

  it('stores encoding as raw', () => {
    const db = createDb();
    setStringFromBytes(db, 'k', new Uint8Array([65]));
    expect(db.get('k')?.encoding).toBe('raw');
  });
});

// --- parseBitOffset ---

describe('parseBitOffset', () => {
  it('parses 0', () => {
    expect(parseBitOffset('0')).toEqual({ value: 0, error: null });
  });

  it('parses positive integer', () => {
    expect(parseBitOffset('100')).toEqual({ value: 100, error: null });
  });

  it('parses max offset (2^32 - 1)', () => {
    expect(parseBitOffset('4294967295')).toEqual({
      value: 4294967295,
      error: null,
    });
  });

  it('rejects negative offset', () => {
    expect(parseBitOffset('-1')).toEqual({ value: 0, error: BIT_OFFSET_ERR });
  });

  it('rejects offset above max', () => {
    expect(parseBitOffset('4294967296')).toEqual({
      value: 0,
      error: BIT_OFFSET_ERR,
    });
  });

  it('rejects non-integer string', () => {
    expect(parseBitOffset('abc')).toEqual({
      value: 0,
      error: BIT_OFFSET_ERR,
    });
  });

  it('rejects float', () => {
    expect(parseBitOffset('1.5')).toEqual({
      value: 0,
      error: BIT_OFFSET_ERR,
    });
  });

  it('accepts empty string as 0 (Number("") === 0)', () => {
    // Number('') is 0, which is a valid integer in range
    expect(parseBitOffset('')).toEqual({ value: 0, error: null });
  });
});

// --- parseIntStrict ---

describe('parseIntStrict', () => {
  it('parses zero', () => {
    expect(parseIntStrict('0')).toEqual({ value: 0, error: null });
  });

  it('parses positive integer', () => {
    expect(parseIntStrict('42')).toEqual({ value: 42, error: null });
  });

  it('parses negative integer', () => {
    expect(parseIntStrict('-7')).toEqual({ value: -7, error: null });
  });

  it('rejects non-numeric string', () => {
    expect(parseIntStrict('abc')).toEqual({
      value: 0,
      error: NOT_INTEGER_ERR,
    });
  });

  it('rejects float string', () => {
    expect(parseIntStrict('3.14')).toEqual({
      value: 0,
      error: NOT_INTEGER_ERR,
    });
  });

  it('rejects string with trailing characters', () => {
    expect(parseIntStrict('42abc')).toEqual({
      value: 0,
      error: NOT_INTEGER_ERR,
    });
  });

  it('rejects string with leading spaces', () => {
    expect(parseIntStrict(' 42')).toEqual({
      value: 0,
      error: NOT_INTEGER_ERR,
    });
  });

  it('rejects empty string', () => {
    expect(parseIntStrict('')).toEqual({ value: 0, error: NOT_INTEGER_ERR });
  });

  it('rejects leading zeros', () => {
    expect(parseIntStrict('007')).toEqual({
      value: 0,
      error: NOT_INTEGER_ERR,
    });
  });
});

// --- POPCOUNT_TABLE ---

describe('POPCOUNT_TABLE', () => {
  it('has 256 entries', () => {
    expect(POPCOUNT_TABLE.length).toBe(256);
  });

  it('POPCOUNT_TABLE[0] is 0', () => {
    expect(POPCOUNT_TABLE[0]).toBe(0);
  });

  it('POPCOUNT_TABLE[1] is 1', () => {
    expect(POPCOUNT_TABLE[1]).toBe(1);
  });

  it('POPCOUNT_TABLE[255] is 8', () => {
    expect(POPCOUNT_TABLE[255]).toBe(8);
  });

  it('POPCOUNT_TABLE[128] is 1 (0b10000000)', () => {
    expect(POPCOUNT_TABLE[128]).toBe(1);
  });

  it('POPCOUNT_TABLE[0x55] is 4 (0b01010101)', () => {
    expect(POPCOUNT_TABLE[0x55]).toBe(4);
  });

  it('POPCOUNT_TABLE[0xAA] is 4 (0b10101010)', () => {
    expect(POPCOUNT_TABLE[0xaa]).toBe(4);
  });

  it('all entries match manual popcount', () => {
    for (let i = 0; i < 256; i++) {
      let expected = 0;
      let n = i;
      while (n) {
        expected += n & 1;
        n >>= 1;
      }
      expect(POPCOUNT_TABLE[i]).toBe(expected);
    }
  });
});

// --- getBit ---

describe('getBit', () => {
  it('returns 0 for empty bytes at offset 0', () => {
    expect(getBit(new Uint8Array([]), 0)).toBe(0);
  });

  it('returns correct bits for 0xFF', () => {
    const bytes = new Uint8Array([0xff]);
    for (let i = 0; i < 8; i++) {
      expect(getBit(bytes, i)).toBe(1);
    }
  });

  it('returns correct bits for 0x00', () => {
    const bytes = new Uint8Array([0x00]);
    for (let i = 0; i < 8; i++) {
      expect(getBit(bytes, i)).toBe(0);
    }
  });

  it('returns correct bits for 0x80 (MSB set)', () => {
    const bytes = new Uint8Array([0x80]); // 10000000
    expect(getBit(bytes, 0)).toBe(1);
    expect(getBit(bytes, 1)).toBe(0);
    expect(getBit(bytes, 7)).toBe(0);
  });

  it('returns correct bits for 0x01 (LSB set)', () => {
    const bytes = new Uint8Array([0x01]); // 00000001
    expect(getBit(bytes, 0)).toBe(0);
    expect(getBit(bytes, 6)).toBe(0);
    expect(getBit(bytes, 7)).toBe(1);
  });

  it('reads bits across multiple bytes', () => {
    const bytes = new Uint8Array([0x80, 0x01]); // 10000000 00000001
    expect(getBit(bytes, 0)).toBe(1); // first bit of byte 0
    expect(getBit(bytes, 7)).toBe(0); // last bit of byte 0
    expect(getBit(bytes, 8)).toBe(0); // first bit of byte 1
    expect(getBit(bytes, 15)).toBe(1); // last bit of byte 1
  });

  it('returns 0 for out-of-bounds offset', () => {
    const bytes = new Uint8Array([0xff]);
    expect(getBit(bytes, 8)).toBe(0);
    expect(getBit(bytes, 100)).toBe(0);
  });

  it('handles alternating bit pattern 0xAA', () => {
    const bytes = new Uint8Array([0xaa]); // 10101010
    expect(getBit(bytes, 0)).toBe(1);
    expect(getBit(bytes, 1)).toBe(0);
    expect(getBit(bytes, 2)).toBe(1);
    expect(getBit(bytes, 3)).toBe(0);
    expect(getBit(bytes, 4)).toBe(1);
    expect(getBit(bytes, 5)).toBe(0);
    expect(getBit(bytes, 6)).toBe(1);
    expect(getBit(bytes, 7)).toBe(0);
  });
});
