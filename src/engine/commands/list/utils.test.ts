import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import {
  fitsListpack,
  getOrCreateList,
  getExistingList,
  updateEncoding,
  deleteIfEmpty,
  parseCount,
  parseInteger,
  resolveIndex,
} from './utils.ts';

function createDb(): { db: Database; engine: RedisEngine } {
  const engine = new RedisEngine({ clock: () => 1000, rng: () => 0.5 });
  return { db: engine.db(0), engine };
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

// --- fitsListpack ---

describe('fitsListpack', () => {
  it('returns true for empty array', () => {
    expect(fitsListpack([])).toBe(true);
  });

  it('returns true for small items within defaults', () => {
    expect(fitsListpack(['a', 'b', 'c'])).toBe(true);
  });

  it('returns false when item count exceeds maxEntries', () => {
    const items = Array.from({ length: 129 }, (_, i) => String(i));
    expect(fitsListpack(items)).toBe(false);
  });

  it('returns true when item count equals maxEntries', () => {
    const items = Array.from({ length: 128 }, (_, i) => String(i));
    expect(fitsListpack(items)).toBe(true);
  });

  it('returns false when any item byte length exceeds maxValue', () => {
    const longItem = 'a'.repeat(65); // 65 bytes > 64
    expect(fitsListpack([longItem])).toBe(false);
  });

  it('returns true when item byte length equals maxValue', () => {
    const item = 'a'.repeat(64);
    expect(fitsListpack([item])).toBe(true);
  });

  it('respects custom maxEntries', () => {
    expect(fitsListpack(['a', 'b', 'c'], 2)).toBe(false);
    expect(fitsListpack(['a', 'b'], 2)).toBe(true);
  });

  it('respects custom maxValue', () => {
    expect(fitsListpack(['abc'], 128, 2)).toBe(false);
    expect(fitsListpack(['ab'], 128, 2)).toBe(true);
  });

  it('counts multi-byte characters correctly', () => {
    // "é" is 2 bytes in UTF-8
    const item = 'é'.repeat(33); // 66 bytes > 64
    expect(fitsListpack([item])).toBe(false);

    const shorter = 'é'.repeat(32); // 64 bytes = 64
    expect(fitsListpack([shorter])).toBe(true);
  });

  it('returns false if only one item among many exceeds maxValue', () => {
    const items = ['a', 'b', 'a'.repeat(65), 'c'];
    expect(fitsListpack(items)).toBe(false);
  });
});

// --- getOrCreateList ---

describe('getOrCreateList', () => {
  it('creates a new list when key does not exist', () => {
    const { db } = createDb();
    const result = getOrCreateList(db, 'k');
    expect(result.error).toBeNull();
    expect(result.list).toEqual([]);
  });

  it('created list is stored in database', () => {
    const { db } = createDb();
    getOrCreateList(db, 'k');
    const entry = db.get('k');
    expect(entry).not.toBeNull();
    expect(entry?.type).toBe('list');
    expect(entry?.encoding).toBe('listpack');
    expect(entry?.value).toEqual([]);
  });

  it('returns existing list', () => {
    const { db } = createDb();
    db.set('k', 'list', 'listpack', ['a', 'b']);
    const result = getOrCreateList(db, 'k');
    expect(result.error).toBeNull();
    expect(result.list).toEqual(['a', 'b']);
  });

  it('returns the same array reference for existing list', () => {
    const { db } = createDb();
    db.set('k', 'list', 'listpack', ['x']);
    const result = getOrCreateList(db, 'k');
    const entry = db.get('k');
    expect(result.list).toBe(entry?.value);
  });

  it('returns WRONGTYPE error for non-list key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    const result = getOrCreateList(db, 'k');
    expect(result.list).toBeNull();
    expect(result.error).toEqual(WRONGTYPE);
  });

  it('returns WRONGTYPE for hash key', () => {
    const { db } = createDb();
    db.set('k', 'hash', 'listpack', new Map());
    const result = getOrCreateList(db, 'k');
    expect(result.list).toBeNull();
    expect(result.error).toEqual(WRONGTYPE);
  });
});

// --- getExistingList ---

describe('getExistingList', () => {
  it('returns null list and null error when key does not exist', () => {
    const { db } = createDb();
    const result = getExistingList(db, 'k');
    expect(result.list).toBeNull();
    expect(result.error).toBeNull();
  });

  it('returns existing list', () => {
    const { db } = createDb();
    db.set('k', 'list', 'listpack', ['a', 'b', 'c']);
    const result = getExistingList(db, 'k');
    expect(result.list).toEqual(['a', 'b', 'c']);
    expect(result.error).toBeNull();
  });

  it('returns WRONGTYPE error for non-list key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    const result = getExistingList(db, 'k');
    expect(result.list).toBeNull();
    expect(result.error).toEqual(WRONGTYPE);
  });

  it('does not create a key when it does not exist', () => {
    const { db } = createDb();
    getExistingList(db, 'k');
    expect(db.get('k')).toBeNull();
  });
});

// --- updateEncoding ---

describe('updateEncoding', () => {
  it('does nothing when key does not exist', () => {
    const { db } = createDb();
    // Should not throw
    updateEncoding(db, 'k');
  });

  it('does nothing for non-list key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('raw');
  });

  it('keeps listpack encoding when list is small', () => {
    const { db } = createDb();
    db.set('k', 'list', 'listpack', ['a', 'b', 'c']);
    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  it('promotes to quicklist when entries exceed threshold', () => {
    const { db } = createDb();
    const items = Array.from({ length: 129 }, (_, i) => String(i));
    db.set('k', 'list', 'listpack', items);
    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('quicklist');
  });

  it('promotes to quicklist when item byte length exceeds threshold', () => {
    const { db } = createDb();
    const items = ['a'.repeat(65)];
    db.set('k', 'list', 'listpack', items);
    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('quicklist');
  });

  it('never demotes from quicklist even when list shrinks', () => {
    const { db } = createDb();
    db.set('k', 'list', 'quicklist', ['a']);
    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('quicklist');
  });

  it('promotes at exactly 129 entries (boundary)', () => {
    const { db } = createDb();
    const items128 = Array.from({ length: 128 }, (_, i) => String(i));
    db.set('k', 'list', 'listpack', items128);
    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('listpack');

    const items129 = [...items128, '128'];
    db.set('k', 'list', 'listpack', items129);
    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('quicklist');
  });
});

// --- deleteIfEmpty ---

describe('deleteIfEmpty', () => {
  it('deletes key when list is empty', () => {
    const { db } = createDb();
    db.set('k', 'list', 'listpack', []);
    deleteIfEmpty(db, 'k', []);
    expect(db.get('k')).toBeNull();
  });

  it('does not delete key when list has elements', () => {
    const { db } = createDb();
    const items = ['a'];
    db.set('k', 'list', 'listpack', items);
    deleteIfEmpty(db, 'k', items);
    expect(db.get('k')).not.toBeNull();
  });

  it('does not delete key when list has multiple elements', () => {
    const { db } = createDb();
    const items = ['a', 'b', 'c'];
    db.set('k', 'list', 'listpack', items);
    deleteIfEmpty(db, 'k', items);
    expect(db.get('k')).not.toBeNull();
  });
});

// --- parseCount ---

describe('parseCount', () => {
  it('returns null count when argument is undefined', () => {
    const result = parseCount(undefined);
    expect(result).toEqual({ count: null, error: null });
  });

  it('parses valid positive integer', () => {
    expect(parseCount('5')).toEqual({ count: 5, error: null });
  });

  it('parses zero', () => {
    expect(parseCount('0')).toEqual({ count: 0, error: null });
  });

  it('returns error for negative number', () => {
    expect(parseCount('-1')).toEqual({ count: null, error: NOT_INTEGER_ERR });
  });

  it('returns error for non-integer string', () => {
    expect(parseCount('abc')).toEqual({ count: null, error: NOT_INTEGER_ERR });
  });

  it('returns error for float string', () => {
    expect(parseCount('3.5')).toEqual({ count: null, error: NOT_INTEGER_ERR });
  });

  it('returns error for empty string', () => {
    expect(parseCount('')).toEqual({ count: null, error: NOT_INTEGER_ERR });
  });

  it('returns error for string with spaces', () => {
    expect(parseCount(' 5 ')).toEqual({ count: null, error: NOT_INTEGER_ERR });
  });

  it('parses large valid number', () => {
    expect(parseCount('999999')).toEqual({ count: 999999, error: null });
  });
});

// --- parseInteger ---

describe('parseInteger', () => {
  it('parses positive integer', () => {
    expect(parseInteger('42')).toEqual({ value: 42, error: null });
  });

  it('parses zero', () => {
    expect(parseInteger('0')).toEqual({ value: 0, error: null });
  });

  it('parses negative integer', () => {
    expect(parseInteger('-7')).toEqual({ value: -7, error: null });
  });

  it('returns error for non-numeric string', () => {
    expect(parseInteger('abc')).toEqual({
      value: null,
      error: NOT_INTEGER_ERR,
    });
  });

  it('returns error for float string', () => {
    expect(parseInteger('1.5')).toEqual({
      value: null,
      error: NOT_INTEGER_ERR,
    });
  });

  it('returns error for empty string', () => {
    expect(parseInteger('')).toEqual({ value: null, error: NOT_INTEGER_ERR });
  });

  it('returns error for string with leading/trailing spaces', () => {
    expect(parseInteger(' 5')).toEqual({ value: null, error: NOT_INTEGER_ERR });
  });

  it('returns error for mixed content', () => {
    expect(parseInteger('12abc')).toEqual({
      value: null,
      error: NOT_INTEGER_ERR,
    });
  });

  it('parses large negative number', () => {
    expect(parseInteger('-999999')).toEqual({ value: -999999, error: null });
  });
});

// --- resolveIndex ---

describe('resolveIndex', () => {
  it('returns positive index unchanged', () => {
    expect(resolveIndex(0, 5)).toBe(0);
    expect(resolveIndex(2, 5)).toBe(2);
    expect(resolveIndex(4, 5)).toBe(4);
  });

  it('resolves -1 to last element', () => {
    expect(resolveIndex(-1, 5)).toBe(4);
  });

  it('resolves -N to first element when N equals length', () => {
    expect(resolveIndex(-5, 5)).toBe(0);
  });

  it('resolves negative beyond length to negative result', () => {
    // -6 + 5 = -1 — still negative, matches Redis behavior
    expect(resolveIndex(-6, 5)).toBe(-1);
  });

  it('handles length of 0', () => {
    expect(resolveIndex(0, 0)).toBe(0);
    expect(resolveIndex(-1, 0)).toBe(-1);
  });

  it('handles length of 1', () => {
    expect(resolveIndex(0, 1)).toBe(0);
    expect(resolveIndex(-1, 1)).toBe(0);
  });

  it('returns index beyond length unchanged', () => {
    expect(resolveIndex(10, 5)).toBe(10);
  });
});
