import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Reply } from '../types.ts';
import { sort, sortRo } from './sort.ts';

function createDb() {
  const engine = new RedisEngine({ clock: () => 1000, rng: () => 0.5 });
  return engine.db(0);
}

function extractBulkValues(reply: Reply): (string | null)[] {
  if (reply.kind !== 'array') return [];
  return reply.value
    .filter((r): r is Reply & { kind: 'bulk' } => r.kind === 'bulk')
    .map((r) => r.value);
}

describe('SORT', () => {
  describe('on lists', () => {
    it('sorts numeric values ascending by default', () => {
      const db = createDb();
      db.set('mylist', 'list', 'quicklist', ['3', '1', '2']);
      const reply = sort(db, ['mylist']);
      expect(extractBulkValues(reply)).toEqual(['1', '2', '3']);
    });

    it('sorts descending with DESC', () => {
      const db = createDb();
      db.set('mylist', 'list', 'quicklist', ['3', '1', '2']);
      const reply = sort(db, ['mylist', 'DESC']);
      expect(extractBulkValues(reply)).toEqual(['3', '2', '1']);
    });

    it('sorts alphabetically with ALPHA', () => {
      const db = createDb();
      db.set('mylist', 'list', 'quicklist', ['banana', 'apple', 'cherry']);
      const reply = sort(db, ['mylist', 'ALPHA']);
      expect(extractBulkValues(reply)).toEqual(['apple', 'banana', 'cherry']);
    });

    it('returns error for non-numeric values without ALPHA', () => {
      const db = createDb();
      db.set('mylist', 'list', 'quicklist', ['a', 'b', 'c']);
      const reply = sort(db, ['mylist']);
      expect(reply.kind).toBe('error');
    });

    it('applies LIMIT offset count', () => {
      const db = createDb();
      db.set('mylist', 'list', 'quicklist', ['5', '3', '1', '4', '2']);
      const reply = sort(db, ['mylist', 'LIMIT', '1', '2']);
      expect(extractBulkValues(reply)).toEqual(['2', '3']);
    });
  });

  describe('on sets', () => {
    it('sorts set members', () => {
      const db = createDb();
      db.set('myset', 'set', 'hashtable', new Set(['3', '1', '2']));
      const reply = sort(db, ['myset']);
      expect(extractBulkValues(reply)).toEqual(['1', '2', '3']);
    });
  });

  describe('on sorted sets', () => {
    it('sorts zset members', () => {
      const db = createDb();
      const zset = new Map<string, number>();
      zset.set('3', 30);
      zset.set('1', 10);
      zset.set('2', 20);
      db.set('myzset', 'zset', 'skiplist', zset);
      const reply = sort(db, ['myzset']);
      expect(extractBulkValues(reply)).toEqual(['1', '2', '3']);
    });
  });

  describe('non-existent key', () => {
    it('returns empty array', () => {
      const db = createDb();
      const reply = sort(db, ['nokey']);
      expect(reply).toEqual({ kind: 'array', value: [] });
    });
  });

  describe('wrong type', () => {
    it('returns WRONGTYPE error for string', () => {
      const db = createDb();
      db.set('k', 'string', 'raw', 'v');
      const reply = sort(db, ['k']);
      expect(reply.kind).toBe('error');
      if (reply.kind === 'error') {
        expect(reply.prefix).toBe('WRONGTYPE');
      }
    });
  });

  describe('BY pattern', () => {
    it('sorts by external key values', () => {
      const db = createDb();
      db.set('mylist', 'list', 'quicklist', ['a', 'b', 'c']);
      db.set('weight_a', 'string', 'raw', '3');
      db.set('weight_b', 'string', 'raw', '1');
      db.set('weight_c', 'string', 'raw', '2');
      const reply = sort(db, ['mylist', 'BY', 'weight_*']);
      expect(extractBulkValues(reply)).toEqual(['b', 'c', 'a']);
    });

    it('BY nosort returns elements in storage order', () => {
      const db = createDb();
      db.set('mylist', 'list', 'quicklist', ['c', 'a', 'b']);
      const reply = sort(db, ['mylist', 'BY', 'nosort']);
      expect(extractBulkValues(reply)).toEqual(['c', 'a', 'b']);
    });

    it('BY with hash field reference', () => {
      const db = createDb();
      db.set('mylist', 'list', 'quicklist', ['a', 'b', 'c']);
      db.set('hash_a', 'hash', 'hashtable', new Map([['weight', '3']]));
      db.set('hash_b', 'hash', 'hashtable', new Map([['weight', '1']]));
      db.set('hash_c', 'hash', 'hashtable', new Map([['weight', '2']]));
      const reply = sort(db, ['mylist', 'BY', 'hash_*->weight']);
      expect(extractBulkValues(reply)).toEqual(['b', 'c', 'a']);
    });

    it('BY with missing external keys sorts as 0', () => {
      const db = createDb();
      db.set('mylist', 'list', 'quicklist', ['a', 'b']);
      db.set('weight_a', 'string', 'raw', '5');
      // weight_b is missing -> treated as 0
      const reply = sort(db, ['mylist', 'BY', 'weight_*']);
      expect(extractBulkValues(reply)).toEqual(['b', 'a']);
    });
  });

  describe('GET pattern', () => {
    it('retrieves external key values', () => {
      const db = createDb();
      db.set('mylist', 'list', 'quicklist', ['1', '2', '3']);
      db.set('name_1', 'string', 'raw', 'alice');
      db.set('name_2', 'string', 'raw', 'bob');
      db.set('name_3', 'string', 'raw', 'carol');
      const reply = sort(db, ['mylist', 'GET', 'name_*']);
      expect(extractBulkValues(reply)).toEqual(['alice', 'bob', 'carol']);
    });

    it('GET # returns element itself', () => {
      const db = createDb();
      db.set('mylist', 'list', 'quicklist', ['1', '2', '3']);
      db.set('name_1', 'string', 'raw', 'alice');
      db.set('name_2', 'string', 'raw', 'bob');
      db.set('name_3', 'string', 'raw', 'carol');
      const reply = sort(db, ['mylist', 'GET', '#', 'GET', 'name_*']);
      expect(extractBulkValues(reply)).toEqual([
        '1',
        'alice',
        '2',
        'bob',
        '3',
        'carol',
      ]);
    });

    it('GET with missing key returns nil', () => {
      const db = createDb();
      db.set('mylist', 'list', 'quicklist', ['1', '2']);
      db.set('name_1', 'string', 'raw', 'alice');
      // name_2 is missing
      const reply = sort(db, ['mylist', 'GET', 'name_*']);
      expect(extractBulkValues(reply)).toEqual(['alice', null]);
    });

    it('GET with hash field', () => {
      const db = createDb();
      db.set('mylist', 'list', 'quicklist', ['1', '2']);
      db.set('obj_1', 'hash', 'hashtable', new Map([['name', 'alice']]));
      db.set('obj_2', 'hash', 'hashtable', new Map([['name', 'bob']]));
      const reply = sort(db, ['mylist', 'GET', 'obj_*->name']);
      expect(extractBulkValues(reply)).toEqual(['alice', 'bob']);
    });
  });

  describe('STORE', () => {
    it('stores result as list and returns count', () => {
      const db = createDb();
      db.set('mylist', 'list', 'quicklist', ['3', '1', '2']);
      const reply = sort(db, ['mylist', 'STORE', 'result']);
      expect(reply).toEqual({ kind: 'integer', value: 3 });
      const stored = db.get('result');
      expect(stored?.type).toBe('list');
      expect(stored?.value).toEqual(['1', '2', '3']);
    });

    it('deletes destination on empty result', () => {
      const db = createDb();
      db.set('result', 'string', 'raw', 'old');
      const reply = sort(db, ['empty', 'STORE', 'result']);
      expect(reply).toEqual({ kind: 'integer', value: 0 });
      expect(db.has('result')).toBe(false);
    });
  });
});

describe('SORT_RO', () => {
  it('sorts like SORT', () => {
    const db = createDb();
    db.set('mylist', 'list', 'quicklist', ['3', '1', '2']);
    const reply = sortRo(db, ['mylist']);
    expect(extractBulkValues(reply)).toEqual(['1', '2', '3']);
  });

  it('rejects STORE option', () => {
    const db = createDb();
    db.set('mylist', 'list', 'quicklist', ['3', '1', '2']);
    const reply = sortRo(db, ['mylist', 'STORE', 'dest']);
    expect(reply.kind).toBe('error');
  });
});
