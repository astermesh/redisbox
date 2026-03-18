import { describe, it, expect } from 'vitest';
import { Database } from './database.ts';

function createDb(time = 1000): {
  db: Database;
  setTime: (t: number) => void;
} {
  let now = time;
  const db = new Database(() => now);
  db.setRng(() => 0.5);
  return {
    db,
    setTime: (t: number) => {
      now = t;
    },
  };
}

describe('Database', () => {
  describe('basic operations', () => {
    it('stores and retrieves entries', () => {
      const { db } = createDb();
      db.set('key1', 'string', 'raw', 'hello');
      const entry = db.get('key1');
      expect(entry).not.toBeNull();
      expect(entry?.type).toBe('string');
      expect(entry?.encoding).toBe('raw');
      expect(entry?.value).toBe('hello');
    });

    it('returns null for non-existent key', () => {
      const { db } = createDb();
      expect(db.get('missing')).toBeNull();
    });

    it('tracks type and encoding per entry', () => {
      const { db } = createDb();
      db.set('str', 'string', 'embstr', 'val');
      db.set('list', 'list', 'quicklist', []);
      expect(db.get('str')?.type).toBe('string');
      expect(db.get('str')?.encoding).toBe('embstr');
      expect(db.get('list')?.type).toBe('list');
      expect(db.get('list')?.encoding).toBe('quicklist');
    });

    it('has() returns true for existing key', () => {
      const { db } = createDb();
      db.set('x', 'string', 'raw', 'v');
      expect(db.has('x')).toBe(true);
    });

    it('has() returns false for missing key', () => {
      const { db } = createDb();
      expect(db.has('missing')).toBe(false);
    });

    it('delete removes key', () => {
      const { db } = createDb();
      db.set('k', 'string', 'raw', 'v');
      expect(db.delete('k')).toBe(true);
      expect(db.get('k')).toBeNull();
    });

    it('delete returns false for non-existent key', () => {
      const { db } = createDb();
      expect(db.delete('missing')).toBe(false);
    });

    it('overwrites existing entry', () => {
      const { db } = createDb();
      db.set('k', 'string', 'raw', 'v1');
      db.set('k', 'string', 'raw', 'v2');
      expect(db.get('k')?.value).toBe('v2');
    });

    it('reports correct size', () => {
      const { db } = createDb();
      expect(db.size).toBe(0);
      db.set('a', 'string', 'raw', '1');
      expect(db.size).toBe(1);
      db.set('b', 'string', 'raw', '2');
      expect(db.size).toBe(2);
      db.delete('a');
      expect(db.size).toBe(1);
    });
  });

  describe('LRU clock', () => {
    it('updates lruClock on get', () => {
      const { db, setTime } = createDb(1000);
      db.set('k', 'string', 'raw', 'v');
      setTime(2000);
      const entry = db.get('k');
      expect(entry?.lruClock).toBe(2000);
    });

    it('getWithoutTouch does not update lruClock', () => {
      const { db, setTime } = createDb(1000);
      db.set('k', 'string', 'raw', 'v');
      setTime(2000);
      const entry = db.getWithoutTouch('k');
      expect(entry?.lruClock).toBe(1000);
    });

    it('touch updates lruClock', () => {
      const { db, setTime } = createDb(1000);
      db.set('k', 'string', 'raw', 'v');
      setTime(3000);
      db.touch('k');
      expect(db.getWithoutTouch('k')?.lruClock).toBe(3000);
    });
  });

  describe('lazy expiration (T02)', () => {
    it('returns null for expired key on get', () => {
      const { db, setTime } = createDb(1000);
      db.set('k', 'string', 'raw', 'v');
      db.setExpiry('k', 2000);
      setTime(2000);
      expect(db.get('k')).toBeNull();
    });

    it('returns entry before expiry time', () => {
      const { db, setTime } = createDb(1000);
      db.set('k', 'string', 'raw', 'v');
      db.setExpiry('k', 2000);
      setTime(1999);
      expect(db.get('k')).not.toBeNull();
    });

    it('cleans up expiry metadata after expiration', () => {
      const { db, setTime } = createDb(1000);
      db.set('k', 'string', 'raw', 'v');
      db.setExpiry('k', 2000);
      setTime(2000);
      db.get('k');
      expect(db.getExpiry('k')).toBeUndefined();
    });

    it('has() returns false for expired key', () => {
      const { db, setTime } = createDb(1000);
      db.set('k', 'string', 'raw', 'v');
      db.setExpiry('k', 2000);
      setTime(3000);
      expect(db.has('k')).toBe(false);
    });

    it('does not affect non-expired keys', () => {
      const { db, setTime } = createDb(1000);
      db.set('k', 'string', 'raw', 'v');
      db.setExpiry('k', 5000);
      setTime(3000);
      expect(db.get('k')).not.toBeNull();
    });

    it('touch returns false for expired key', () => {
      const { db, setTime } = createDb(1000);
      db.set('k', 'string', 'raw', 'v');
      db.setExpiry('k', 2000);
      setTime(2000);
      expect(db.touch('k')).toBe(false);
    });

    it('cleans up fieldExpiry when key lazily expires', () => {
      const { db, setTime } = createDb(1000);
      const fields = new Map([['f1', 'v1'], ['f2', 'v2']]);
      db.set('h', 'hash', 'hashtable', fields);
      db.setExpiry('h', 2000);
      db.setFieldExpiry('h', 'f1', 5000);
      db.setFieldExpiry('h', 'f2', 6000);
      expect(db.fieldExpirySize).toBe(1);

      setTime(2000);
      db.get('h'); // triggers lazy key-level expiration
      expect(db.has('h')).toBe(false);
      expect(db.fieldExpirySize).toBe(0);
    });
  });

  describe('key version tracking (T03)', () => {
    it('returns 0 for non-existent key', () => {
      const { db } = createDb();
      expect(db.getVersion('missing')).toBe(0);
    });

    it('increments version on set', () => {
      const { db } = createDb();
      db.set('k', 'string', 'raw', 'v1');
      const v1 = db.getVersion('k');
      db.set('k', 'string', 'raw', 'v2');
      const v2 = db.getVersion('k');
      expect(v2).toBeGreaterThan(v1);
    });

    it('increments version on delete', () => {
      const { db } = createDb();
      db.set('k', 'string', 'raw', 'v');
      const v1 = db.getVersion('k');
      db.delete('k');
      const v2 = db.getVersion('k');
      expect(v2).toBeGreaterThan(v1);
    });

    it('increments version on expiration', () => {
      const { db, setTime } = createDb(1000);
      db.set('k', 'string', 'raw', 'v');
      db.setExpiry('k', 2000);
      const v1 = db.getVersion('k');
      setTime(2000);
      db.get('k'); // triggers expiration
      const v2 = db.getVersion('k');
      expect(v2).toBeGreaterThan(v1);
    });

    it('increments version on rename (both keys)', () => {
      const { db } = createDb();
      db.set('src', 'string', 'raw', 'v');
      const vSrc = db.getVersion('src');
      db.rename('src', 'dst');
      expect(db.getVersion('src')).toBeGreaterThan(vSrc);
      expect(db.getVersion('dst')).toBeGreaterThan(0);
    });

    it('version is monotonically increasing', () => {
      const { db } = createDb();
      const versions: number[] = [];
      db.set('a', 'string', 'raw', '1');
      versions.push(db.getVersion('a'));
      db.set('b', 'string', 'raw', '2');
      versions.push(db.getVersion('b'));
      db.set('a', 'string', 'raw', '3');
      versions.push(db.getVersion('a'));
      for (let i = 1; i < versions.length; i++) {
        const prev = versions[i - 1] ?? 0;
        const curr = versions[i] ?? 0;
        expect(curr).toBeGreaterThan(prev);
      }
    });
  });

  describe('rename', () => {
    it('renames key', () => {
      const { db } = createDb();
      db.set('src', 'string', 'raw', 'val');
      db.rename('src', 'dst');
      expect(db.get('src')).toBeNull();
      expect(db.get('dst')?.value).toBe('val');
    });

    it('preserves TTL on rename', () => {
      const { db } = createDb();
      db.set('src', 'string', 'raw', 'val');
      db.setExpiry('src', 5000);
      db.rename('src', 'dst');
      expect(db.getExpiry('dst')).toBe(5000);
      expect(db.getExpiry('src')).toBeUndefined();
    });

    it('src == dst is a no-op', () => {
      const { db } = createDb();
      db.set('k', 'string', 'raw', 'v');
      db.rename('k', 'k');
      expect(db.get('k')?.value).toBe('v');
    });

    it('overwrites destination', () => {
      const { db } = createDb();
      db.set('src', 'string', 'raw', 'new');
      db.set('dst', 'string', 'raw', 'old');
      db.rename('src', 'dst');
      expect(db.get('dst')?.value).toBe('new');
    });
  });

  describe('expiry management', () => {
    it('setExpiry returns false for non-existent key', () => {
      const { db } = createDb();
      expect(db.setExpiry('missing', 5000)).toBe(false);
    });

    it('removeExpiry returns false for non-existent key', () => {
      const { db } = createDb();
      expect(db.removeExpiry('missing')).toBe(false);
    });

    it('removeExpiry returns false when no expiry set', () => {
      const { db } = createDb();
      db.set('k', 'string', 'raw', 'v');
      expect(db.removeExpiry('k')).toBe(false);
    });

    it('removeExpiry returns true and removes expiry', () => {
      const { db } = createDb();
      db.set('k', 'string', 'raw', 'v');
      db.setExpiry('k', 5000);
      expect(db.removeExpiry('k')).toBe(true);
      expect(db.getExpiry('k')).toBeUndefined();
    });
  });

  describe('copyEntry', () => {
    it('returns deep copy of entry', () => {
      const { db } = createDb();
      db.set('k', 'list', 'quicklist', ['a', 'b']);
      const copy = db.copyEntry('k');
      expect(copy).not.toBeNull();
      expect(copy?.value).toEqual(['a', 'b']);
      // Mutating original should not affect copy
      const original = db.get('k');
      (original?.value as string[]).push('c');
      expect(copy?.value).toEqual(['a', 'b']);
    });

    it('returns null for non-existent key', () => {
      const { db } = createDb();
      expect(db.copyEntry('missing')).toBeNull();
    });
  });

  describe('randomKey', () => {
    it('returns null for empty db', () => {
      const { db } = createDb();
      expect(db.randomKey()).toBeNull();
    });

    it('returns a key from the db', () => {
      const { db } = createDb();
      db.set('a', 'string', 'raw', '1');
      db.set('b', 'string', 'raw', '2');
      const key = db.randomKey();
      expect(['a', 'b']).toContain(key);
    });

    it('skips expired keys', () => {
      const { db, setTime } = createDb(1000);
      db.set('expired', 'string', 'raw', 'v');
      db.setExpiry('expired', 1500);
      db.set('alive', 'string', 'raw', 'v');
      setTime(2000);
      expect(db.randomKey()).toBe('alive');
    });
  });

  describe('field expiry management', () => {
    it('setFieldExpiry returns false for non-existent key', () => {
      const { db } = createDb();
      expect(db.setFieldExpiry('missing', 'f1', 5000)).toBe(false);
    });

    it('setFieldExpiry returns false for non-hash key', () => {
      const { db } = createDb();
      db.set('k', 'string', 'raw', 'v');
      expect(db.setFieldExpiry('k', 'f1', 5000)).toBe(false);
    });

    it('setFieldExpiry returns false for non-existent field', () => {
      const { db } = createDb();
      const fields = new Map([['f1', 'v1']]);
      db.set('h', 'hash', 'hashtable', fields);
      expect(db.setFieldExpiry('h', 'missing', 5000)).toBe(false);
    });

    it('sets and gets field expiry', () => {
      const { db } = createDb();
      const fields = new Map([['f1', 'v1'], ['f2', 'v2']]);
      db.set('h', 'hash', 'hashtable', fields);
      expect(db.setFieldExpiry('h', 'f1', 5000)).toBe(true);
      expect(db.getFieldExpiry('h', 'f1')).toBe(5000);
      expect(db.getFieldExpiry('h', 'f2')).toBeUndefined();
    });

    it('getFieldExpiry returns undefined for non-existent key', () => {
      const { db } = createDb();
      expect(db.getFieldExpiry('missing', 'f1')).toBeUndefined();
    });

    it('removeFieldExpiry removes field TTL', () => {
      const { db } = createDb();
      const fields = new Map([['f1', 'v1']]);
      db.set('h', 'hash', 'hashtable', fields);
      db.setFieldExpiry('h', 'f1', 5000);
      expect(db.removeFieldExpiry('h', 'f1')).toBe(true);
      expect(db.getFieldExpiry('h', 'f1')).toBeUndefined();
    });

    it('removeFieldExpiry returns false when no field expiry set', () => {
      const { db } = createDb();
      const fields = new Map([['f1', 'v1']]);
      db.set('h', 'hash', 'hashtable', fields);
      expect(db.removeFieldExpiry('h', 'f1')).toBe(false);
    });

    it('removeFieldExpiry returns false for non-existent key', () => {
      const { db } = createDb();
      expect(db.removeFieldExpiry('missing', 'f1')).toBe(false);
    });

    it('tracks fieldExpirySize correctly', () => {
      const { db } = createDb();
      expect(db.fieldExpirySize).toBe(0);

      const fields1 = new Map([['f1', 'v1'], ['f2', 'v2']]);
      db.set('h1', 'hash', 'hashtable', fields1);
      db.setFieldExpiry('h1', 'f1', 5000);
      expect(db.fieldExpirySize).toBe(1);

      const fields2 = new Map([['f1', 'v1']]);
      db.set('h2', 'hash', 'hashtable', fields2);
      db.setFieldExpiry('h2', 'f1', 5000);
      expect(db.fieldExpirySize).toBe(2);

      // Adding more field expiry to same key doesn't increase size
      db.setFieldExpiry('h1', 'f2', 6000);
      expect(db.fieldExpirySize).toBe(2);
    });

    it('cleans up fieldExpiry index when last field expiry removed', () => {
      const { db } = createDb();
      const fields = new Map([['f1', 'v1'], ['f2', 'v2']]);
      db.set('h', 'hash', 'hashtable', fields);
      db.setFieldExpiry('h', 'f1', 5000);
      db.setFieldExpiry('h', 'f2', 6000);
      expect(db.fieldExpirySize).toBe(1);

      db.removeFieldExpiry('h', 'f1');
      expect(db.fieldExpirySize).toBe(1); // still has f2

      db.removeFieldExpiry('h', 'f2');
      expect(db.fieldExpirySize).toBe(0); // cleaned up
    });

    it('cleans up fieldExpiry when key is deleted', () => {
      const { db } = createDb();
      const fields = new Map([['f1', 'v1']]);
      db.set('h', 'hash', 'hashtable', fields);
      db.setFieldExpiry('h', 'f1', 5000);
      expect(db.fieldExpirySize).toBe(1);

      db.delete('h');
      expect(db.fieldExpirySize).toBe(0);
    });

    it('bumps version on setFieldExpiry', () => {
      const { db } = createDb();
      const fields = new Map([['f1', 'v1']]);
      db.set('h', 'hash', 'hashtable', fields);
      const v1 = db.getVersion('h');
      db.setFieldExpiry('h', 'f1', 5000);
      expect(db.getVersion('h')).toBeGreaterThan(v1);
    });
  });

  describe('lazy field expiration', () => {
    it('tryExpireField expires a field past its TTL', () => {
      const { db, setTime } = createDb(1000);
      const fields = new Map([['f1', 'v1'], ['f2', 'v2']]);
      db.set('h', 'hash', 'hashtable', fields);
      db.setFieldExpiry('h', 'f1', 2000);

      setTime(2000);
      expect(db.tryExpireField('h', 'f1')).toBe(true);

      const entry = db.get('h');
      const val = entry?.value as Map<string, string>;
      expect(val.has('f1')).toBe(false);
      expect(val.has('f2')).toBe(true);
    });

    it('tryExpireField does not expire a field before its TTL', () => {
      const { db, setTime } = createDb(1000);
      const fields = new Map([['f1', 'v1']]);
      db.set('h', 'hash', 'hashtable', fields);
      db.setFieldExpiry('h', 'f1', 3000);

      setTime(2000);
      expect(db.tryExpireField('h', 'f1')).toBe(false);

      const entry = db.get('h');
      const val = entry?.value as Map<string, string>;
      expect(val.has('f1')).toBe(true);
    });

    it('tryExpireField returns false for field without TTL', () => {
      const { db } = createDb(1000);
      const fields = new Map([['f1', 'v1']]);
      db.set('h', 'hash', 'hashtable', fields);
      expect(db.tryExpireField('h', 'f1')).toBe(false);
    });

    it('tryExpireField returns false for non-existent key', () => {
      const { db } = createDb(1000);
      expect(db.tryExpireField('missing', 'f1')).toBe(false);
    });

    it('tryExpireField deletes key when last field expires', () => {
      const { db, setTime } = createDb(1000);
      const fields = new Map([['f1', 'v1']]);
      db.set('h', 'hash', 'hashtable', fields);
      db.setFieldExpiry('h', 'f1', 2000);

      setTime(2000);
      db.tryExpireField('h', 'f1');

      expect(db.has('h')).toBe(false);
      expect(db.fieldExpirySize).toBe(0);
    });

    it('tryExpireField cleans up field expiry metadata', () => {
      const { db, setTime } = createDb(1000);
      const fields = new Map([['f1', 'v1'], ['f2', 'v2']]);
      db.set('h', 'hash', 'hashtable', fields);
      db.setFieldExpiry('h', 'f1', 2000);
      db.setFieldExpiry('h', 'f2', 5000);

      setTime(2000);
      db.tryExpireField('h', 'f1');

      expect(db.getFieldExpiry('h', 'f1')).toBeUndefined();
      expect(db.getFieldExpiry('h', 'f2')).toBe(5000);
      expect(db.fieldExpirySize).toBe(1); // still has h with f2
    });

    it('tryExpireField bumps version when field expired', () => {
      const { db, setTime } = createDb(1000);
      const fields = new Map([['f1', 'v1'], ['f2', 'v2']]);
      db.set('h', 'hash', 'hashtable', fields);
      db.setFieldExpiry('h', 'f1', 2000);
      const v1 = db.getVersion('h');

      setTime(2000);
      db.tryExpireField('h', 'f1');
      expect(db.getVersion('h')).toBeGreaterThan(v1);
    });
  });

  describe('sampleFieldExpiryKeys', () => {
    it('returns up to count random keys from field expiry index', () => {
      const { db } = createDb();
      for (let i = 0; i < 50; i++) {
        const fields = new Map([['f1', `v${i}`]]);
        db.set(`h${i}`, 'hash', 'hashtable', fields);
        db.setFieldExpiry(`h${i}`, 'f1', 5000);
      }

      const sample = db.sampleFieldExpiryKeys(20, () => Math.random());
      expect(sample.length).toBe(20);
      for (const key of sample) {
        expect(db.getFieldExpiry(key, 'f1')).toBeDefined();
      }
    });

    it('returns all keys when count exceeds fieldExpirySize', () => {
      const { db } = createDb();
      for (let i = 0; i < 3; i++) {
        const fields = new Map([['f1', `v${i}`]]);
        db.set(`h${i}`, 'hash', 'hashtable', fields);
        db.setFieldExpiry(`h${i}`, 'f1', 5000);
      }

      const sample = db.sampleFieldExpiryKeys(20, () => Math.random());
      expect(sample.length).toBe(3);
    });

    it('returns empty array when no field expiry exists', () => {
      const { db } = createDb();
      db.set('h', 'hash', 'hashtable', new Map([['f1', 'v1']]));
      expect(db.sampleFieldExpiryKeys(20, () => Math.random()).length).toBe(0);
    });

    it('returns unique keys', () => {
      const { db } = createDb();
      for (let i = 0; i < 50; i++) {
        const fields = new Map([['f1', `v${i}`]]);
        db.set(`h${i}`, 'hash', 'hashtable', fields);
        db.setFieldExpiry(`h${i}`, 'f1', 5000);
      }

      const sample = db.sampleFieldExpiryKeys(20, () => Math.random());
      const unique = new Set(sample);
      expect(unique.size).toBe(sample.length);
    });
  });

  describe('sampleFieldsWithExpiry', () => {
    it('returns up to count fields with TTL for a given key', () => {
      const { db } = createDb();
      const fields = new Map<string, string>();
      for (let i = 0; i < 50; i++) {
        fields.set(`f${i}`, `v${i}`);
      }
      db.set('h', 'hash', 'hashtable', fields);
      for (let i = 0; i < 50; i++) {
        db.setFieldExpiry('h', `f${i}`, 5000 + i);
      }

      const sample = db.sampleFieldsWithExpiry('h', 20, () => Math.random());
      expect(sample.length).toBe(20);
      for (const field of sample) {
        expect(db.getFieldExpiry('h', field)).toBeDefined();
      }
    });

    it('returns all fields when count exceeds available', () => {
      const { db } = createDb();
      const fields = new Map([['f1', 'v1'], ['f2', 'v2']]);
      db.set('h', 'hash', 'hashtable', fields);
      db.setFieldExpiry('h', 'f1', 5000);
      db.setFieldExpiry('h', 'f2', 6000);

      const sample = db.sampleFieldsWithExpiry('h', 20, () => Math.random());
      expect(sample.length).toBe(2);
    });

    it('returns empty array for non-existent key', () => {
      const { db } = createDb();
      expect(db.sampleFieldsWithExpiry('missing', 20, () => Math.random()).length).toBe(0);
    });

    it('returns unique fields', () => {
      const { db } = createDb();
      const fields = new Map<string, string>();
      for (let i = 0; i < 50; i++) {
        fields.set(`f${i}`, `v${i}`);
      }
      db.set('h', 'hash', 'hashtable', fields);
      for (let i = 0; i < 50; i++) {
        db.setFieldExpiry('h', `f${i}`, 5000 + i);
      }

      const sample = db.sampleFieldsWithExpiry('h', 20, () => Math.random());
      const unique = new Set(sample);
      expect(unique.size).toBe(sample.length);
    });
  });
});
