import { describe, it, expect, beforeEach } from 'vitest';
import { ConfigStore, globMatch } from './config-store.ts';

// ===========================================================================
// globMatch
// ===========================================================================

describe('globMatch', () => {
  it('matches exact string', () => {
    expect(globMatch('hello', 'hello')).toBe(true);
  });

  it('rejects different string', () => {
    expect(globMatch('hello', 'world')).toBe(false);
  });

  it('matches * wildcard for any substring', () => {
    expect(globMatch('h*o', 'hello')).toBe(true);
    expect(globMatch('h*o', 'ho')).toBe(true);
    expect(globMatch('h*o', 'hxo')).toBe(true);
  });

  it('matches * at beginning', () => {
    expect(globMatch('*ello', 'hello')).toBe(true);
  });

  it('matches * at end', () => {
    expect(globMatch('hell*', 'hello')).toBe(true);
  });

  it('matches standalone *', () => {
    expect(globMatch('*', 'anything')).toBe(true);
    expect(globMatch('*', '')).toBe(true);
  });

  it('matches ? for single character', () => {
    expect(globMatch('h?llo', 'hello')).toBe(true);
    expect(globMatch('h?llo', 'hallo')).toBe(true);
    expect(globMatch('h?llo', 'hllo')).toBe(false);
  });

  it('matches character class [abc]', () => {
    expect(globMatch('h[ae]llo', 'hello')).toBe(true);
    expect(globMatch('h[ae]llo', 'hallo')).toBe(true);
    expect(globMatch('h[ae]llo', 'hillo')).toBe(false);
  });

  it('matches negated character class [^abc]', () => {
    expect(globMatch('h[^ae]llo', 'hillo')).toBe(true);
    expect(globMatch('h[^ae]llo', 'hello')).toBe(false);
  });

  it('matches character range [a-z]', () => {
    expect(globMatch('[a-z]ello', 'hello')).toBe(true);
    expect(globMatch('[a-z]ello', 'Hello')).toBe(false);
  });

  it('handles escaped characters', () => {
    expect(globMatch('h\\*llo', 'h*llo')).toBe(true);
    expect(globMatch('h\\*llo', 'hello')).toBe(false);
  });

  it('handles multiple wildcards', () => {
    expect(globMatch('*max*entries*', 'hash-max-listpack-entries')).toBe(true);
    expect(globMatch('*max*entries*', 'zset-max-ziplist-entries')).toBe(true);
    expect(globMatch('*max*entries*', 'maxmemory')).toBe(false);
  });

  it('handles empty pattern and string', () => {
    expect(globMatch('', '')).toBe(true);
    expect(globMatch('', 'a')).toBe(false);
  });

  it('matches Redis-style config patterns', () => {
    expect(globMatch('maxmemory*', 'maxmemory')).toBe(true);
    expect(globMatch('maxmemory*', 'maxmemory-policy')).toBe(true);
    expect(globMatch('maxmemory*', 'maxmemory-samples')).toBe(true);
    expect(globMatch('maxmemory*', 'maxclients')).toBe(false);
  });
});

// ===========================================================================
// ConfigStore
// ===========================================================================

describe('ConfigStore', () => {
  let store: ConfigStore;

  beforeEach(() => {
    store = new ConfigStore();
  });

  // -------------------------------------------------------------------------
  // get
  // -------------------------------------------------------------------------

  describe('get', () => {
    it('returns key-value pair for exact match', () => {
      const result = store.get('maxmemory');
      expect(result).toEqual(['maxmemory', '0']);
    });

    it('returns empty array for non-existent key', () => {
      const result = store.get('nonexistent-key-xyz');
      expect(result).toEqual([]);
    });

    it('returns all matching keys for glob pattern', () => {
      const result = store.get('maxmemory*');
      expect(result.length).toBeGreaterThanOrEqual(4);
      // Should have key-value pairs
      expect(result.length % 2).toBe(0);
      expect(result).toContain('maxmemory');
      expect(result).toContain('maxmemory-policy');
      expect(result).toContain('maxmemory-samples');
    });

    it('returns all config params for * pattern', () => {
      const result = store.get('*');
      expect(result.length).toBeGreaterThan(100);
      expect(result.length % 2).toBe(0);
    });

    it('returns flat array [key, val, key, val, ...]', () => {
      const result = store.get('hz');
      expect(result).toEqual(['hz', '10']);
    });

    it('is case-insensitive on pattern', () => {
      const lower = store.get('maxmemory');
      const upper = store.get('MAXMEMORY');
      expect(lower).toEqual(upper);
    });

    it('matches complex glob patterns', () => {
      const result = store.get('*max*entries*');
      expect(result.length).toBeGreaterThanOrEqual(2);
      // All keys should contain "max" and "entries"
      for (let i = 0; i < result.length; i += 2) {
        expect(result[i]).toContain('max');
        expect(result[i]).toContain('entries');
      }
    });
  });

  // -------------------------------------------------------------------------
  // getMulti
  // -------------------------------------------------------------------------

  describe('getMulti', () => {
    it('returns results for multiple patterns', () => {
      const result = store.getMulti(['hz', 'maxmemory']);
      expect(result).toContain('hz');
      expect(result).toContain('10');
      expect(result).toContain('maxmemory');
      expect(result).toContain('0');
    });

    it('deduplicates keys matching multiple patterns', () => {
      const result = store.getMulti(['hz', 'h*']);
      const keys = result.filter((_, i) => i % 2 === 0);
      const uniqueKeys = new Set(keys);
      expect(keys.length).toBe(uniqueKeys.size);
    });
  });

  // -------------------------------------------------------------------------
  // set
  // -------------------------------------------------------------------------

  describe('set', () => {
    it('sets a valid config value', () => {
      const err = store.set('maxmemory', '1048576');
      expect(err).toBeNull();
      expect(store.get('maxmemory')).toEqual(['maxmemory', '1048576']);
    });

    it('returns error for unknown parameter', () => {
      const err = store.set('nonexistent-param', 'value');
      expect(err).toBe('ERR Unsupported CONFIG parameter: nonexistent-param');
    });

    it('returns error for invalid value', () => {
      const err = store.set('maxmemory-policy', 'invalid-policy');
      expect(err).toBe(
        "ERR Invalid argument 'invalid-policy' for CONFIG SET 'maxmemory-policy'"
      );
    });

    it('accepts valid maxmemory-policy values', () => {
      for (const policy of [
        'volatile-lru',
        'allkeys-lru',
        'volatile-lfu',
        'allkeys-lfu',
        'volatile-random',
        'allkeys-random',
        'volatile-ttl',
        'noeviction',
      ]) {
        expect(store.set('maxmemory-policy', policy)).toBeNull();
        expect(store.get('maxmemory-policy')).toEqual([
          'maxmemory-policy',
          policy,
        ]);
      }
    });

    it('validates yes/no parameters', () => {
      expect(store.set('activerehashing', 'yes')).toBeNull();
      expect(store.set('activerehashing', 'no')).toBeNull();
      expect(store.set('activerehashing', 'maybe')).not.toBeNull();
    });

    it('validates numeric parameters', () => {
      expect(store.set('hz', '100')).toBeNull();
      expect(store.set('hz', 'abc')).not.toBeNull();
    });

    it('validates loglevel', () => {
      expect(store.set('loglevel', 'debug')).toBeNull();
      expect(store.set('loglevel', 'verbose')).toBeNull();
      expect(store.set('loglevel', 'notice')).toBeNull();
      expect(store.set('loglevel', 'warning')).toBeNull();
      expect(store.set('loglevel', 'critical')).not.toBeNull();
    });

    it('is case-insensitive on key', () => {
      expect(store.set('MAXMEMORY', '999')).toBeNull();
      expect(store.get('maxmemory')).toEqual(['maxmemory', '999']);
    });

    it('allows setting params without validators', () => {
      expect(store.set('logfile', '/tmp/redis.log')).toBeNull();
      expect(store.get('logfile')).toEqual(['logfile', '/tmp/redis.log']);
    });

    it('accepts negative integers where allowed', () => {
      expect(store.set('list-max-listpack-size', '-5')).toBeNull();
      expect(store.get('list-max-listpack-size')).toEqual([
        'list-max-listpack-size',
        '-5',
      ]);
    });

    it('rejects negative integers for non-negative fields', () => {
      expect(store.set('hz', '-1')).not.toBeNull();
    });
  });

  // -------------------------------------------------------------------------
  // setMulti
  // -------------------------------------------------------------------------

  describe('setMulti', () => {
    it('sets multiple values atomically', () => {
      const err = store.setMulti([
        ['hz', '20'],
        ['maxmemory', '2048'],
      ]);
      expect(err).toBeNull();
      expect(store.get('hz')).toEqual(['hz', '20']);
      expect(store.get('maxmemory')).toEqual(['maxmemory', '2048']);
    });

    it('rolls back on first error (all-or-nothing)', () => {
      store.set('hz', '10');
      const err = store.setMulti([
        ['hz', '20'],
        ['nonexistent', 'value'],
      ]);
      expect(err).not.toBeNull();
      // hz should remain unchanged
      expect(store.get('hz')).toEqual(['hz', '10']);
    });

    it('validates all values before applying any', () => {
      store.set('hz', '10');
      const err = store.setMulti([
        ['hz', 'invalid'],
        ['maxmemory', '1024'],
      ]);
      expect(err).not.toBeNull();
      // neither should change
      expect(store.get('hz')).toEqual(['hz', '10']);
      expect(store.get('maxmemory')).toEqual(['maxmemory', '0']);
    });
  });

  // -------------------------------------------------------------------------
  // resetStat
  // -------------------------------------------------------------------------

  describe('resetStat', () => {
    it('does not throw', () => {
      expect(() => store.resetStat()).not.toThrow();
    });
  });

  // -------------------------------------------------------------------------
  // resetToDefaults
  // -------------------------------------------------------------------------

  describe('resetToDefaults', () => {
    it('restores modified values to defaults', () => {
      store.set('hz', '100');
      store.set('maxmemory', '999');
      store.resetToDefaults();
      expect(store.get('hz')).toEqual(['hz', '10']);
      expect(store.get('maxmemory')).toEqual(['maxmemory', '0']);
    });
  });

  // -------------------------------------------------------------------------
  // onChange
  // -------------------------------------------------------------------------

  describe('onChange', () => {
    it('calls listener on set', () => {
      const changes: { key: string; value: string; oldValue: string }[] =
        [];
      store.onChange((c) => changes.push(...c));
      store.set('hz', '50');
      expect(changes).toEqual([{ key: 'hz', value: '50', oldValue: '10' }]);
    });

    it('calls listener on setMulti', () => {
      const changes: { key: string; value: string; oldValue: string }[] =
        [];
      store.onChange((c) => changes.push(...c));
      store.setMulti([
        ['hz', '20'],
        ['maxmemory', '1024'],
      ]);
      expect(changes).toEqual([
        { key: 'hz', value: '20', oldValue: '10' },
        { key: 'maxmemory', value: '1024', oldValue: '0' },
      ]);
    });

    it('does not call listener when value unchanged', () => {
      const changes: { key: string; value: string; oldValue: string }[] =
        [];
      store.onChange((c) => changes.push(...c));
      store.set('hz', '10'); // same as default
      expect(changes).toEqual([]);
    });

    it('does not call listener on validation error', () => {
      const changes: { key: string; value: string; oldValue: string }[] =
        [];
      store.onChange((c) => changes.push(...c));
      store.set('hz', 'invalid');
      expect(changes).toEqual([]);
    });

    it('unsubscribe stops notifications', () => {
      const changes: { key: string; value: string; oldValue: string }[] =
        [];
      const unsub = store.onChange((c) => changes.push(...c));
      store.set('hz', '20');
      expect(changes).toHaveLength(1);
      unsub();
      store.set('hz', '30');
      expect(changes).toHaveLength(1);
    });
  });

  // -------------------------------------------------------------------------
  // Default values match Redis 7.2
  // -------------------------------------------------------------------------

  describe('default values', () => {
    it.each([
      ['maxmemory', '0'],
      ['maxmemory-policy', 'noeviction'],
      ['maxmemory-samples', '5'],
      ['hz', '10'],
      ['databases', '16'],
      ['port', '6379'],
      ['tcp-backlog', '511'],
      ['timeout', '0'],
      ['tcp-keepalive', '300'],
      ['loglevel', 'notice'],
      ['maxclients', '10000'],
      ['appendonly', 'no'],
      ['appendfsync', 'everysec'],
      ['slowlog-log-slower-than', '10000'],
      ['slowlog-max-len', '128'],
      ['hash-max-listpack-entries', '128'],
      ['hash-max-listpack-value', '64'],
      ['list-max-listpack-size', '-2'],
      ['list-compress-depth', '0'],
      ['set-max-intset-entries', '512'],
      ['set-max-listpack-entries', '128'],
      ['zset-max-listpack-entries', '128'],
      ['zset-max-listpack-value', '64'],
      ['hll-sparse-max-bytes', '3000'],
      ['stream-node-max-bytes', '4096'],
      ['stream-node-max-entries', '100'],
      ['activerehashing', 'yes'],
      ['notify-keyspace-events', ''],
      ['requirepass', ''],
      ['protected-mode', 'yes'],
      ['latency-monitor-threshold', '0'],
      ['lua-time-limit', '5000'],
      ['replica-priority', '100'],
      ['acllog-max-len', '128'],
    ])('defaults %s = %s', (key, expected) => {
      const result = store.get(key);
      expect(result).toEqual([key, expected]);
    });
  });
});
