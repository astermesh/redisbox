/**
 * Integration tests for the dual-backend parity harness.
 *
 * These tests require a real Redis instance on localhost:6379.
 * They are automatically skipped when Redis is not available.
 */

import { describe, it, expect, beforeEach, afterAll } from 'vitest';
import { ParityHarness } from './parity-harness.ts';
import { canConnect } from './resp-client.ts';

let harness: ParityHarness | null = null;
let redisAvailable = false;

// Check Redis availability once
const redisCheck = canConnect('127.0.0.1', 6379);

/** Get harness, throwing if not available (for use inside skipIf blocks). */
function h(): ParityHarness {
  if (!harness) throw new Error('Harness not initialized');
  return harness;
}

describe('ParityHarness', () => {
  beforeEach(async () => {
    redisAvailable = await redisCheck;
    if (!redisAvailable) return;

    harness = await ParityHarness.create();
    if (harness) {
      await harness.flush();
    }
  });

  afterAll(async () => {
    if (harness) {
      await harness.teardown();
      harness = null;
    }
  });

  // ========================================================================
  // Harness setup
  // ========================================================================

  it('returns null when Redis is not available', async () => {
    const result = await ParityHarness.create({
      redisPort: 1, // no Redis here
    });
    expect(result).toBeNull();
  });

  it.skipIf(!redisAvailable)('creates harness when Redis is available', () => {
    expect(harness).not.toBeNull();
    expect(h().box.connected).toBe(true);
    expect(h().redis.connected).toBe(true);
  });

  // ========================================================================
  // compareCommand — deterministic commands
  // ========================================================================

  describe.skipIf(!redisAvailable)('compareCommand', () => {
    afterAll(async () => {
      if (harness) {
        await harness.teardown();
        harness = null;
      }
    });

    it('compares PING', async () => {
      const result = await h().compareCommand('PING');
      expect(result.box).toEqual({ type: 'simple', value: 'PONG' });
    });

    it('compares SET and GET', async () => {
      await h().compareCommand('SET', 'k1', 'hello');
      await h().compareCommand('GET', 'k1');
    });

    it('compares GET on nonexistent key', async () => {
      await h().compareCommand('GET', 'no-such-key');
    });

    it('compares integer replies (INCR)', async () => {
      await h().compareCommand('SET', 'cnt', '0');
      await h().compareCommand('INCR', 'cnt');
      await h().compareCommand('INCR', 'cnt');
      await h().compareCommand('GET', 'cnt');
    });

    it('compares error replies (wrong type)', async () => {
      await h().compareCommand('SET', 'str', 'value');
      await h().compareCommand('LPUSH', 'str', 'item');
    });

    it('compares error replies (wrong arity)', async () => {
      await h().compareCommand('SET', 'k');
    });

    it('compares list operations', async () => {
      await h().compareCommand('RPUSH', 'mylist', 'a', 'b', 'c');
      await h().compareCommand('LLEN', 'mylist');
      await h().compareCommand('LRANGE', 'mylist', '0', '-1');
      await h().compareCommand('LPOP', 'mylist');
      await h().compareCommand('LRANGE', 'mylist', '0', '-1');
    });

    it('compares hash operations', async () => {
      await h().compareCommand('HSET', 'h', 'f1', 'v1', 'f2', 'v2');
      await h().compareCommand('HGET', 'h', 'f1');
      await h().compareCommand('HLEN', 'h');
      await h().compareCommand('HEXISTS', 'h', 'f1');
      await h().compareCommand('HEXISTS', 'h', 'f3');
    });

    it('compares sorted set operations', async () => {
      await h().compareCommand(
        'ZADD',
        'zs',
        '1.5',
        'a',
        '2.5',
        'b',
        '3.5',
        'c'
      );
      await h().compareCommand('ZCARD', 'zs');
      await h().compareCommand('ZRANGEBYSCORE', 'zs', '1', '3', 'WITHSCORES');
      await h().compareCommand('ZSCORE', 'zs', 'b');
    });

    it('compares APPEND', async () => {
      await h().compareCommand('APPEND', 'app', 'hello');
      await h().compareCommand('APPEND', 'app', ' world');
      await h().compareCommand('GET', 'app');
    });

    it('compares STRLEN', async () => {
      await h().compareCommand('SET', 'sk', 'test');
      await h().compareCommand('STRLEN', 'sk');
      await h().compareCommand('STRLEN', 'missing');
    });

    it('compares EXISTS', async () => {
      await h().compareCommand('SET', 'ek', 'val');
      await h().compareCommand('EXISTS', 'ek');
      await h().compareCommand('EXISTS', 'nope');
    });

    it('compares DEL', async () => {
      await h().compareCommand('SET', 'dk1', 'v');
      await h().compareCommand('SET', 'dk2', 'v');
      await h().compareCommand('DEL', 'dk1', 'dk2', 'dk3');
    });

    it('compares TYPE', async () => {
      await h().compareCommand('SET', 'ts', 'v');
      await h().compareCommand('RPUSH', 'tl', 'a');
      await h().compareCommand('TYPE', 'ts');
      await h().compareCommand('TYPE', 'tl');
      await h().compareCommand('TYPE', 'missing');
    });

    it('compares SETEX/TTL', async () => {
      await h().compareCommand('SETEX', 'ttlkey', '100', 'v');
      await h().compareCommand('TTL', 'ttlkey');
    });

    it('compares MSET/MGET', async () => {
      await h().compareCommand('MSET', 'mk1', 'v1', 'mk2', 'v2', 'mk3', 'v3');
      await h().compareCommand('MGET', 'mk1', 'mk2', 'mk3', 'missing');
    });
  });

  // ========================================================================
  // compareUnordered — set-like commands
  // ========================================================================

  describe.skipIf(!redisAvailable)('compareUnordered', () => {
    afterAll(async () => {
      if (harness) {
        await harness.teardown();
        harness = null;
      }
    });

    it('compares SMEMBERS', async () => {
      await h().compareCommand('SADD', 'myset', 'a', 'b', 'c', 'd');
      await h().compareUnordered('SMEMBERS', 'myset');
    });

    it('compares KEYS', async () => {
      await h().compareCommand('SET', 'pkey:1', 'v');
      await h().compareCommand('SET', 'pkey:2', 'v');
      await h().compareCommand('SET', 'pkey:3', 'v');
      await h().compareUnordered('KEYS', 'pkey:*');
    });

    it('compares HKEYS', async () => {
      await h().compareCommand('HSET', 'uhash', 'b', '2', 'a', '1', 'c', '3');
      await h().compareUnordered('HKEYS', 'uhash');
    });

    it('compares HVALS', async () => {
      await h().compareCommand('HSET', 'uhash2', 'f1', 'v1', 'f2', 'v2');
      await h().compareUnordered('HVALS', 'uhash2');
    });

    it('compares SUNION', async () => {
      await h().compareCommand('SADD', 'su1', 'a', 'b', 'c');
      await h().compareCommand('SADD', 'su2', 'b', 'c', 'd');
      await h().compareUnordered('SUNION', 'su1', 'su2');
    });

    it('compares SINTER', async () => {
      await h().compareCommand('SADD', 'si1', 'a', 'b', 'c');
      await h().compareCommand('SADD', 'si2', 'b', 'c', 'd');
      await h().compareUnordered('SINTER', 'si1', 'si2');
    });

    it('compares SDIFF', async () => {
      await h().compareCommand('SADD', 'sd1', 'a', 'b', 'c');
      await h().compareCommand('SADD', 'sd2', 'b', 'c', 'd');
      await h().compareUnordered('SDIFF', 'sd1', 'sd2');
    });
  });

  // ========================================================================
  // compareStructure — non-deterministic commands
  // ========================================================================

  describe.skipIf(!redisAvailable)('compareStructure', () => {
    afterAll(async () => {
      if (harness) {
        await harness.teardown();
        harness = null;
      }
    });

    it('compares RANDOMKEY type (bulk string or null)', async () => {
      // Empty database: both should return null
      await h().compareCommand('RANDOMKEY');

      // With keys: both should return a bulk string
      await h().compareCommand('SET', 'rk1', 'v');
      await h().compareCommand('SET', 'rk2', 'v');
      await h().compareStructure('RANDOMKEY');
    });

    it('compares SRANDMEMBER structure with negative count', async () => {
      await h().compareCommand('SADD', 'rset', 'a', 'b', 'c');
      // Negative count: may have duplicates, array of 5 elements
      await h().compareStructure('SRANDMEMBER', 'rset', '-5');
    });
  });

  // ========================================================================
  // compareSideEffects
  // ========================================================================

  describe.skipIf(!redisAvailable)('compareSideEffects', () => {
    afterAll(async () => {
      if (harness) {
        await harness.teardown();
        harness = null;
      }
    });

    it('compares side effects for string key', async () => {
      await h().compareCommand('SET', 'se1', 'hello');
      await h().compareSideEffects('se1');
    });

    it('compares side effects for missing key', async () => {
      await h().compareSideEffects('nonexistent');
    });

    it('compares side effects for list key', async () => {
      await h().compareCommand('RPUSH', 'sel', 'a', 'b', 'c');
      await h().compareSideEffects('sel');
    });

    it('compares side effects for hash key', async () => {
      await h().compareCommand('HSET', 'seh', 'f1', 'v1');
      await h().compareSideEffects('seh');
    });

    it('compares side effects for set key', async () => {
      await h().compareCommand('SADD', 'ses', 'a', 'b');
      await h().compareSideEffects('ses');
    });

    it('compares side effects for sorted set key', async () => {
      await h().compareCommand('ZADD', 'sez', '1', 'a', '2', 'b');
      await h().compareSideEffects('sez');
    });

    it('compares side effects after TTL set', async () => {
      await h().compareCommand('SET', 'se_ttl', 'val');
      await h().compareCommand('EXPIRE', 'se_ttl', '1000');
      const result = await h().compareSideEffects('se_ttl');
      // TTL should be > 0 on both
      if (result.box.ttl.type === 'integer') {
        expect(Number(result.box.ttl.value)).toBeGreaterThan(0);
      }
    });

    it('compares side effects after DEL', async () => {
      await h().compareCommand('SET', 'se_del', 'val');
      await h().compareCommand('DEL', 'se_del');
      await h().compareSideEffects('se_del');
    });
  });

  // ========================================================================
  // flush
  // ========================================================================

  describe.skipIf(!redisAvailable)('flush', () => {
    afterAll(async () => {
      if (harness) {
        await harness.teardown();
        harness = null;
      }
    });

    it('clears both databases', async () => {
      await h().compareCommand('SET', 'fk', 'fv');
      await h().flush();
      await h().compareCommand('DBSIZE');
    });
  });
});
