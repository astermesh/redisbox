/**
 * Memory eviction manager for RedisBox.
 *
 * Implements all 8 Redis eviction policies. Before executing write commands
 * (those with the `denyoom` flag), the dispatcher calls `tryEvict()` to
 * ensure memory is within the configured `maxmemory` limit.
 */

import type { RedisEngine } from './engine.ts';
import type { ConfigStore } from '../config-store.ts';
import type { Database } from './database.ts';
import type { Reply } from './types.ts';
import { errorReply } from './types.ts';
import { parseMemorySize } from './memory.ts';

export type EvictionPolicy =
  | 'noeviction'
  | 'allkeys-lru'
  | 'volatile-lru'
  | 'allkeys-lfu'
  | 'volatile-lfu'
  | 'allkeys-random'
  | 'volatile-random'
  | 'volatile-ttl';

const OOM_REPLY: Reply = errorReply(
  'OOM',
  "command not allowed when used memory > 'maxmemory'."
);

/**
 * Maximum number of eviction loop iterations to prevent infinite loops.
 * In Redis this is bounded by the number of keys; we use a generous limit.
 */
const MAX_EVICTION_ITERATIONS = 10000;

export class EvictionManager {
  private readonly engine: RedisEngine;
  private readonly config: ConfigStore;

  constructor(engine: RedisEngine, config: ConfigStore) {
    this.engine = engine;
    this.config = config;
  }

  /**
   * Return the OOM error reply matching real Redis.
   */
  oomReply(): Reply {
    return OOM_REPLY;
  }

  /**
   * Get current used memory from the engine.
   */
  currentUsedMemory(): number {
    return this.engine.usedMemory();
  }

  /**
   * Check if memory is within limits. If over the limit, attempt eviction.
   * Returns true if the command can proceed, false if OOM.
   */
  tryEvict(): boolean {
    const maxmemory = this.getMaxmemory();
    if (maxmemory <= 0) return true; // unlimited

    if (this.currentUsedMemory() <= maxmemory) return true;

    const policy = this.getPolicy();
    if (policy === 'noeviction') return false;

    return this.performEviction(maxmemory, policy);
  }

  private getMaxmemory(): number {
    const result = this.config.get('maxmemory');
    const raw = result[1] ?? '0';
    return parseMemorySize(raw);
  }

  private getPolicy(): EvictionPolicy {
    const result = this.config.get('maxmemory-policy');
    return (result[1] ?? 'noeviction') as EvictionPolicy;
  }

  private getSampleCount(): number {
    const result = this.config.get('maxmemory-samples');
    const n = parseInt(result[1] ?? '5', 10);
    return isNaN(n) || n < 1 ? 5 : n;
  }

  /**
   * Run the eviction loop for the given policy.
   * Returns true if memory was brought below the limit, false otherwise.
   */
  private performEviction(maxmemory: number, policy: EvictionPolicy): boolean {
    const samples = this.getSampleCount();
    let iterations = 0;

    while (
      this.currentUsedMemory() > maxmemory &&
      iterations < MAX_EVICTION_ITERATIONS
    ) {
      iterations++;
      const evicted = this.evictOne(policy, samples);
      if (!evicted) break; // no more keys to evict
    }

    return this.currentUsedMemory() <= maxmemory;
  }

  /**
   * Evict a single key according to the given policy.
   * Returns true if a key was evicted.
   */
  private evictOne(policy: EvictionPolicy, samples: number): boolean {
    switch (policy) {
      case 'allkeys-random':
        return this.evictRandom(false);
      case 'volatile-random':
        return this.evictRandom(true);
      case 'allkeys-lru':
        return this.evictByLru(false, samples);
      case 'volatile-lru':
        return this.evictByLru(true, samples);
      case 'allkeys-lfu':
        return this.evictByLfu(false, samples);
      case 'volatile-lfu':
        return this.evictByLfu(true, samples);
      case 'volatile-ttl':
        return this.evictByTtl(samples);
      default:
        return false;
    }
  }

  /**
   * Evict a random key. If volatileOnly, only pick from keys with expiry.
   * Samples across all databases and picks one randomly, matching Redis behavior
   * of not biasing toward lower-indexed databases.
   */
  private evictRandom(volatileOnly: boolean): boolean {
    const candidates: { db: Database; key: string }[] = [];
    for (const db of this.engine.databases) {
      const keys = volatileOnly
        ? db.sampleVolatileKeys(1, this.engine.rng)
        : db.sampleKeys(1, this.engine.rng);
      const key = keys[0];
      if (key !== undefined) {
        candidates.push({ db, key });
      }
    }
    if (candidates.length === 0) return false;
    const idx = Math.floor(this.engine.rng() * candidates.length);
    const picked = candidates[idx];
    if (!picked) return false;
    picked.db.delete(picked.key);
    return true;
  }

  /**
   * Sample keys and evict the one with the oldest access time (smallest lruClock).
   */
  private evictByLru(volatileOnly: boolean, samples: number): boolean {
    let bestKey: string | null = null;
    let bestDb: Database | null = null;
    let bestClock = Infinity;

    for (const db of this.engine.databases) {
      const keys = volatileOnly
        ? db.sampleVolatileKeys(samples, this.engine.rng)
        : db.sampleKeys(samples, this.engine.rng);

      for (const key of keys) {
        const entry = db.getRaw(key);
        if (!entry) continue;
        if (entry.lruClock < bestClock) {
          bestClock = entry.lruClock;
          bestKey = key;
          bestDb = db;
        }
      }
    }

    if (bestKey && bestDb) {
      bestDb.delete(bestKey);
      return true;
    }
    return false;
  }

  /**
   * Sample keys and evict the one with the lowest frequency counter (smallest lruFreq).
   * Ties are broken by LRU clock (oldest access wins).
   */
  private evictByLfu(volatileOnly: boolean, samples: number): boolean {
    let bestKey: string | null = null;
    let bestDb: Database | null = null;
    let bestFreq = Infinity;
    let bestClock = Infinity;

    for (const db of this.engine.databases) {
      const keys = volatileOnly
        ? db.sampleVolatileKeys(samples, this.engine.rng)
        : db.sampleKeys(samples, this.engine.rng);

      for (const key of keys) {
        const entry = db.getRaw(key);
        if (!entry) continue;
        if (
          entry.lruFreq < bestFreq ||
          (entry.lruFreq === bestFreq && entry.lruClock < bestClock)
        ) {
          bestFreq = entry.lruFreq;
          bestClock = entry.lruClock;
          bestKey = key;
          bestDb = db;
        }
      }
    }

    if (bestKey && bestDb) {
      bestDb.delete(bestKey);
      return true;
    }
    return false;
  }

  /**
   * Sample volatile keys and evict the one with the earliest expiry time
   * (closest to expiring / smallest TTL remaining).
   */
  private evictByTtl(samples: number): boolean {
    let bestKey: string | null = null;
    let bestDb: Database | null = null;
    let bestExpiry = Infinity;

    for (const db of this.engine.databases) {
      const keys = db.sampleVolatileKeys(samples, this.engine.rng);

      for (const key of keys) {
        const expiry = db.getExpiry(key);
        if (expiry === undefined) continue;
        if (expiry < bestExpiry) {
          bestExpiry = expiry;
          bestKey = key;
          bestDb = db;
        }
      }
    }

    if (bestKey && bestDb) {
      bestDb.delete(bestKey);
      return true;
    }
    return false;
  }
}
