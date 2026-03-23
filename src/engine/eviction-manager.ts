/**
 * Memory eviction manager for RedisBox.
 *
 * Implements all 8 Redis eviction policies. Before executing write commands
 * (those with the `denyoom` flag), the dispatcher calls `tryEvict()` to
 * ensure memory is within the configured `maxmemory` limit.
 *
 * LRU eviction uses Redis's approximated LRU algorithm:
 * - Each key stores a 24-bit last-access timestamp (seconds resolution)
 * - On eviction, sample `maxmemory-samples` random keys per database
 * - Maintain an eviction pool (sorted array of 16 candidates by idle time)
 * - Evict the candidate with the highest idle time from the pool
 */

import type { RedisEngine } from './engine.ts';
import type { ConfigStore } from '../config-store.ts';
import type { Database } from './database.ts';
import type { Reply } from './types.ts';
import { errorReply } from './types.ts';
import { parseMemorySize } from './memory.ts';
import { getLruClock, estimateIdleTime } from './lru.ts';
import { lfuGetTimeInMinutes, lfuDecrAndReturn } from './lfu.ts';
import { notifyKeyspaceEvent, EVENT_FLAGS } from './keyspace-events.ts';

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

/** Size of the eviction pool, matching Redis EVPOOL_SIZE. */
const EVPOOL_SIZE = 16;

interface EvictionPoolEntry {
  /** Idle time in milliseconds. */
  idle: number;
  /** Key name. */
  key: string;
  /** Database index. */
  dbIndex: number;
}

export class EvictionManager {
  private readonly engine: RedisEngine;
  private readonly config: ConfigStore;

  /**
   * Eviction pool: sorted by idle time ascending (lowest idle first).
   * The candidate with the highest idle time is at the end.
   * Persists across eviction cycles for better approximation.
   */
  private readonly lruPool: EvictionPoolEntry[] = [];

  constructor(engine: RedisEngine, config: ConfigStore) {
    this.engine = engine;
    this.config = config;
    for (const db of engine.databases) {
      db.setConfig(config);
    }
  }

  private notifyEvicted(key: string, dbIndex: number): void {
    notifyKeyspaceEvent(
      this.config,
      this.engine.pubsub,
      EVENT_FLAGS.EVICTED,
      'evicted',
      key,
      dbIndex
    );
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
    const candidates: { db: Database; key: string; dbIdx: number }[] = [];
    for (let dbIdx = 0; dbIdx < this.engine.databases.length; dbIdx++) {
      const db = this.engine.databases[dbIdx];
      if (!db) continue;
      const keys = volatileOnly
        ? db.sampleVolatileKeys(1, this.engine.rng)
        : db.sampleKeys(1, this.engine.rng);
      const key = keys[0];
      if (key !== undefined) {
        candidates.push({ db, key, dbIdx });
      }
    }
    if (candidates.length === 0) return false;
    const idx = Math.floor(this.engine.rng() * candidates.length);
    const picked = candidates[idx];
    if (!picked) return false;
    picked.db.delete(picked.key);
    this.notifyEvicted(picked.key, picked.dbIdx);
    return true;
  }

  /**
   * Populate the eviction pool with sampled keys, then evict the candidate
   * with the highest idle time. The pool persists across eviction cycles
   * for better LRU approximation.
   *
   * This matches Redis's `evictionPoolPopulate()` + pool-based eviction.
   */
  private evictByLru(volatileOnly: boolean, samples: number): boolean {
    const currentClock = getLruClock(this.engine.clock());

    // Populate pool from all databases
    for (let dbIdx = 0; dbIdx < this.engine.databases.length; dbIdx++) {
      const db = this.engine.databases[dbIdx];
      if (!db) continue;
      const keys = volatileOnly
        ? db.sampleVolatileKeys(samples, this.engine.rng)
        : db.sampleKeys(samples, this.engine.rng);

      for (const key of keys) {
        const entry = db.getRaw(key);
        if (!entry) continue;

        const idle = estimateIdleTime(currentClock, entry.lruClock);
        this.insertIntoPool(idle, key, dbIdx);
      }
    }

    // Evict from pool: scan from the end (highest idle) to find a valid key
    return this.evictFromPool();
  }

  /**
   * Insert a candidate into the eviction pool.
   * The pool is sorted by idle time ascending (lowest first).
   * If the pool is full and the candidate's idle time is less than or equal
   * to the minimum in the pool, it is not inserted.
   */
  private insertIntoPool(idle: number, key: string, dbIndex: number): void {
    // Skip if pool is full and this candidate is not more idle than the minimum
    const poolMin = this.lruPool[0];
    if (this.lruPool.length >= EVPOOL_SIZE && poolMin && poolMin.idle >= idle) {
      return;
    }

    // Check if this key is already in the pool — if so, update its idle time
    for (const existing of this.lruPool) {
      if (existing.key === key && existing.dbIndex === dbIndex) {
        existing.idle = idle;
        this.lruPool.sort((a, b) => a.idle - b.idle);
        return;
      }
    }

    // Find insertion point (binary search for sorted insert)
    let lo = 0;
    let hi = this.lruPool.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      const midEntry = this.lruPool[mid];
      if (midEntry && midEntry.idle < idle) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }

    this.lruPool.splice(lo, 0, { idle, key, dbIndex });

    // Trim pool to max size by removing the entry with lowest idle time
    if (this.lruPool.length > EVPOOL_SIZE) {
      this.lruPool.shift();
    }
  }

  /**
   * Evict the best candidate from the pool (highest idle time, at the end).
   * Scans from the end to find a key that still exists.
   * Returns true if a key was evicted.
   */
  private evictFromPool(): boolean {
    while (this.lruPool.length > 0) {
      const candidate = this.lruPool.pop();
      if (!candidate) break;
      const db = this.engine.databases[candidate.dbIndex];
      if (!db) continue;

      // Verify the key still exists (it may have been deleted since pool population)
      if (db.getRaw(candidate.key)) {
        db.delete(candidate.key);
        this.notifyEvicted(candidate.key, candidate.dbIndex);
        return true;
      }
    }
    return false;
  }

  /**
   * Sample keys and evict using approximated LFU with the eviction pool.
   * Matches Redis: scores each key as `255 - LFUDecrAndReturn(o)` and
   * uses the same pool mechanism as LRU eviction.
   */
  private evictByLfu(volatileOnly: boolean, samples: number): boolean {
    const nowMinutes = lfuGetTimeInMinutes(this.engine.clock());
    const decayTime = this.getLfuDecayTime();

    for (let dbIdx = 0; dbIdx < this.engine.databases.length; dbIdx++) {
      const db = this.engine.databases[dbIdx];
      if (!db) continue;
      const keys = volatileOnly
        ? db.sampleVolatileKeys(samples, this.engine.rng)
        : db.sampleKeys(samples, this.engine.rng);

      for (const key of keys) {
        const entry = db.getRaw(key);
        if (!entry) continue;

        const counter = lfuDecrAndReturn(
          entry.lruFreq,
          entry.lfuLastDecrTime,
          nowMinutes,
          decayTime
        );
        const idle = 255 - counter;
        this.insertIntoPool(idle, key, dbIdx);
      }
    }

    return this.evictFromPool();
  }

  private getLfuDecayTime(): number {
    const result = this.config.get('lfu-decay-time');
    const n = parseInt(result[1] ?? '1', 10);
    return isNaN(n) ? 1 : n;
  }

  /**
   * Sample volatile keys and evict the one with the earliest expiry time
   * (closest to expiring / smallest TTL remaining).
   */
  private evictByTtl(samples: number): boolean {
    let bestKey: string | null = null;
    let bestDb: Database | null = null;
    let bestDbIdx = 0;
    let bestExpiry = Infinity;

    for (let dbIdx = 0; dbIdx < this.engine.databases.length; dbIdx++) {
      const db = this.engine.databases[dbIdx];
      if (!db) continue;
      const keys = db.sampleVolatileKeys(samples, this.engine.rng);

      for (const key of keys) {
        const expiry = db.getExpiry(key);
        if (expiry === undefined) continue;
        if (expiry < bestExpiry) {
          bestExpiry = expiry;
          bestKey = key;
          bestDb = db;
          bestDbIdx = dbIdx;
        }
      }
    }

    if (bestKey && bestDb) {
      bestDb.delete(bestKey);
      this.notifyEvicted(bestKey, bestDbIdx);
      return true;
    }
    return false;
  }
}
