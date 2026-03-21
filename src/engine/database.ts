import type { RedisEntry, RedisEncoding, RedisType } from './types.ts';
import { partialShuffle } from './utils.ts';
import { getLruClock } from './lru.ts';

export class Database {
  private readonly store = new Map<string, RedisEntry>();
  private readonly expiry = new Map<string, number>();
  private readonly fieldExpiry = new Map<string, Map<string, number>>();
  private readonly versions = new Map<string, number>();
  private globalVersion = 0;
  private readonly clock: () => number;

  constructor(clock: () => number) {
    this.clock = clock;
  }

  get(key: string): RedisEntry | null {
    if (this.expireIfNeeded(key)) return null;
    const entry = this.store.get(key);
    if (!entry) return null;
    entry.lruClock = getLruClock(this.clock());
    return entry;
  }

  getWithoutTouch(key: string): RedisEntry | null {
    if (this.expireIfNeeded(key)) return null;
    return this.store.get(key) ?? null;
  }

  has(key: string): boolean {
    if (this.expireIfNeeded(key)) return false;
    return this.store.has(key);
  }

  set(
    key: string,
    type: RedisType,
    encoding: RedisEncoding,
    value: unknown
  ): void {
    this.store.set(key, {
      type,
      encoding,
      value,
      lruClock: getLruClock(this.clock()),
      lruFreq: 0,
    });
    this.bumpVersion(key);
  }

  setEntry(key: string, entry: RedisEntry): void {
    this.store.set(key, entry);
    this.bumpVersion(key);
  }

  delete(key: string): boolean {
    const existed = this.store.delete(key);
    if (existed) {
      this.expiry.delete(key);
      this.fieldExpiry.delete(key);
      this.bumpVersion(key);
    }
    return existed;
  }

  rename(src: string, dst: string): void {
    const entry = this.store.get(src);
    if (!entry) return;
    const srcExpiry = this.expiry.get(src);
    const srcFieldExpiry = this.fieldExpiry.get(src);

    if (src === dst) return;

    this.store.delete(src);
    this.expiry.delete(src);
    this.fieldExpiry.delete(src);
    this.bumpVersion(src);

    this.store.delete(dst);
    this.expiry.delete(dst);
    this.fieldExpiry.delete(dst);

    this.store.set(dst, entry);
    if (srcExpiry !== undefined) {
      this.expiry.set(dst, srcExpiry);
    }
    if (srcFieldExpiry !== undefined) {
      this.fieldExpiry.set(dst, srcFieldExpiry);
    }
    this.bumpVersion(dst);
  }

  touch(key: string): boolean {
    if (this.expireIfNeeded(key)) return false;
    const entry = this.store.get(key);
    if (!entry) return false;
    entry.lruClock = getLruClock(this.clock());
    return true;
  }

  setExpiry(key: string, expiryMs: number): boolean {
    if (!this.store.has(key)) return false;
    this.expiry.set(key, expiryMs);
    this.bumpVersion(key);
    return true;
  }

  getExpiry(key: string): number | undefined {
    return this.expiry.get(key);
  }

  removeExpiry(key: string): boolean {
    if (!this.store.has(key)) return false;
    const had = this.expiry.has(key);
    this.expiry.delete(key);
    if (had) this.bumpVersion(key);
    return had;
  }

  getVersion(key: string): number {
    return this.versions.get(key) ?? 0;
  }

  keys(): IterableIterator<string> {
    return this.store.keys();
  }

  get size(): number {
    return this.store.size;
  }

  flush(): void {
    this.store.clear();
    this.expiry.clear();
    this.fieldExpiry.clear();
    this.versions.clear();
    this.globalVersion = 0;
  }

  randomKey(): string | null {
    if (this.store.size === 0) return null;
    const allKeys = Array.from(this.store.keys());
    const validKeys: string[] = [];
    for (const key of allKeys) {
      if (!this.expireIfNeeded(key)) {
        validKeys.push(key);
      }
    }
    if (validKeys.length === 0) return null;
    const idx = Math.floor(this.getRng() * validKeys.length);
    return validKeys[idx] ?? null;
  }

  private rng: (() => number) | null = null;

  setRng(rng: () => number): void {
    this.rng = rng;
  }

  private getRng(): number {
    return this.rng ? this.rng() : Math.random();
  }

  copyEntry(key: string): RedisEntry | null {
    const entry = this.get(key);
    if (!entry) return null;
    return {
      type: entry.type,
      encoding: entry.encoding,
      value: deepCopyValue(entry.value),
      lruClock: getLruClock(this.clock()),
      lruFreq: 0,
    };
  }

  entriesIterator(): IterableIterator<[string, RedisEntry]> {
    return this.store.entries();
  }

  get expirySize(): number {
    return this.expiry.size;
  }

  /**
   * Sample up to `count` random keys from the expiry index.
   */
  sampleExpiryKeys(count: number, rng: () => number): string[] {
    const keys = Array.from(this.expiry.keys());
    if (keys.length === 0) return [];
    if (count >= keys.length) return keys;
    return partialShuffle(keys, count, rng);
  }

  /**
   * Try to expire a single key. Returns true if the key was expired.
   * Public variant for active expiration cycle.
   */
  tryExpire(key: string): boolean {
    return this.expireIfNeeded(key);
  }

  expiryEntries(): IterableIterator<[string, number]> {
    return this.expiry.entries();
  }

  /**
   * Sample up to `count` random keys from all keys in the store.
   */
  sampleKeys(count: number, rng: () => number): string[] {
    const keys = Array.from(this.store.keys());
    if (keys.length === 0) return [];
    if (count >= keys.length) return keys;
    return partialShuffle(keys, count, rng);
  }

  /**
   * Sample up to `count` random keys that have an expiry set (volatile keys).
   */
  sampleVolatileKeys(count: number, rng: () => number): string[] {
    return this.sampleExpiryKeys(count, rng);
  }

  /**
   * Get the raw entry without expiry check or LRU touch.
   * Used by eviction to inspect entries without side effects.
   */
  getRaw(key: string): RedisEntry | null {
    return this.store.get(key) ?? null;
  }

  flushExpired(): void {
    for (const key of Array.from(this.expiry.keys())) {
      this.expireIfNeeded(key);
    }
  }

  // --- Field-level expiry (Redis 7.4+ hash field TTL) ---

  get fieldExpirySize(): number {
    return this.fieldExpiry.size;
  }

  setFieldExpiry(key: string, field: string, expiryMs: number): boolean {
    const entry = this.store.get(key);
    if (!entry || entry.type !== 'hash') return false;
    const hash = entry.value as Map<string, string>;
    if (!hash.has(field)) return false;

    let fields = this.fieldExpiry.get(key);
    if (!fields) {
      fields = new Map();
      this.fieldExpiry.set(key, fields);
    }
    fields.set(field, expiryMs);
    this.bumpVersion(key);
    return true;
  }

  getFieldExpiry(key: string, field: string): number | undefined {
    return this.fieldExpiry.get(key)?.get(field);
  }

  removeFieldExpiry(key: string, field: string): boolean {
    const fields = this.fieldExpiry.get(key);
    if (!fields) return false;
    const had = fields.delete(field);
    if (had && fields.size === 0) {
      this.fieldExpiry.delete(key);
    }
    return had;
  }

  /**
   * Sample up to `count` random keys from the field expiry index.
   */
  sampleFieldExpiryKeys(count: number, rng: () => number): string[] {
    const keys = Array.from(this.fieldExpiry.keys());
    if (keys.length === 0) return [];
    if (count >= keys.length) return keys;
    return partialShuffle(keys, count, rng);
  }

  /**
   * Sample up to `count` random fields with TTL for a given key.
   */
  sampleFieldsWithExpiry(
    key: string,
    count: number,
    rng: () => number
  ): string[] {
    const fields = this.fieldExpiry.get(key);
    if (!fields || fields.size === 0) return [];

    const fieldNames = Array.from(fields.keys());
    if (count >= fieldNames.length) return fieldNames;
    return partialShuffle(fieldNames, count, rng);
  }

  /**
   * Expire all expired fields for a given hash key.
   * Returns the number of fields expired.
   * If the hash becomes empty after expiration, deletes the key.
   */
  expireHashFields(key: string): number {
    const fields = this.fieldExpiry.get(key);
    if (!fields || fields.size === 0) return 0;

    const now = this.clock();
    const expired: string[] = [];
    for (const [field, expiryTime] of fields) {
      if (now >= expiryTime) {
        expired.push(field);
      }
    }

    if (expired.length === 0) return 0;

    const entry = this.store.get(key);
    if (!entry || entry.type !== 'hash') return 0;
    const hash = entry.value as Map<string, string>;

    for (const field of expired) {
      hash.delete(field);
      fields.delete(field);
    }

    if (fields.size === 0) {
      this.fieldExpiry.delete(key);
    }

    if (hash.size === 0) {
      this.store.delete(key);
      this.expiry.delete(key);
      this.fieldExpiry.delete(key);
    }

    this.bumpVersion(key);
    return expired.length;
  }

  /**
   * Try to expire a single field of a hash. Returns true if the field was expired.
   * If the hash becomes empty after field deletion, deletes the key.
   */
  tryExpireField(key: string, field: string): boolean {
    const fields = this.fieldExpiry.get(key);
    if (!fields) return false;

    const expiryTime = fields.get(field);
    if (expiryTime === undefined) return false;
    if (this.clock() < expiryTime) return false;

    // Field is expired — delete it from the hash
    const entry = this.store.get(key);
    if (!entry || entry.type !== 'hash') return false;

    const hash = entry.value as Map<string, string>;
    hash.delete(field);
    fields.delete(field);

    if (fields.size === 0) {
      this.fieldExpiry.delete(key);
    }

    // If hash is now empty, delete the entire key
    if (hash.size === 0) {
      this.store.delete(key);
      this.expiry.delete(key);
      this.fieldExpiry.delete(key);
    }

    this.bumpVersion(key);
    return true;
  }

  private expireIfNeeded(key: string): boolean {
    const expiryTime = this.expiry.get(key);
    if (expiryTime === undefined) return false;
    if (this.clock() < expiryTime) return false;

    this.store.delete(key);
    this.expiry.delete(key);
    this.fieldExpiry.delete(key);
    this.bumpVersion(key);
    return true;
  }

  private bumpVersion(key: string): void {
    this.globalVersion++;
    this.versions.set(key, this.globalVersion);
  }
}

function deepCopyValue(value: unknown): unknown {
  if (value === null || value === undefined) return value;
  if (typeof value === 'string' || typeof value === 'number') return value;
  if (Array.isArray(value)) return value.map(deepCopyValue);
  if (value instanceof Map) {
    const copy = new Map();
    for (const [k, v] of value) {
      copy.set(k, deepCopyValue(v));
    }
    return copy;
  }
  if (value instanceof Set) {
    const copy = new Set();
    for (const v of value) {
      copy.add(deepCopyValue(v));
    }
    return copy;
  }
  if (typeof value === 'object') {
    const copy: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      copy[k] = deepCopyValue(v);
    }
    return copy;
  }
  return value;
}
