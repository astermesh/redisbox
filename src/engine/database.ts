import type { RedisEntry, RedisEncoding, RedisType } from './types.ts';

export class Database {
  private readonly store = new Map<string, RedisEntry>();
  private readonly expiry = new Map<string, number>();
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
    entry.lruClock = this.clock();
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
    const now = this.clock();
    this.store.set(key, {
      type,
      encoding,
      value,
      lruClock: now,
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
      this.bumpVersion(key);
    }
    return existed;
  }

  rename(src: string, dst: string): void {
    const entry = this.store.get(src);
    if (!entry) return;
    const srcExpiry = this.expiry.get(src);

    if (src === dst) return;

    this.store.delete(src);
    this.expiry.delete(src);
    this.bumpVersion(src);

    this.store.delete(dst);
    this.expiry.delete(dst);

    this.store.set(dst, entry);
    if (srcExpiry !== undefined) {
      this.expiry.set(dst, srcExpiry);
    }
    this.bumpVersion(dst);
  }

  touch(key: string): boolean {
    if (this.expireIfNeeded(key)) return false;
    const entry = this.store.get(key);
    if (!entry) return false;
    entry.lruClock = this.clock();
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
      lruClock: this.clock(),
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
   * Uses Fisher-Yates partial shuffle on an array of expiry keys.
   */
  sampleExpiryKeys(count: number, rng: () => number): string[] {
    const keys = Array.from(this.expiry.keys());
    if (keys.length === 0) return [];
    if (count >= keys.length) return keys;

    // Partial Fisher-Yates: shuffle first `count` positions
    for (let i = 0; i < count; i++) {
      const j = i + Math.floor(rng() * (keys.length - i));
      const tmp = keys[i] as string;
      keys[i] = keys[j] as string;
      keys[j] = tmp;
    }
    return keys.slice(0, count);
  }

  /**
   * Try to expire a single key. Returns true if the key was expired.
   * Public variant for active expiration cycle.
   */
  tryExpire(key: string): boolean {
    return this.expireIfNeeded(key);
  }

  flushExpired(): void {
    for (const key of Array.from(this.expiry.keys())) {
      this.expireIfNeeded(key);
    }
  }

  private expireIfNeeded(key: string): boolean {
    const expiryTime = this.expiry.get(key);
    if (expiryTime === undefined) return false;
    if (this.clock() < expiryTime) return false;

    this.store.delete(key);
    this.expiry.delete(key);
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
