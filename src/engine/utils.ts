import type { ConfigStore } from '../config-store.ts';

const textEncoder = new TextEncoder();

export function strByteLength(s: string): number {
  return textEncoder.encode(s).length;
}

// Default thresholds — match Redis 7.2 defaults.
export const DEFAULT_MAX_LISTPACK_ENTRIES = 128;
export const DEFAULT_MAX_LISTPACK_VALUE = 64;

/**
 * Read an integer config value from ConfigStore with a fallback default.
 * ConfigStore.get() returns [key, value] for exact matches.
 */
export function configInt(
  config: ConfigStore | undefined,
  key: string,
  fallback: number
): number {
  if (!config) return fallback;
  const result = config.get(key);
  return result.length >= 2 ? Number(result[1]) : fallback;
}

export const INT64_MAX = BigInt('9223372036854775807');
export const INT64_MIN = BigInt('-9223372036854775808');

/**
 * Fisher-Yates partial shuffle: randomly reorders the first `count` elements
 * of the array in-place using the provided RNG.
 * Returns a slice of the first `count` elements.
 */
export function partialShuffle<T>(
  arr: T[],
  count: number,
  rng: () => number
): T[] {
  const n = Math.min(count, arr.length);
  for (let i = 0; i < n; i++) {
    const j = i + Math.floor(rng() * (arr.length - i));
    const tmp = arr[i] as T;
    arr[i] = arr[j] as T;
    arr[j] = tmp;
  }
  return arr.slice(0, n);
}
