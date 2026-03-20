const textEncoder = new TextEncoder();

export function strByteLength(s: string): number {
  return textEncoder.encode(s).length;
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
  rng: () => number,
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
