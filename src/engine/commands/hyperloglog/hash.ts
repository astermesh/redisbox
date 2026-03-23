import { stringToBytes, bB, HLL_P_MASK, HLL_P, HLL_Q } from './encoding.ts';

// MurmurHash64A seed (matches Redis)
const MURMURHASH_SEED = 0xadc83b19n;

// --- MurmurHash64A (Redis-compatible) ---

export function murmurHash64A(data: Uint8Array): bigint {
  const m = 0xc6a4a7935bd1e995n;
  const r = 47n;
  const len = data.length;
  let h = (MURMURHASH_SEED ^ (BigInt(len) * m)) & 0xffffffffffffffffn;

  const nblocks = Math.floor(len / 8);
  for (let i = 0; i < nblocks; i++) {
    const off = i * 8;
    let k =
      bB(data, off) |
      (bB(data, off + 1) << 8n) |
      (bB(data, off + 2) << 16n) |
      (bB(data, off + 3) << 24n) |
      (bB(data, off + 4) << 32n) |
      (bB(data, off + 5) << 40n) |
      (bB(data, off + 6) << 48n) |
      (bB(data, off + 7) << 56n);

    k = (k * m) & 0xffffffffffffffffn;
    k ^= k >> r;
    k = (k * m) & 0xffffffffffffffffn;

    h ^= k;
    h = (h * m) & 0xffffffffffffffffn;
  }

  const tail = nblocks * 8;
  const remaining = len & 7;
  /* eslint-disable no-fallthrough */
  switch (remaining) {
    case 7:
      h ^= bB(data, tail + 6) << 48n;
    case 6:
      h ^= bB(data, tail + 5) << 40n;
    case 5:
      h ^= bB(data, tail + 4) << 32n;
    case 4:
      h ^= bB(data, tail + 3) << 24n;
    case 3:
      h ^= bB(data, tail + 2) << 16n;
    case 2:
      h ^= bB(data, tail + 1) << 8n;
    case 1:
      h ^= bB(data, tail);
      h = (h * m) & 0xffffffffffffffffn;
  }
  /* eslint-enable no-fallthrough */

  h ^= h >> r;
  h = (h * m) & 0xffffffffffffffffn;
  h ^= h >> r;

  return h;
}

/** Hash an element and return [registerIndex, runLength]. */
export function hllPatLen(element: string): [number, number] {
  // Use Latin-1 raw bytes to match Redis, which hashes raw SDS bytes
  const data = stringToBytes(element);
  const hash = murmurHash64A(data);

  const index = Number(hash & BigInt(HLL_P_MASK));
  let bits = hash >> BigInt(HLL_P);
  // Set sentinel bit at position HLL_Q to guarantee termination
  bits |= 1n << BigInt(HLL_Q);

  let count = 1;
  while ((bits & 1n) === 0n) {
    count++;
    bits >>= 1n;
  }
  return [index, count];
}
