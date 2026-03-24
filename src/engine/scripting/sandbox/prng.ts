/**
 * Redis-compatible PRNG (redisLrand48) matching Redis rand.c.
 *
 * Uses a 48-bit linear congruential generator with the same constants
 * as the POSIX drand48/lrand48 family. Redis uses this for deterministic
 * replication of math.random() in Lua scripts.
 */

const LRAND48_A = 0x5deece66dn;
const LRAND48_C = 0xbn;
const LRAND48_M = 1n << 48n;

/** 48-bit PRNG state */
let prngState = 0n;

/** REDIS_LRAND48_MAX = INT32_MAX = 2^31 - 1 */
export const LRAND48_MAX = 2147483647;

/**
 * Set PRNG state from a seed value, matching Redis srand48 behavior.
 * Xi = {0x330E, seed_low16, seed_high16}
 */
export function srand48(seed: number): void {
  const s = seed & 0xffffffff;
  const lo = s & 0xffff;
  const hi = (s >>> 16) & 0xffff;
  prngState = (BigInt(hi) << 32n) | (BigInt(lo) << 16n) | 0x330en;
}

/**
 * Advance PRNG and return upper 31 bits (matching Redis lrand48).
 */
export function lrand48(): number {
  prngState = (LRAND48_A * prngState + LRAND48_C) % LRAND48_M;
  return Number(prngState >> 17n);
}

/**
 * Reset PRNG to Redis per-EVAL state: srand48(0).
 * Redis calls redisSrand48(0) before every EVAL for deterministic replication.
 */
export function resetPrng(): void {
  srand48(0);
}
