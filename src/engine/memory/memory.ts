/**
 * Memory usage estimation for RedisBox.
 *
 * Models Redis's internal memory accounting:
 * - dictEntry overhead per key
 * - SDS string allocation with jemalloc bin rounding
 * - robj (RedisObject) header
 * - Per-type value estimation
 */

import type { RedisEntry } from '../types.ts';

// ---------------------------------------------------------------------------
// jemalloc allocation bin sizes (64-bit)
// ---------------------------------------------------------------------------

const JEMALLOC_BINS = [
  8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384,
  448, 512, 640, 768, 896, 1024, 1280, 1536, 1792, 2048, 2560, 3072, 3584, 4096,
];

/**
 * Round up to nearest jemalloc allocation bin size.
 */
export function jemallocSize(size: number): number {
  for (const bin of JEMALLOC_BINS) {
    if (size <= bin) return bin;
  }
  // Large allocations: round up to 4096-byte pages
  return Math.ceil(size / 4096) * 4096;
}

// ---------------------------------------------------------------------------
// SDS (Simple Dynamic String) overhead — mirrors Redis SDS
// ---------------------------------------------------------------------------

function sdsHdrSize(len: number): number {
  if (len < 256) return 3; // sdshdr8: 1 len + 1 alloc + 1 flags
  if (len < 65536) return 5; // sdshdr16
  return 9; // sdshdr32
}

/**
 * Allocated size for an SDS string of given byte length.
 */
export function sdsAllocSize(len: number): number {
  return jemallocSize(sdsHdrSize(len) + len + 1);
}

// ---------------------------------------------------------------------------
// Redis constants (64-bit platform)
// ---------------------------------------------------------------------------

/** dictEntry: 3 pointers (key, val, next) + metadata = 24 bytes raw, jemalloc → 32 */
const DICT_ENTRY_SIZE = 56;

/** robj: type(4) + encoding(4) + lru(24) + refcount(4) + ptr(8) = 16 bytes */
const ROBJ_SIZE = 16;

/** dict structure overhead: 2 hash tables + rehash state */
const DICT_OVERHEAD = 128;

/** Expiry entry overhead in the expires dict */
const EXPIRY_OVERHEAD = DICT_ENTRY_SIZE + 8;

// ---------------------------------------------------------------------------
// Per-type value memory estimation
// ---------------------------------------------------------------------------

function estimateStringValue(entry: RedisEntry): number {
  const str = entry.value as string;
  const len = Buffer.byteLength(str, 'utf8');

  switch (entry.encoding) {
    case 'int':
      // Integer is stored directly in the robj pointer — no extra allocation
      return 0;
    case 'embstr':
      // Embedded string: robj + sds header + string + null in one allocation
      return jemallocSize(ROBJ_SIZE + sdsHdrSize(len) + len + 1) - ROBJ_SIZE;
    case 'raw':
    default:
      return sdsAllocSize(len);
  }
}

function estimateHashValue(entry: RedisEntry): number {
  const hash = entry.value as Map<string, string>;

  if (entry.encoding === 'listpack' || entry.encoding === 'ziplist') {
    // Listpack: compact encoding — estimate as sum of field+value byte lengths
    // plus ~11 bytes header + 1 byte per entry overhead + 1 end byte
    let dataSize = 11;
    for (const [field, value] of hash) {
      dataSize +=
        1 +
        Buffer.byteLength(field, 'utf8') +
        1 +
        Buffer.byteLength(value, 'utf8');
    }
    return jemallocSize(dataSize);
  }

  // hashtable encoding: dict + entries
  let size = DICT_OVERHEAD;
  // Hash table buckets (next power of 2 >= size)
  const buckets = nextPowerOf2(Math.max(hash.size, 4));
  size += jemallocSize(buckets * 8);

  for (const [field, value] of hash) {
    size += DICT_ENTRY_SIZE;
    size += sdsAllocSize(Buffer.byteLength(field, 'utf8'));
    size += sdsAllocSize(Buffer.byteLength(value, 'utf8'));
  }

  return size;
}

function estimateListValue(entry: RedisEntry): number {
  const list = entry.value as string[];

  if (entry.encoding === 'listpack' || entry.encoding === 'ziplist') {
    let dataSize = 11;
    for (const item of list) {
      dataSize += 1 + Buffer.byteLength(item, 'utf8');
    }
    return jemallocSize(dataSize);
  }

  // quicklist: quicklist struct + nodes + entries
  let size = 48; // quicklist struct
  // Simplified: treat as one quicklist node with all entries
  const nodeOverhead = 80; // quicklistNode + quicklistLZF headers
  let dataSize = 11;
  for (const item of list) {
    dataSize += 1 + Buffer.byteLength(item, 'utf8');
  }
  size += nodeOverhead + jemallocSize(dataSize);
  return size;
}

function estimateSetValue(entry: RedisEntry): number {
  const set = entry.value as Set<string>;

  if (entry.encoding === 'intset') {
    // intset: header(4+4+4) + n * intsize(usually 2,4, or 8)
    return jemallocSize(12 + set.size * 8);
  }

  if (entry.encoding === 'listpack') {
    let dataSize = 11;
    for (const member of set) {
      dataSize += 1 + Buffer.byteLength(member, 'utf8');
    }
    return jemallocSize(dataSize);
  }

  // hashtable encoding: dict + entries
  let size = DICT_OVERHEAD;
  const buckets = nextPowerOf2(Math.max(set.size, 4));
  size += jemallocSize(buckets * 8);

  for (const member of set) {
    size += DICT_ENTRY_SIZE;
    size += sdsAllocSize(Buffer.byteLength(member, 'utf8'));
  }

  return size;
}

function estimateZSetValue(entry: RedisEntry): number {
  const zsetData = entry.value as { dict: Map<string, number> };
  const dict = zsetData.dict;

  if (entry.encoding === 'listpack' || entry.encoding === 'ziplist') {
    let dataSize = 11;
    for (const [member, score] of dict) {
      dataSize += 1 + Buffer.byteLength(member, 'utf8');
      // Score stored as string in listpack
      dataSize += 1 + String(score).length;
    }
    return jemallocSize(dataSize);
  }

  // skiplist encoding: dict + skiplist + nodes
  let size = DICT_OVERHEAD;
  const buckets = nextPowerOf2(Math.max(dict.size, 4));
  size += jemallocSize(buckets * 8);

  // Skiplist header
  size += 32; // zskiplist struct

  for (const [member] of dict) {
    // dictEntry for the dict
    size += DICT_ENTRY_SIZE;
    // SDS for member (shared between dict and skiplist)
    size += sdsAllocSize(Buffer.byteLength(member, 'utf8'));
    // Skiplist node: base + average ~1.33 levels (with p=0.25)
    // zskiplistNode: ele(8) + score(8) + backward(8) + level[] = 24 + 16*levels
    const avgLevels = 2;
    size += jemallocSize(24 + 16 * avgLevels);
  }

  return size;
}

function estimateStreamValue(_entry: RedisEntry): number {
  // Stream type: minimal estimate
  return 128;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Estimate the memory used by a single key-value pair in bytes.
 * Models Redis MEMORY USAGE output.
 */
export function estimateKeyMemory(
  key: string,
  entry: RedisEntry,
  hasExpiry: boolean
): number {
  const keyLen = Buffer.byteLength(key, 'utf8');

  // Base: dictEntry + key SDS + robj
  let size = DICT_ENTRY_SIZE + sdsAllocSize(keyLen) + ROBJ_SIZE;

  // Value overhead depends on type
  switch (entry.type) {
    case 'string':
      size += estimateStringValue(entry);
      break;
    case 'hash':
      size += estimateHashValue(entry);
      break;
    case 'list':
      size += estimateListValue(entry);
      break;
    case 'set':
      size += estimateSetValue(entry);
      break;
    case 'zset':
      size += estimateZSetValue(entry);
      break;
    case 'stream':
      size += estimateStreamValue(entry);
      break;
    default:
      break;
  }

  // Expiry overhead
  if (hasExpiry) {
    size += EXPIRY_OVERHEAD;
  }

  return size;
}

/**
 * Estimate memory used by a single key with sampling for complex types.
 * When samples > 0, only sample that many elements and extrapolate.
 */
export function estimateKeyMemoryWithSamples(
  key: string,
  entry: RedisEntry,
  hasExpiry: boolean,
  samples: number
): number {
  if (samples === 0) {
    return estimateKeyMemory(key, entry, hasExpiry);
  }

  const keyLen = Buffer.byteLength(key, 'utf8');
  let size = DICT_ENTRY_SIZE + sdsAllocSize(keyLen) + ROBJ_SIZE;

  if (hasExpiry) {
    size += EXPIRY_OVERHEAD;
  }

  size += estimateValueWithSamples(entry, samples);
  return size;
}

function estimateValueWithSamples(entry: RedisEntry, samples: number): number {
  switch (entry.type) {
    case 'string':
      return estimateStringValue(entry);
    case 'hash':
      return estimateHashValueSampled(entry, samples);
    case 'list':
      return estimateListValueSampled(entry, samples);
    case 'set':
      return estimateSetValueSampled(entry, samples);
    case 'zset':
      return estimateZSetValueSampled(entry, samples);
    case 'stream':
      return estimateStreamValue(entry);
    default:
      return 0;
  }
}

function estimateHashValueSampled(entry: RedisEntry, samples: number): number {
  const hash = entry.value as Map<string, string>;
  if (hash.size === 0) return 0;

  if (entry.encoding === 'listpack' || entry.encoding === 'ziplist') {
    return estimateHashValue(entry);
  }

  let size = DICT_OVERHEAD;
  const buckets = nextPowerOf2(Math.max(hash.size, 4));
  size += jemallocSize(buckets * 8);

  const count = Math.min(samples, hash.size);
  let sampled = 0;
  let sampleSize = 0;

  for (const [field, value] of hash) {
    if (sampled >= count) break;
    sampleSize += DICT_ENTRY_SIZE;
    sampleSize += sdsAllocSize(Buffer.byteLength(field, 'utf8'));
    sampleSize += sdsAllocSize(Buffer.byteLength(value, 'utf8'));
    sampled++;
  }

  size += Math.round((sampleSize / sampled) * hash.size);
  return size;
}

function estimateListValueSampled(entry: RedisEntry, samples: number): number {
  const list = entry.value as string[];
  if (list.length === 0) return 0;

  if (entry.encoding === 'listpack' || entry.encoding === 'ziplist') {
    return estimateListValue(entry);
  }

  const count = Math.min(samples, list.length);
  let sampleDataSize = 0;

  for (let i = 0; i < count; i++) {
    const item = list[i] ?? '';
    sampleDataSize += 1 + Buffer.byteLength(item, 'utf8');
  }

  const totalDataSize = Math.round((sampleDataSize / count) * list.length);
  return 48 + 80 + jemallocSize(11 + totalDataSize);
}

function estimateSetValueSampled(entry: RedisEntry, samples: number): number {
  const set = entry.value as Set<string>;
  if (set.size === 0) return 0;

  if (entry.encoding === 'intset' || entry.encoding === 'listpack') {
    return estimateSetValue(entry);
  }

  let size = DICT_OVERHEAD;
  const buckets = nextPowerOf2(Math.max(set.size, 4));
  size += jemallocSize(buckets * 8);

  const count = Math.min(samples, set.size);
  let sampled = 0;
  let sampleSize = 0;

  for (const member of set) {
    if (sampled >= count) break;
    sampleSize += DICT_ENTRY_SIZE;
    sampleSize += sdsAllocSize(Buffer.byteLength(member, 'utf8'));
    sampled++;
  }

  size += Math.round((sampleSize / sampled) * set.size);
  return size;
}

function estimateZSetValueSampled(entry: RedisEntry, samples: number): number {
  const zset = entry.value as Map<string, number>;
  if (zset.size === 0) return 0;

  if (entry.encoding === 'listpack' || entry.encoding === 'ziplist') {
    return estimateZSetValue(entry);
  }

  let size = DICT_OVERHEAD;
  const buckets = nextPowerOf2(Math.max(zset.size, 4));
  size += jemallocSize(buckets * 8);
  size += 32; // skiplist header

  const count = Math.min(samples, zset.size);
  let sampled = 0;
  let sampleSize = 0;
  const avgLevels = 2;

  for (const [member] of zset) {
    if (sampled >= count) break;
    sampleSize += DICT_ENTRY_SIZE;
    sampleSize += sdsAllocSize(Buffer.byteLength(member, 'utf8'));
    sampleSize += jemallocSize(24 + 16 * avgLevels);
    sampled++;
  }

  size += Math.round((sampleSize / sampled) * zset.size);
  return size;
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

function nextPowerOf2(n: number): number {
  let v = n - 1;
  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;
  return v + 1;
}

// ---------------------------------------------------------------------------
// Memory size parsing (for maxmemory config)
// ---------------------------------------------------------------------------

/**
 * Parse a memory size string into bytes.
 * Supports: plain number, or number followed by kb/mb/gb (case-insensitive).
 * Returns -1 for invalid input.
 */
export function parseMemorySize(str: string): number {
  const trimmed = str.trim().toLowerCase();
  if (trimmed === '' || trimmed === '0') return 0;

  const match = trimmed.match(/^(\d+)\s*([kmg]b?)?$/);
  if (!match) return -1;

  const num = parseInt(match[1] ?? '', 10);
  if (isNaN(num) || num < 0) return -1;

  const unit = match[2] ?? '';
  switch (unit) {
    case 'k':
    case 'kb':
      return num * 1024;
    case 'm':
    case 'mb':
      return num * 1024 * 1024;
    case 'g':
    case 'gb':
      return num * 1024 * 1024 * 1024;
    case '':
      return num;
    default:
      return -1;
  }
}
