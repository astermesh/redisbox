import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import { errorReply, WRONGTYPE_ERR, NOT_INTEGER_ERR } from '../../types.ts';

// --- Error constants ---

export const BIT_OFFSET_ERR = errorReply(
  'ERR',
  'bit offset is not an integer or out of range'
);

export const BIT_VALUE_ERR = errorReply(
  'ERR',
  'bit is not an integer or out of range'
);

export const BIT_ARG_ERR = errorReply(
  'ERR',
  'The bit argument must be 1 or 0.'
);

export const BITOP_NOT_ERR = errorReply(
  'ERR',
  'BITOP NOT must be called with a single source key.'
);

// --- Binary-safe string <-> bytes conversion ---
// Uses Latin-1 style mapping: each character <-> one byte (0-255).
// This is necessary for bitmap operations that produce arbitrary byte values
// (e.g., 0x80) which are not valid single-byte UTF-8.

export function stringToBytes(s: string): Uint8Array {
  const bytes = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) {
    bytes[i] = s.charCodeAt(i) & 0xff;
  }
  return bytes;
}

export function bytesToString(bytes: Uint8Array): string {
  const chunks: string[] = [];
  const chunkSize = 8192;
  for (let i = 0; i < bytes.length; i += chunkSize) {
    const end = Math.min(i + chunkSize, bytes.length);
    const slice = bytes.subarray(i, end);
    chunks.push(String.fromCharCode(...slice));
  }
  return chunks.join('');
}

// --- Helpers ---

/** Read a single byte from a Uint8Array, returning 0 if out of bounds. */
export function byteAt(bytes: Uint8Array, idx: number): number {
  return idx < bytes.length ? (bytes[idx] ?? 0) : 0;
}

export function getStringBytes(
  db: Database,
  key: string
): { bytes: Uint8Array | null; error: Reply | null } {
  const entry = db.get(key);
  if (!entry) return { bytes: null, error: null };
  if (entry.type !== 'string') return { bytes: null, error: WRONGTYPE_ERR };
  return { bytes: stringToBytes(entry.value as string), error: null };
}

export function setStringFromBytes(
  db: Database,
  key: string,
  bytes: Uint8Array
): void {
  db.set(key, 'string', 'raw', bytesToString(bytes));
}

export function parseBitOffset(s: string): {
  value: number;
  error: Reply | null;
} {
  const n = Number(s);
  if (!Number.isInteger(n) || n < 0 || n > 4294967295) {
    return { value: 0, error: BIT_OFFSET_ERR };
  }
  return { value: n, error: null };
}

export function parseIntStrict(s: string): {
  value: number;
  error: Reply | null;
} {
  const val = parseInt(s, 10);
  if (isNaN(val) || String(val) !== s) {
    return { value: 0, error: NOT_INTEGER_ERR };
  }
  return { value: val, error: null };
}

// Population count (number of set bits) for a byte
export const POPCOUNT_TABLE = new Uint8Array(256);
for (let i = 0; i < 256; i++) {
  let count = 0;
  let n = i;
  while (n) {
    count += n & 1;
    n >>= 1;
  }
  POPCOUNT_TABLE[i] = count;
}

export function getBit(bytes: Uint8Array, bitOffset: number): number {
  const byteIdx = bitOffset >> 3;
  const bitIdx = 7 - (bitOffset & 7);
  return (byteAt(bytes, byteIdx) >> bitIdx) & 1;
}
