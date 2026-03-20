import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import {
  integerReply,
  arrayReply,
  errorReply,
  ZERO,
  NIL,
  WRONGTYPE_ERR,
  SYNTAX_ERR,
  NOT_INTEGER_ERR,
} from '../types.ts';

// --- Error constants ---

const BIT_OFFSET_ERR = errorReply(
  'ERR',
  'bit offset is not an integer or out of range'
);

const BIT_VALUE_ERR = errorReply(
  'ERR',
  'bit is not an integer or out of range'
);

const BIT_ARG_ERR = errorReply('ERR', 'The bit argument must be 1 or 0.');

const BITOP_NOT_ERR = errorReply(
  'ERR',
  'BITOP NOT requires one and only one key.'
);

const BITFIELD_TYPE_ERR = errorReply(
  'ERR',
  'Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.'
);

// --- Binary-safe string ↔ bytes conversion ---
// Uses Latin-1 style mapping: each character ↔ one byte (0-255).
// This is necessary for bitmap operations that produce arbitrary byte values
// (e.g., 0x80) which are not valid single-byte UTF-8.

function stringToBytes(s: string): Uint8Array {
  const bytes = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) {
    bytes[i] = s.charCodeAt(i) & 0xff;
  }
  return bytes;
}

function bytesToString(bytes: Uint8Array): string {
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
function byteAt(bytes: Uint8Array, idx: number): number {
  return idx < bytes.length ? (bytes[idx] ?? 0) : 0;
}

function getStringBytes(
  db: Database,
  key: string
): { bytes: Uint8Array | null; error: Reply | null } {
  const entry = db.get(key);
  if (!entry) return { bytes: null, error: null };
  if (entry.type !== 'string') return { bytes: null, error: WRONGTYPE_ERR };
  return { bytes: stringToBytes(entry.value as string), error: null };
}

function setStringFromBytes(
  db: Database,
  key: string,
  bytes: Uint8Array
): void {
  db.set(key, 'string', 'raw', bytesToString(bytes));
}

function parseBitOffset(s: string): { value: number; error: Reply | null } {
  const n = Number(s);
  if (!Number.isInteger(n) || n < 0 || n > 4294967295) {
    return { value: 0, error: BIT_OFFSET_ERR };
  }
  return { value: n, error: null };
}

function parseIntStrict(s: string): { value: number; error: Reply | null } {
  const val = parseInt(s, 10);
  if (isNaN(val) || String(val) !== s) {
    return { value: 0, error: NOT_INTEGER_ERR };
  }
  return { value: val, error: null };
}

// Population count (number of set bits) for a byte
const POPCOUNT_TABLE = new Uint8Array(256);
for (let i = 0; i < 256; i++) {
  let count = 0;
  let n = i;
  while (n) {
    count += n & 1;
    n >>= 1;
  }
  POPCOUNT_TABLE[i] = count;
}

function getBit(bytes: Uint8Array, bitOffset: number): number {
  const byteIdx = bitOffset >> 3;
  const bitIdx = 7 - (bitOffset & 7);
  return (byteAt(bytes, byteIdx) >> bitIdx) & 1;
}

// --- SETBIT ---

export function setbit(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const { value: offset, error: offsetErr } = parseBitOffset(args[1] ?? '');
  if (offsetErr) return offsetErr;

  const bitVal = parseInt(args[2] ?? '', 10);
  if (isNaN(bitVal) || (bitVal !== 0 && bitVal !== 1)) {
    return BIT_VALUE_ERR;
  }

  const { bytes: existing, error } = getStringBytes(db, key);
  if (error) return error;

  const byteIdx = offset >> 3;
  const bitIdx = 7 - (offset & 7);
  const neededLen = byteIdx + 1;

  const bufLen =
    existing && existing.length >= neededLen ? existing.length : neededLen;
  const buf = new Uint8Array(bufLen);
  if (existing) buf.set(existing);

  const oldBit = (byteAt(buf, byteIdx) >> bitIdx) & 1;

  if (bitVal) {
    buf[byteIdx] = byteAt(buf, byteIdx) | (1 << bitIdx);
  } else {
    buf[byteIdx] = byteAt(buf, byteIdx) & ~(1 << bitIdx);
  }

  setStringFromBytes(db, key, buf);
  return integerReply(oldBit);
}

// --- GETBIT ---

export function getbit(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const { value: offset, error: offsetErr } = parseBitOffset(args[1] ?? '');
  if (offsetErr) return offsetErr;

  const { bytes, error } = getStringBytes(db, key);
  if (error) return error;
  if (!bytes) return ZERO;

  return integerReply(getBit(bytes, offset));
}

// --- BITCOUNT ---

export function bitcount(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  if (args.length === 2 || args.length > 5) {
    return SYNTAX_ERR;
  }

  const { bytes, error } = getStringBytes(db, key);
  if (error) return error;
  if (!bytes || bytes.length === 0) return ZERO;

  let startIdx: number;
  let endIdx: number;
  let bitMode = false;

  if (args.length === 1) {
    startIdx = 0;
    endIdx = bytes.length - 1;
  } else {
    const { value: start, error: startErr } = parseIntStrict(args[1] ?? '');
    if (startErr) return startErr;
    const { value: end, error: endErr } = parseIntStrict(args[2] ?? '');
    if (endErr) return endErr;

    if (args.length >= 4) {
      const unit = (args[3] ?? '').toUpperCase();
      if (unit === 'BIT') {
        bitMode = true;
      } else if (unit !== 'BYTE') {
        return SYNTAX_ERR;
      }
    }

    if (bitMode) {
      const totalBits = bytes.length * 8;
      let s = start < 0 ? totalBits + start : start;
      let e = end < 0 ? totalBits + end : end;
      if (s < 0) s = 0;
      if (e >= totalBits) e = totalBits - 1;
      if (s > e) return ZERO;

      let count = 0;
      for (let j = s; j <= e; j++) {
        count += getBit(bytes, j);
      }
      return integerReply(count);
    }

    const len = bytes.length;
    startIdx = start < 0 ? Math.max(len + start, 0) : start;
    endIdx = end < 0 ? Math.max(len + end, 0) : end;
    if (endIdx >= len) endIdx = len - 1;
    if (startIdx > endIdx) return ZERO;
  }

  let count = 0;
  for (let j = startIdx; j <= endIdx; j++) {
    count += POPCOUNT_TABLE[byteAt(bytes, j)] ?? 0;
  }
  return integerReply(count);
}

// --- BITPOS ---

export function bitpos(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  const bitArgStr = args[1] ?? '';
  const { value: bitArg, error: bitArgErr } = parseIntStrict(bitArgStr);
  if (bitArgErr) return BIT_ARG_ERR;
  if (bitArg !== 0 && bitArg !== 1) return BIT_ARG_ERR;

  const entry = db.get(key);
  if (!entry) {
    return integerReply(bitArg === 0 ? 0 : -1);
  }
  if (entry.type !== 'string') return WRONGTYPE_ERR;

  const bytes = stringToBytes(entry.value as string);
  const byteLen = bytes.length;

  if (byteLen === 0) {
    return integerReply(bitArg === 0 ? 0 : -1);
  }

  let startByte = 0;
  let endByte = byteLen - 1;
  let endGiven = false;
  let bitMode = false;

  if (args.length >= 3) {
    const { value: s, error: sErr } = parseIntStrict(args[2] ?? '');
    if (sErr) return sErr;
    startByte = s < 0 ? Math.max(byteLen + s, 0) : s;
  }

  if (args.length >= 4) {
    const { value: e, error: eErr } = parseIntStrict(args[3] ?? '');
    if (eErr) return eErr;
    endByte = e < 0 ? Math.max(byteLen + e, 0) : e;
    endGiven = true;
  }

  if (args.length >= 5) {
    const unit = (args[4] ?? '').toUpperCase();
    if (unit === 'BIT') {
      bitMode = true;
    } else if (unit !== 'BYTE') {
      return SYNTAX_ERR;
    }
  }

  if (bitMode) {
    const totalBits = byteLen * 8;
    const rawStart = parseInt(args[2] ?? '0', 10);
    const rawEnd = parseInt(args[3] ?? String(totalBits - 1), 10);
    const startBit =
      rawStart < 0 ? Math.max(totalBits + rawStart, 0) : rawStart;
    const endBitClamped = rawEnd < 0 ? Math.max(totalBits + rawEnd, 0) : rawEnd;
    const endBit = endBitClamped >= totalBits ? totalBits - 1 : endBitClamped;

    if (startBit > endBit || startBit >= totalBits) return integerReply(-1);

    for (let j = startBit; j <= endBit; j++) {
      if (getBit(bytes, j) === bitArg) return integerReply(j);
    }
    return integerReply(-1);
  }

  // BYTE mode
  if (startByte > endByte || startByte >= byteLen) {
    return integerReply(-1);
  }
  if (endByte >= byteLen) endByte = byteLen - 1;

  for (let j = startByte; j <= endByte; j++) {
    const byte = byteAt(bytes, j);
    if (bitArg === 1 && byte === 0) continue;
    if (bitArg === 0 && byte === 0xff) continue;

    for (let bit = 7; bit >= 0; bit--) {
      if (((byte >> bit) & 1) === bitArg) {
        return integerReply(j * 8 + (7 - bit));
      }
    }
  }

  if (bitArg === 0 && !endGiven) {
    return integerReply((endByte + 1) * 8);
  }
  return integerReply(-1);
}

// --- BITOP ---

export function bitop(db: Database, args: string[]): Reply {
  const op = (args[0] ?? '').toUpperCase();
  const destKey = args[1] ?? '';
  const srcKeys = args.slice(2);

  if (op !== 'AND' && op !== 'OR' && op !== 'XOR' && op !== 'NOT') {
    return SYNTAX_ERR;
  }

  if (op === 'NOT' && srcKeys.length !== 1) {
    return BITOP_NOT_ERR;
  }

  const sources: Uint8Array[] = [];
  let maxLen = 0;

  for (const key of srcKeys) {
    const { bytes, error } = getStringBytes(db, key);
    if (error) return error;
    const b = bytes ?? new Uint8Array(0);
    sources.push(b);
    if (b.length > maxLen) maxLen = b.length;
  }

  if (maxLen === 0) {
    db.delete(destKey);
    return ZERO;
  }

  const result = new Uint8Array(maxLen);

  if (op === 'NOT') {
    const src = sources[0] ?? new Uint8Array(0);
    for (let j = 0; j < maxLen; j++) {
      result[j] = ~byteAt(src, j) & 0xff;
    }
  } else {
    if (op === 'AND') {
      result.fill(0xff);
    }

    for (const src of sources) {
      for (let j = 0; j < maxLen; j++) {
        const byte = byteAt(src, j);
        if (op === 'AND') {
          result[j] = (result[j] ?? 0) & byte;
        } else if (op === 'OR') {
          result[j] = (result[j] ?? 0) | byte;
        } else {
          result[j] = (result[j] ?? 0) ^ byte;
        }
      }
    }
  }

  setStringFromBytes(db, destKey, result);
  return integerReply(maxLen);
}

// --- BITFIELD ---

interface BitfieldType {
  signed: boolean;
  width: number;
}

type OverflowMode = 'WRAP' | 'SAT' | 'FAIL';

function parseBitfieldType(s: string): BitfieldType | null {
  if (s.length < 2) return null;
  const signChar = s[0];
  if (signChar !== 'i' && signChar !== 'u') return null;
  const signed = signChar === 'i';
  const widthStr = s.slice(1);
  const width = parseInt(widthStr, 10);
  if (isNaN(width) || String(width) !== widthStr || width < 1) return null;
  if (signed && width > 64) return null;
  if (!signed && width > 63) return null;
  return { signed, width };
}

function parseBitfieldOffset(
  s: string,
  width: number
): { value: number; error: Reply | null } {
  if (s.startsWith('#')) {
    const numStr = s.slice(1);
    const num = parseInt(numStr, 10);
    if (isNaN(num) || num < 0 || String(num) !== numStr) {
      return { value: 0, error: BIT_OFFSET_ERR };
    }
    return { value: num * width, error: null };
  }
  const num = parseInt(s, 10);
  if (isNaN(num) || num < 0 || String(num) !== s) {
    return { value: 0, error: BIT_OFFSET_ERR };
  }
  return { value: num, error: null };
}

function readBits(
  bytes: Uint8Array,
  bitOffset: number,
  width: number,
  signed: boolean
): bigint {
  let value = 0n;
  for (let j = 0; j < width; j++) {
    value = (value << 1n) | BigInt(getBit(bytes, bitOffset + j));
  }
  if (signed && width > 0 && (value >> BigInt(width - 1)) & 1n) {
    value -= 1n << BigInt(width);
  }
  return value;
}

function writeBits(
  bytes: Uint8Array,
  bitOffset: number,
  width: number,
  value: bigint
): Uint8Array {
  const lastBit = bitOffset + width - 1;
  const neededBytes = (lastBit >> 3) + 1;
  const bufLen = Math.max(bytes.length, neededBytes);
  const buf = new Uint8Array(bufLen);
  buf.set(bytes);

  const mask = (1n << BigInt(width)) - 1n;
  const masked = value & mask;

  for (let j = 0; j < width; j++) {
    const pos = bitOffset + j;
    const byteIdx = pos >> 3;
    const bitIdx = 7 - (pos & 7);
    const bit = Number((masked >> BigInt(width - 1 - j)) & 1n);
    if (bit) {
      buf[byteIdx] = byteAt(buf, byteIdx) | (1 << bitIdx);
    } else {
      buf[byteIdx] = byteAt(buf, byteIdx) & ~(1 << bitIdx);
    }
  }
  return buf;
}

function wrapValue(value: bigint, width: number, signed: boolean): bigint {
  const mask = (1n << BigInt(width)) - 1n;
  if (signed) {
    let v = value & mask;
    if (v >= 1n << BigInt(width - 1)) {
      v -= 1n << BigInt(width);
    }
    return v;
  }
  return value & mask;
}

function satValue(value: bigint, width: number, signed: boolean): bigint {
  if (signed) {
    const max = (1n << BigInt(width - 1)) - 1n;
    const min = -(1n << BigInt(width - 1));
    if (value > max) return max;
    if (value < min) return min;
    return value;
  }
  const max = (1n << BigInt(width)) - 1n;
  if (value > max) return max;
  if (value < 0n) return 0n;
  return value;
}

function fitsInType(value: bigint, width: number, signed: boolean): boolean {
  if (signed) {
    const max = (1n << BigInt(width - 1)) - 1n;
    const min = -(1n << BigInt(width - 1));
    return value >= min && value <= max;
  }
  const max = (1n << BigInt(width)) - 1n;
  return value >= 0n && value <= max;
}

export function bitfield(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  const entry = db.get(key);
  if (entry && entry.type !== 'string') return WRONGTYPE_ERR;

  const results: Reply[] = [];
  let overflow: OverflowMode = 'WRAP';
  let currentBytes: Uint8Array | null = entry
    ? stringToBytes(entry.value as string)
    : null;
  let modified = false;

  let i = 1;
  while (i < args.length) {
    const subcmd = (args[i] ?? '').toUpperCase();

    if (subcmd === 'OVERFLOW') {
      i++;
      const mode = (args[i] ?? '').toUpperCase();
      if (mode !== 'WRAP' && mode !== 'SAT' && mode !== 'FAIL') {
        return errorReply('ERR', 'Invalid OVERFLOW type specified');
      }
      overflow = mode;
      i++;
      continue;
    }

    if (subcmd === 'GET') {
      i++;
      const typeStr = args[i] ?? '';
      const type = parseBitfieldType(typeStr);
      if (!type) return BITFIELD_TYPE_ERR;
      i++;
      const { value: offset, error: offsetErr } = parseBitfieldOffset(
        args[i] ?? '',
        type.width
      );
      if (offsetErr) return offsetErr;
      i++;

      const bytes = currentBytes ?? new Uint8Array(0);
      const val = readBits(bytes, offset, type.width, type.signed);
      results.push(integerReply(Number(val)));
      continue;
    }

    if (subcmd === 'SET') {
      i++;
      const typeStr = args[i] ?? '';
      const type = parseBitfieldType(typeStr);
      if (!type) return BITFIELD_TYPE_ERR;
      i++;
      const { value: offset, error: offsetErr } = parseBitfieldOffset(
        args[i] ?? '',
        type.width
      );
      if (offsetErr) return offsetErr;
      i++;
      const valStr = args[i] ?? '';
      const parsedVal = parseBigInt(valStr);
      if (parsedVal === null) return NOT_INTEGER_ERR;
      i++;

      const bytes = currentBytes ?? new Uint8Array(0);
      const oldVal = readBits(bytes, offset, type.width, type.signed);

      if (!fitsInType(parsedVal, type.width, type.signed)) {
        if (overflow === 'FAIL') {
          results.push(NIL);
          continue;
        }
        const newVal =
          overflow === 'SAT'
            ? satValue(parsedVal, type.width, type.signed)
            : wrapValue(parsedVal, type.width, type.signed);
        currentBytes = writeBits(bytes, offset, type.width, newVal);
      } else {
        currentBytes = writeBits(bytes, offset, type.width, parsedVal);
      }
      modified = true;
      results.push(integerReply(Number(oldVal)));
      continue;
    }

    if (subcmd === 'INCRBY') {
      i++;
      const typeStr = args[i] ?? '';
      const type = parseBitfieldType(typeStr);
      if (!type) return BITFIELD_TYPE_ERR;
      i++;
      const { value: offset, error: offsetErr } = parseBitfieldOffset(
        args[i] ?? '',
        type.width
      );
      if (offsetErr) return offsetErr;
      i++;
      const incrStr = args[i] ?? '';
      const incr = parseBigInt(incrStr);
      if (incr === null) return NOT_INTEGER_ERR;
      i++;

      const bytes = currentBytes ?? new Uint8Array(0);
      const oldVal = readBits(bytes, offset, type.width, type.signed);
      const newVal = oldVal + incr;

      if (!fitsInType(newVal, type.width, type.signed)) {
        if (overflow === 'FAIL') {
          results.push(NIL);
          continue;
        }
        const finalVal =
          overflow === 'SAT'
            ? satValue(newVal, type.width, type.signed)
            : wrapValue(newVal, type.width, type.signed);
        currentBytes = writeBits(bytes, offset, type.width, finalVal);
        modified = true;
        results.push(integerReply(Number(finalVal)));
      } else {
        currentBytes = writeBits(bytes, offset, type.width, newVal);
        modified = true;
        results.push(integerReply(Number(newVal)));
      }
      continue;
    }

    return errorReply('ERR', `Unknown BITFIELD subcommand '${args[i] ?? ''}'`);
  }

  if (modified && currentBytes) {
    setStringFromBytes(db, key, currentBytes);
  }

  return arrayReply(results);
}

function parseBigInt(s: string): bigint | null {
  if (!/^-?\d+$/.test(s)) return null;
  try {
    return BigInt(s);
  } catch {
    return null;
  }
}
