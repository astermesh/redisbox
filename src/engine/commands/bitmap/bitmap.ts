import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import { integerReply, ZERO, WRONGTYPE_ERR, SYNTAX_ERR } from '../../types.ts';
import type { CommandSpec } from '../../command-table.ts';
import { notify, EVENT_FLAGS } from '../../pubsub/notify.ts';
import {
  stringToBytes,
  byteAt,
  getStringBytes,
  setStringFromBytes,
  parseBitOffset,
  parseIntStrict,
  getBit,
  POPCOUNT_TABLE,
  BIT_VALUE_ERR,
  BIT_ARG_ERR,
  BITOP_NOT_ERR,
} from './bytes.ts';

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

  if (args.length === 2 || args.length > 4) {
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

// --- Specs ---

export const specs: CommandSpec[] = [
  {
    name: 'setbit',
    handler: (ctx, args) => {
      const reply = setbit(ctx.db, args);
      if (reply.kind === 'integer') {
        notify(ctx, EVENT_FLAGS.STRING, 'setbit', args[0] ?? '');
      }
      return reply;
    },
    arity: 4,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@bitmap'],
  },
  {
    name: 'getbit',
    handler: (ctx, args) => getbit(ctx.db, args),
    arity: 3,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@bitmap', '@fast'],
  },
  {
    name: 'bitcount',
    handler: (ctx, args) => bitcount(ctx.db, args),
    arity: -2,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@bitmap'],
  },
  {
    name: 'bitpos',
    handler: (ctx, args) => bitpos(ctx.db, args),
    arity: -3,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@bitmap'],
  },
  {
    name: 'bitop',
    handler: (ctx, args) => {
      const reply = bitop(ctx.db, args);
      if (reply.kind === 'integer') {
        notify(ctx, EVENT_FLAGS.STRING, 'set', args[1] ?? '');
      }
      return reply;
    },
    arity: -4,
    flags: ['write', 'denyoom'],
    firstKey: 2,
    lastKey: -1,
    keyStep: 1,
    categories: ['@write', '@bitmap'],
  },
];
