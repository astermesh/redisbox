import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import {
  integerReply,
  arrayReply,
  errorReply,
  NIL,
  WRONGTYPE_ERR,
  SYNTAX_ERR,
  NOT_INTEGER_ERR,
} from '../../types.ts';
import type { CommandSpec } from '../../command-table.ts';
import { notify, EVENT_FLAGS } from '../../pubsub/notify.ts';
import {
  stringToBytes,
  byteAt,
  setStringFromBytes,
  getBit,
  BIT_OFFSET_ERR,
} from './bytes.ts';

// --- Error constants ---

const BITFIELD_TYPE_ERR = errorReply(
  'ERR',
  'Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.'
);

const BITFIELD_RO_ERR = errorReply(
  'ERR',
  'BITFIELD_RO only supports the GET subcommand'
);

// --- Types ---

interface BitfieldType {
  signed: boolean;
  width: number;
}

type OverflowMode = 'WRAP' | 'SAT' | 'FAIL';

// --- Parsing ---

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

function parseBigInt(s: string): bigint | null {
  if (!/^-?\d+$/.test(s)) return null;
  try {
    return BigInt(s);
  } catch {
    return null;
  }
}

// --- Bit manipulation ---

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

// --- Overflow helpers ---

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

// --- Handlers ---

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

    return SYNTAX_ERR;
  }

  if (modified && currentBytes) {
    setStringFromBytes(db, key, currentBytes);
  }

  return arrayReply(results);
}

export function bitfieldRo(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  const entry = db.get(key);
  if (entry && entry.type !== 'string') return WRONGTYPE_ERR;

  const results: Reply[] = [];
  const bytes = entry
    ? stringToBytes(entry.value as string)
    : new Uint8Array(0);

  let i = 1;
  while (i < args.length) {
    const subcmd = (args[i] ?? '').toUpperCase();

    if (subcmd !== 'GET') {
      return BITFIELD_RO_ERR;
    }

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

    const val = readBits(bytes, offset, type.width, type.signed);
    results.push(integerReply(Number(val)));
  }

  return arrayReply(results);
}

// --- Specs ---

export const specs: CommandSpec[] = [
  {
    name: 'bitfield',
    handler: (ctx, args) => {
      const reply = bitfield(ctx.db, args);
      if (reply.kind === 'array') {
        // Check if any SET or INCRBY operation was performed
        const hasWrite = args.some(
          (a) => a.toUpperCase() === 'SET' || a.toUpperCase() === 'INCRBY'
        );
        if (hasWrite) {
          notify(ctx, EVENT_FLAGS.STRING, 'setbit', args[0] ?? '');
        }
      }
      return reply;
    },
    arity: -2,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@bitmap'],
  },
  {
    name: 'bitfield_ro',
    handler: (ctx, args) => bitfieldRo(ctx.db, args),
    arity: -2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@bitmap', '@fast'],
  },
];
