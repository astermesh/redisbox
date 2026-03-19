import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import {
  integerReply,
  bulkReply,
  WRONGTYPE_ERR,
  NOT_INTEGER_ERR,
  NOT_FLOAT_ERR,
  INF_NAN_ERR,
  OVERFLOW_ERR,
} from '../types.ts';
import { determineStringEncoding } from './string.ts';

const INT64_MAX = BigInt('9223372036854775807');
const INT64_MIN = BigInt('-9223372036854775808');

/**
 * Parse a string as a 64-bit signed integer.
 * Returns null if the value is not a valid integer or out of range.
 */
export function parseInteger(value: string): bigint | null {
  if (value === '' || value !== value.trim()) return null;
  // reject floats, leading zeros (except "0" itself), and non-numeric
  if (!/^-?(?:0|[1-9]\d*)$/.test(value)) return null;
  try {
    const n = BigInt(value);
    if (n < INT64_MIN || n > INT64_MAX) return null;
    return n;
  } catch {
    return null;
  }
}

/**
 * Get current integer value of a key, treating missing keys as 0.
 * Returns error reply if the key holds wrong type or non-integer value.
 */
function getIntValue(
  db: Database,
  key: string
): { val: bigint; error: Reply | null } {
  const entry = db.get(key);
  if (!entry) return { val: 0n, error: null };
  if (entry.type !== 'string') return { val: 0n, error: WRONGTYPE_ERR };
  const n = parseInteger(entry.value as string);
  if (n === null) return { val: 0n, error: NOT_INTEGER_ERR };
  return { val: n, error: null };
}

/**
 * Perform integer increment/decrement and store result.
 */
function incrByInt(db: Database, key: string, delta: bigint): Reply {
  const { val, error } = getIntValue(db, key);
  if (error) return error;

  const result = val + delta;
  if (result > INT64_MAX || result < INT64_MIN) return OVERFLOW_ERR;

  const strResult = result.toString();
  db.set(key, 'string', 'int', strResult);

  // Use Number for values within safe integer range, BigInt otherwise
  // to preserve precision for 64-bit integers beyond Number.MAX_SAFE_INTEGER
  const replyValue =
    result >= -9007199254740991n && result <= 9007199254740991n
      ? Number(result)
      : result;
  return integerReply(replyValue);
}

export function incr(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  return incrByInt(db, key, 1n);
}

export function decr(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  return incrByInt(db, key, -1n);
}

export function incrby(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const increment = args[1] ?? '';
  const delta = parseInteger(increment);
  if (delta === null) return NOT_INTEGER_ERR;
  return incrByInt(db, key, delta);
}

export function decrby(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const decrement = args[1] ?? '';
  const delta = parseInteger(decrement);
  if (delta === null) return NOT_INTEGER_ERR;
  return incrByInt(db, key, -delta);
}

/**
 * Parse a float value for INCRBYFLOAT.
 * Returns { value, isInf } where isInf indicates inf/-inf input.
 * Returns null for unparseable values.
 */
export function parseFloat64(
  value: string
): { value: number; isInf: boolean } | null {
  if (value === '' || value !== value.trim()) return null;
  const lower = value.toLowerCase();
  if (lower === 'nan') return null;
  if (lower === 'inf' || lower === '+inf' || lower === '-inf') {
    return { value: lower === '-inf' ? -Infinity : Infinity, isInf: true };
  }
  const n = Number(value);
  if (isNaN(n)) return null;
  if (!isFinite(n)) return { value: n, isInf: true };
  return { value: n, isInf: false };
}

/**
 * Format a float result matching Redis's ld2string(LD_STR_AUTO) behavior.
 *
 * Redis uses %.17Lg (17 significant digits, C's %g scientific notation rules)
 * with trailing zeroes trimmed. C's %g uses scientific notation when
 * exponent < -4 or >= precision (17).
 *
 * For the fixed-notation range (exp -4..16), we use String(n) which gives
 * the shortest round-trip representation. This matches Redis's long double
 * output in most cases (Redis's 80-bit precision means 17-digit output
 * looks "clean", while 64-bit double's 17-digit output reveals artifacts).
 *
 * For the scientific-notation range, we format with C conventions
 * (at least 2-digit exponent, sign prefix).
 */
export function formatFloat(n: number): string {
  if (n === 0 || Object.is(n, -0)) return '0';

  const exp = getExponent(n);

  // C's %g with precision 17: scientific when exp < -4 or exp >= 17
  if (exp < -4 || exp >= 17) {
    return formatScientific(n);
  }
  // Fixed notation — String(n) gives shortest round-trip representation
  return String(n);
}

function getExponent(n: number): number {
  const parts = Math.abs(n).toExponential().split('e');
  return parseInt(parts[1] ?? '0', 10);
}

function formatScientific(n: number): string {
  const s = n.toExponential(16); // 1 + 16 = 17 significant digits
  const eIdx = s.indexOf('e');
  let mantissa = s.substring(0, eIdx);
  const expRaw = s.substring(eIdx + 1);

  // Trim trailing zeros from mantissa
  if (mantissa.includes('.')) {
    mantissa = mantissa.replace(/0+$/, '').replace(/\.$/, '');
  }

  // Pad exponent to at least 2 digits (C convention)
  const sign = expRaw.startsWith('-') ? '-' : '+';
  const absExp = Math.abs(parseInt(expRaw, 10)).toString().padStart(2, '0');

  return `${mantissa}e${sign}${absExp}`;
}

export function incrbyfloat(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const incrStr = args[1] ?? '';

  // Parse increment
  const incrParsed = parseFloat64(incrStr);
  if (incrParsed === null) return NOT_FLOAT_ERR;
  if (incrParsed.isInf) return INF_NAN_ERR;

  // Get current value
  const entry = db.get(key);
  let current = 0;
  if (entry) {
    if (entry.type !== 'string') return WRONGTYPE_ERR;
    const parsed = parseFloat64(entry.value as string);
    if (parsed === null) return NOT_FLOAT_ERR;
    if (parsed.isInf) return INF_NAN_ERR;
    current = parsed.value;
  }

  const result = current + incrParsed.value;
  if (!isFinite(result)) return INF_NAN_ERR;

  const strResult = formatFloat(result);
  const encoding = determineStringEncoding(strResult);
  db.set(key, 'string', encoding, strResult);

  return bulkReply(strResult);
}
