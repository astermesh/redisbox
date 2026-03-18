import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import {
  integerReply,
  bulkReply,
  errorReply,
  wrongTypeError,
} from '../types.ts';
import { determineStringEncoding } from './string.ts';

const INT64_MAX = BigInt('9223372036854775807');
const INT64_MIN = BigInt('-9223372036854775808');

const NOT_INTEGER_ERR = errorReply(
  'ERR',
  'value is not an integer or out of range'
);
const OVERFLOW_ERR = errorReply('ERR', 'increment or decrement would overflow');

/**
 * Parse a string as a 64-bit signed integer.
 * Returns null if the value is not a valid integer or out of range.
 */
function parseInteger(value: string): bigint | null {
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
  if (entry.type !== 'string') return { val: 0n, error: wrongTypeError() };
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

  return integerReply(Number(result));
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

const NOT_FLOAT_ERR = errorReply('ERR', 'value is not a valid float');
const INF_NAN_ERR = errorReply(
  'ERR',
  'increment would produce NaN or Infinity'
);

/**
 * Parse a float value for INCRBYFLOAT.
 * Returns { value, isInf } where isInf indicates inf/-inf input.
 * Returns null for unparseable values.
 */
function parseFloat64(value: string): { value: number; isInf: boolean } | null {
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
 * Format a float result the way Redis does:
 * - Uses shortest representation that round-trips
 * - No trailing zeroes after decimal point
 * - No decimal point if integer
 */
function formatFloat(n: number): string {
  // toString() produces the shortest decimal that uniquely identifies
  // the double, matching Redis's %.17Lg behavior after trimming
  return String(n);
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
    if (entry.type !== 'string') return wrongTypeError();
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
