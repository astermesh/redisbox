import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import {
  bulkReply,
  integerReply,
  arrayReply,
  errorReply,
  wrongArityError,
  OK,
  NIL,
  ZERO,
  ONE,
  EMPTY_ARRAY,
  WRONGTYPE_ERR,
  NOT_INTEGER_ERR,
  NOT_FLOAT_ERR,
  INF_NAN_ERR,
  OVERFLOW_ERR,
  SYNTAX_ERR,
} from '../types.ts';
import { parseInteger, parseFloat64, formatFloat } from './incr.ts';
import { matchGlob } from '../glob-pattern.ts';

const textEncoder = new TextEncoder();

function strByteLength(s: string): number {
  return textEncoder.encode(s).length;
}

// Default thresholds — match Redis defaults.
// TODO: read from ConfigStore when config is wired into CommandContext.
const DEFAULT_MAX_LISTPACK_ENTRIES = 128;
const DEFAULT_MAX_LISTPACK_VALUE = 64;

/**
 * Check if a hash should use listpack encoding based on current state.
 * Returns true if the hash is small enough for listpack.
 */
function fitsListpack(
  hash: Map<string, string>,
  maxEntries: number = DEFAULT_MAX_LISTPACK_ENTRIES,
  maxValue: number = DEFAULT_MAX_LISTPACK_VALUE
): boolean {
  if (hash.size > maxEntries) return false;
  for (const [field, value] of hash) {
    if (strByteLength(field) > maxValue || strByteLength(value) > maxValue) {
      return false;
    }
  }
  return true;
}

/**
 * Get or create a hash entry. Returns the hash map and entry, or an error reply.
 * If the key doesn't exist, creates a new empty hash.
 */
function getOrCreateHash(
  db: Database,
  key: string
): { hash: Map<string, string>; error: null } | { hash: null; error: Reply } {
  const entry = db.get(key);
  if (entry) {
    if (entry.type !== 'hash') return { hash: null, error: WRONGTYPE_ERR };
    return { hash: entry.value as Map<string, string>, error: null };
  }
  const hash = new Map<string, string>();
  db.set(key, 'hash', 'listpack', hash);
  return { hash, error: null };
}

/**
 * Get an existing hash entry. Returns null if key doesn't exist.
 */
function getExistingHash(
  db: Database,
  key: string
): { hash: Map<string, string> | null; error: Reply | null } {
  const entry = db.get(key);
  if (!entry) return { hash: null, error: null };
  if (entry.type !== 'hash') return { hash: null, error: WRONGTYPE_ERR };
  return { hash: entry.value as Map<string, string>, error: null };
}

/**
 * Promote encoding from listpack to hashtable if needed.
 * Redis only transitions in one direction: listpack → hashtable.
 * Once promoted, it never reverts back — even if the hash shrinks.
 */
function updateEncoding(db: Database, key: string): void {
  const entry = db.get(key);
  if (!entry || entry.type !== 'hash') return;
  if (entry.encoding === 'hashtable') return; // never demote
  const hash = entry.value as Map<string, string>;
  if (!fitsListpack(hash)) {
    entry.encoding = 'hashtable';
  }
}

// --- HSET ---

export function hset(db: Database, args: string[]): Reply {
  if (args.length < 3 || (args.length - 1) % 2 !== 0) {
    return wrongArityError('hset');
  }

  const key = args[0] ?? '';
  const { hash, error } = getOrCreateHash(db, key);
  if (error) return error;

  let added = 0;
  for (let i = 1; i < args.length; i += 2) {
    const field = args[i] ?? '';
    const value = args[i + 1] ?? '';
    db.tryExpireField(key, field);
    if (!hash.has(field)) added++;
    hash.set(field, value);
    db.removeFieldExpiry(key, field);
  }

  updateEncoding(db, key);
  return integerReply(added);
}

// --- HGET ---

export function hget(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const field = args[1] ?? '';

  const { hash, error } = getExistingHash(db, key);
  if (error) return error;
  if (!hash) return NIL;

  // Lazy field expiration
  db.tryExpireField(key, field);

  const value = hash.get(field);
  return value !== undefined ? bulkReply(value) : NIL;
}

// --- HMSET ---

export function hmset(db: Database, args: string[]): Reply {
  if (args.length < 3 || (args.length - 1) % 2 !== 0) {
    return wrongArityError('hmset');
  }

  const key = args[0] ?? '';
  const { hash, error } = getOrCreateHash(db, key);
  if (error) return error;

  for (let i = 1; i < args.length; i += 2) {
    const field = args[i] ?? '';
    const value = args[i + 1] ?? '';
    hash.set(field, value);
    db.removeFieldExpiry(key, field);
  }

  updateEncoding(db, key);
  return OK;
}

// --- HMGET ---

export function hmget(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  const { hash, error } = getExistingHash(db, key);
  if (error) return error;

  const results: Reply[] = [];
  for (let i = 1; i < args.length; i++) {
    const field = args[i] ?? '';
    if (hash) {
      // Lazy field expiration
      db.tryExpireField(key, field);
      const value = hash.get(field);
      results.push(value !== undefined ? bulkReply(value) : NIL);
    } else {
      results.push(NIL);
    }
  }
  return arrayReply(results);
}

// --- HGETALL ---

export function hgetall(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  const { hash, error } = getExistingHash(db, key);
  if (error) return error;
  if (!hash || hash.size === 0) return EMPTY_ARRAY;

  // Lazy field expiration for all fields
  db.expireHashFields(key);
  if (hash.size === 0) return EMPTY_ARRAY;

  const results: Reply[] = [];
  for (const [field, value] of hash) {
    results.push(bulkReply(field));
    results.push(bulkReply(value));
  }
  return arrayReply(results);
}

// --- HDEL ---

export function hdel(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  const { hash, error } = getExistingHash(db, key);
  if (error) return error;
  if (!hash) return ZERO;

  let deleted = 0;
  for (let i = 1; i < args.length; i++) {
    const field = args[i] ?? '';
    if (hash.delete(field)) {
      db.removeFieldExpiry(key, field);
      deleted++;
    }
  }

  // If hash is now empty, delete the key
  if (hash.size === 0) {
    db.delete(key);
  }

  return integerReply(deleted);
}

// --- HEXISTS ---

export function hexists(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const field = args[1] ?? '';

  const { hash, error } = getExistingHash(db, key);
  if (error) return error;
  if (!hash) return ZERO;

  // Lazy field expiration
  db.tryExpireField(key, field);

  return hash.has(field) ? ONE : ZERO;
}

// --- HLEN ---

export function hlen(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  const { hash, error } = getExistingHash(db, key);
  if (error) return error;
  if (!hash) return ZERO;

  return integerReply(hash.size);
}

// --- HKEYS ---

export function hkeys(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  const { hash, error } = getExistingHash(db, key);
  if (error) return error;
  if (!hash || hash.size === 0) return EMPTY_ARRAY;

  // Lazy field expiration for all fields
  db.expireHashFields(key);
  if (hash.size === 0) return EMPTY_ARRAY;

  const results: Reply[] = [];
  for (const field of hash.keys()) {
    results.push(bulkReply(field));
  }
  return arrayReply(results);
}

// --- HVALS ---

export function hvals(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  const { hash, error } = getExistingHash(db, key);
  if (error) return error;
  if (!hash || hash.size === 0) return EMPTY_ARRAY;

  // Lazy field expiration for all fields
  db.expireHashFields(key);
  if (hash.size === 0) return EMPTY_ARRAY;

  const results: Reply[] = [];
  for (const value of hash.values()) {
    results.push(bulkReply(value));
  }
  return arrayReply(results);
}

// --- HSETNX ---

export function hsetnx(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const field = args[1] ?? '';
  const value = args[2] ?? '';

  const { hash, error } = getOrCreateHash(db, key);
  if (error) return error;

  db.tryExpireField(key, field);
  if (hash.has(field)) return ZERO;

  hash.set(field, value);
  updateEncoding(db, key);
  return ONE;
}

// --- HINCRBY ---

const INT64_MAX = BigInt('9223372036854775807');
const INT64_MIN = BigInt('-9223372036854775808');

const HASH_NOT_INTEGER_ERR = errorReply('ERR', 'hash value is not an integer');
const HASH_NOT_FLOAT_ERR = errorReply('ERR', 'hash value is not a valid float');

export function hincrby(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const field = args[1] ?? '';
  const incrStr = args[2] ?? '';

  const delta = parseInteger(incrStr);
  if (delta === null) return NOT_INTEGER_ERR;

  const { hash, error } = getOrCreateHash(db, key);
  if (error) return error;

  db.tryExpireField(key, field);
  const currentStr = hash.get(field) ?? '0';
  const current = parseInteger(currentStr);
  if (current === null) return HASH_NOT_INTEGER_ERR;

  const result = current + delta;
  if (result > INT64_MAX || result < INT64_MIN) return OVERFLOW_ERR;

  hash.set(field, result.toString());
  db.removeFieldExpiry(key, field);
  updateEncoding(db, key);

  const replyValue =
    result >= -9007199254740991n && result <= 9007199254740991n
      ? Number(result)
      : result;
  return integerReply(replyValue);
}

// --- HINCRBYFLOAT ---

export function hincrbyfloat(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const field = args[1] ?? '';
  const incrStr = args[2] ?? '';

  const incrParsed = parseFloat64(incrStr);
  if (incrParsed === null) return NOT_FLOAT_ERR;
  if (incrParsed.isInf) return INF_NAN_ERR;

  const { hash, error } = getOrCreateHash(db, key);
  if (error) return error;

  db.tryExpireField(key, field);
  const currentStr = hash.get(field) ?? '0';
  const currentParsed = parseFloat64(currentStr);
  if (currentParsed === null) return HASH_NOT_FLOAT_ERR;
  if (currentParsed.isInf) return HASH_NOT_FLOAT_ERR;

  const result = currentParsed.value + incrParsed.value;
  if (!isFinite(result)) return INF_NAN_ERR;

  const strResult = formatFloat(result);
  hash.set(field, strResult);
  db.removeFieldExpiry(key, field);
  updateEncoding(db, key);

  return bulkReply(strResult);
}

// --- HRANDFIELD ---

export function hrandfield(
  db: Database,
  args: string[],
  rng: () => number
): Reply {
  const key = args[0] ?? '';

  const { hash, error } = getExistingHash(db, key);
  if (error) return error;

  // Bulk-expire all expired fields before random selection (Redis behavior)
  if (hash) db.expireHashFields(key);

  // No count argument — return single field or nil
  if (args.length === 1) {
    if (!hash || hash.size === 0) return NIL;
    const fields = Array.from(hash.keys());
    const idx = Math.floor(rng() * fields.length);
    return bulkReply(fields[idx] ?? '');
  }

  // Parse count
  const countStr = args[1] ?? '';
  const countParsed = parseInteger(countStr);
  if (countParsed === null) return NOT_INTEGER_ERR;
  const count = Number(countParsed);

  // Check for WITHVALUES
  let withValues = false;
  if (args.length > 2) {
    const flag = (args[2] ?? '').toUpperCase();
    if (flag !== 'WITHVALUES') return SYNTAX_ERR;
    if (args.length > 3) return SYNTAX_ERR;
    withValues = true;
  }

  if (!hash || hash.size === 0) return EMPTY_ARRAY;

  if (count === 0) return EMPTY_ARRAY;

  const fields = Array.from(hash.keys());
  const results: Reply[] = [];

  if (count > 0) {
    // Positive count: unique elements, at most hash size
    const actual = Math.min(count, fields.length);
    // Fisher-Yates partial shuffle
    const shuffled = [...fields];
    for (let i = 0; i < actual; i++) {
      const j = i + Math.floor(rng() * (shuffled.length - i));
      const tmp = shuffled[i] ?? '';
      shuffled[i] = shuffled[j] ?? '';
      shuffled[j] = tmp;
    }
    for (let i = 0; i < actual; i++) {
      const f = shuffled[i] ?? '';
      results.push(bulkReply(f));
      if (withValues) {
        results.push(bulkReply(hash.get(f) ?? ''));
      }
    }
  } else {
    // Negative count: |count| elements, may repeat
    const absCount = Math.abs(count);
    for (let i = 0; i < absCount; i++) {
      const idx = Math.floor(rng() * fields.length);
      const f = fields[idx] ?? '';
      results.push(bulkReply(f));
      if (withValues) {
        results.push(bulkReply(hash.get(f) ?? ''));
      }
    }
  }

  return arrayReply(results);
}

// --- HSCAN ---

export function hscan(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const cursorStr = args[1] ?? '0';

  const cursor = parseInt(cursorStr, 10);
  if (isNaN(cursor) || cursor < 0) {
    return errorReply('ERR', 'invalid cursor');
  }

  // Check key type before parsing options
  const entry = db.get(key);
  if (entry && entry.type !== 'hash') return WRONGTYPE_ERR;

  let matchPattern: string | null = null;
  let count = 10;
  let noValues = false;

  let i = 2;
  while (i < args.length) {
    const flag = (args[i] ?? '').toUpperCase();
    if (flag === 'MATCH') {
      i++;
      matchPattern = args[i] ?? '*';
    } else if (flag === 'COUNT') {
      i++;
      count = parseInt(args[i] ?? '10', 10);
      if (isNaN(count)) {
        return NOT_INTEGER_ERR;
      }
      if (count < 1) {
        return SYNTAX_ERR;
      }
    } else if (flag === 'NOVALUES') {
      noValues = true;
    } else {
      return SYNTAX_ERR;
    }
    i++;
  }

  if (!entry) {
    return arrayReply([bulkReply('0'), EMPTY_ARRAY]);
  }

  const hash = entry.value as Map<string, string>;
  const allFields = Array.from(hash.keys());

  if (allFields.length === 0) {
    return arrayReply([bulkReply('0'), EMPTY_ARRAY]);
  }

  const results: Reply[] = [];
  let position = cursor;
  let scanned = 0;

  while (position < allFields.length && scanned < count) {
    const field = allFields[position] ?? '';
    position++;
    scanned++;

    // Skip expired fields (lazy field expiration)
    if (db.tryExpireField(key, field)) continue;

    if (matchPattern && !matchGlob(matchPattern, field)) continue;

    results.push(bulkReply(field));
    if (!noValues) {
      results.push(bulkReply(hash.get(field) ?? ''));
    }
  }

  const nextCursor = position >= allFields.length ? 0 : position;

  return arrayReply([bulkReply(String(nextCursor)), arrayReply(results)]);
}
