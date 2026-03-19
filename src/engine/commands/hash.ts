import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import {
  bulkReply,
  integerReply,
  arrayReply,
  wrongArityError,
  OK,
  NIL,
  ZERO,
  ONE,
  EMPTY_ARRAY,
  WRONGTYPE_ERR,
} from '../types.ts';

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
    if (!hash.has(field)) added++;
    hash.set(field, value);
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

  if (hash.has(field)) return ZERO;

  hash.set(field, value);
  updateEncoding(db, key);
  return ONE;
}
