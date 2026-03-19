import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import {
  bulkReply,
  integerReply,
  arrayReply,
  NIL,
  ZERO,
  EMPTY_ARRAY,
  WRONGTYPE_ERR,
  NOT_INTEGER_ERR,
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
 * Check if a list should use listpack encoding based on current state.
 * Returns true if the list is small enough for listpack.
 */
function fitsListpack(
  items: string[],
  maxEntries: number = DEFAULT_MAX_LISTPACK_ENTRIES,
  maxValue: number = DEFAULT_MAX_LISTPACK_VALUE
): boolean {
  if (items.length > maxEntries) return false;
  for (const item of items) {
    if (strByteLength(item) > maxValue) return false;
  }
  return true;
}

/**
 * Get or create a list entry. Returns the list array and entry, or an error reply.
 * If the key doesn't exist, creates a new empty list.
 */
function getOrCreateList(
  db: Database,
  key: string
): { list: string[]; error: null } | { list: null; error: Reply } {
  const entry = db.get(key);
  if (entry) {
    if (entry.type !== 'list') return { list: null, error: WRONGTYPE_ERR };
    return { list: entry.value as string[], error: null };
  }
  const items: string[] = [];
  db.set(key, 'list', 'listpack', items);
  return { list: items, error: null };
}

/**
 * Get an existing list entry. Returns null if key doesn't exist.
 */
function getExistingList(
  db: Database,
  key: string
): { list: string[] | null; error: Reply | null } {
  const entry = db.get(key);
  if (!entry) return { list: null, error: null };
  if (entry.type !== 'list') return { list: null, error: WRONGTYPE_ERR };
  return { list: entry.value as string[], error: null };
}

/**
 * Promote encoding from listpack to quicklist if needed.
 * Redis only transitions in one direction: listpack → quicklist.
 * Once promoted, it never reverts back — even if the list shrinks.
 */
function updateEncoding(db: Database, key: string): void {
  const entry = db.get(key);
  if (!entry || entry.type !== 'list') return;
  if (entry.encoding === 'quicklist') return; // never demote
  const items = entry.value as string[];
  if (!fitsListpack(items)) {
    entry.encoding = 'quicklist';
  }
}

/**
 * Delete the key if the list is empty.
 */
function deleteIfEmpty(db: Database, key: string, items: string[]): void {
  if (items.length === 0) {
    db.delete(key);
  }
}

/**
 * Parse count argument for LPOP/RPOP. Returns parsed count or error reply.
 */
function parseCount(
  countArg: string | undefined
): { count: number | null; error: null } | { count: null; error: Reply } {
  if (countArg === undefined) return { count: null, error: null };
  if (!/^-?\d+$/.test(countArg)) {
    return { count: null, error: NOT_INTEGER_ERR };
  }
  const n = Number(countArg);
  if (!Number.isInteger(n) || n < 0) {
    return { count: null, error: NOT_INTEGER_ERR };
  }
  return { count: n, error: null };
}

// --- LPUSH ---

export function lpush(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const { list, error } = getOrCreateList(db, key);
  if (error) return error;

  for (let i = 1; i < args.length; i++) {
    list.unshift(args[i] ?? '');
  }

  updateEncoding(db, key);
  return integerReply(list.length);
}

// --- RPUSH ---

export function rpush(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const { list, error } = getOrCreateList(db, key);
  if (error) return error;

  for (let i = 1; i < args.length; i++) {
    list.push(args[i] ?? '');
  }

  updateEncoding(db, key);
  return integerReply(list.length);
}

// --- LPUSHX ---

export function lpushx(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return ZERO;

  for (let i = 1; i < args.length; i++) {
    list.unshift(args[i] ?? '');
  }

  updateEncoding(db, key);
  return integerReply(list.length);
}

// --- RPUSHX ---

export function rpushx(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return ZERO;

  for (let i = 1; i < args.length; i++) {
    list.push(args[i] ?? '');
  }

  updateEncoding(db, key);
  return integerReply(list.length);
}

// --- LPOP ---

export function lpop(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const { count, error: countErr } = parseCount(args[1]);
  if (countErr) return countErr;

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return NIL;

  // No count argument — single pop, return bulk
  if (count === null) {
    const value = list.shift() ?? '';
    deleteIfEmpty(db, key, list);
    return bulkReply(value);
  }

  // Count = 0 — return empty array
  if (count === 0) return EMPTY_ARRAY;

  // Pop up to count elements
  const popped = list.splice(0, count);
  deleteIfEmpty(db, key, list);
  return arrayReply(popped.map((v) => bulkReply(v)));
}

// --- RPOP ---

export function rpop(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const { count, error: countErr } = parseCount(args[1]);
  if (countErr) return countErr;

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return NIL;

  // No count argument — single pop, return bulk
  if (count === null) {
    const value = list.pop() ?? '';
    deleteIfEmpty(db, key, list);
    return bulkReply(value);
  }

  // Count = 0 — return empty array
  if (count === 0) return EMPTY_ARRAY;

  // Pop up to count elements from the tail
  const actualCount = Math.min(count, list.length);
  const popped = list.splice(list.length - actualCount, actualCount);
  popped.reverse();
  deleteIfEmpty(db, key, list);
  return arrayReply(popped.map((v) => bulkReply(v)));
}

// --- LLEN ---

export function llen(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return ZERO;

  return integerReply(list.length);
}
