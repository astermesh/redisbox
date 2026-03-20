import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import {
  bulkReply,
  integerReply,
  arrayReply,
  errorReply,
  NIL,
  OK,
  ZERO,
  EMPTY_ARRAY,
  WRONGTYPE_ERR,
  NOT_INTEGER_ERR,
  NO_SUCH_KEY_ERR,
  SYNTAX_ERR,
} from '../types.ts';

import { strByteLength } from '../utils.ts';

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

/**
 * Parse a string as an integer, returning NOT_INTEGER_ERR on failure.
 */
function parseInteger(
  s: string
): { value: number; error: null } | { value: null; error: Reply } {
  if (!/^-?\d+$/.test(s)) {
    return { value: null, error: NOT_INTEGER_ERR };
  }
  const n = Number(s);
  if (!Number.isInteger(n)) {
    return { value: null, error: NOT_INTEGER_ERR };
  }
  return { value: n, error: null };
}

/**
 * Resolve a Redis-style index (supports negatives) to an absolute index.
 */
function resolveIndex(index: number, length: number): number {
  if (index < 0) index += length;
  return index;
}

// --- LRANGE ---

export function lrange(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const startParsed = parseInteger(args[1] ?? '');
  if (startParsed.error) return startParsed.error;
  const stopParsed = parseInteger(args[2] ?? '');
  if (stopParsed.error) return stopParsed.error;

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return EMPTY_ARRAY;

  let start = resolveIndex(startParsed.value, list.length);
  let stop = resolveIndex(stopParsed.value, list.length);

  if (start < 0) start = 0;
  if (stop >= list.length) stop = list.length - 1;

  if (start > stop) return EMPTY_ARRAY;

  const result: Reply[] = [];
  for (let i = start; i <= stop; i++) {
    result.push(bulkReply(list[i] ?? ''));
  }
  return arrayReply(result);
}

// --- LINDEX ---

export function lindex(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const indexParsed = parseInteger(args[1] ?? '');
  if (indexParsed.error) return indexParsed.error;

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return NIL;

  const idx = resolveIndex(indexParsed.value, list.length);
  if (idx < 0 || idx >= list.length) return NIL;

  return bulkReply(list[idx] ?? '');
}

// --- LSET ---

export function lset(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const indexParsed = parseInteger(args[1] ?? '');
  if (indexParsed.error) return indexParsed.error;
  const value = args[2] ?? '';

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return NO_SUCH_KEY_ERR;

  const idx = resolveIndex(indexParsed.value, list.length);
  if (idx < 0 || idx >= list.length) {
    return errorReply('ERR', 'index out of range');
  }

  list[idx] = value;
  updateEncoding(db, key);
  return OK;
}

// --- LINSERT ---

export function linsert(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const direction = (args[1] ?? '').toUpperCase();
  const pivot = args[2] ?? '';
  const value = args[3] ?? '';

  if (direction !== 'BEFORE' && direction !== 'AFTER') {
    return SYNTAX_ERR;
  }

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return ZERO;

  const pivotIndex = list.indexOf(pivot);
  if (pivotIndex === -1) return integerReply(-1);

  const insertIndex = direction === 'BEFORE' ? pivotIndex : pivotIndex + 1;
  list.splice(insertIndex, 0, value);
  updateEncoding(db, key);
  return integerReply(list.length);
}

// --- LREM ---

export function lrem(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const countParsed = parseInteger(args[1] ?? '');
  if (countParsed.error) return countParsed.error;
  const element = args[2] ?? '';
  const count = countParsed.value;

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return ZERO;

  let removed = 0;

  if (count > 0) {
    for (let i = 0; i < list.length && removed < count; ) {
      if (list[i] === element) {
        list.splice(i, 1);
        removed++;
      } else {
        i++;
      }
    }
  } else if (count < 0) {
    const toRemove = -count;
    for (let i = list.length - 1; i >= 0 && removed < toRemove; i--) {
      if (list[i] === element) {
        list.splice(i, 1);
        removed++;
      }
    }
  } else {
    for (let i = 0; i < list.length; ) {
      if (list[i] === element) {
        list.splice(i, 1);
        removed++;
      } else {
        i++;
      }
    }
  }

  deleteIfEmpty(db, key, list);
  return integerReply(removed);
}

// --- LTRIM ---

export function ltrim(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const startParsed = parseInteger(args[1] ?? '');
  if (startParsed.error) return startParsed.error;
  const stopParsed = parseInteger(args[2] ?? '');
  if (stopParsed.error) return stopParsed.error;

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return OK;

  let start = resolveIndex(startParsed.value, list.length);
  let stop = resolveIndex(stopParsed.value, list.length);

  if (start < 0) start = 0;
  if (stop >= list.length) stop = list.length - 1;

  if (start > stop || start >= list.length) {
    list.length = 0;
    deleteIfEmpty(db, key, list);
    return OK;
  }

  const kept = list.slice(start, stop + 1);
  list.length = 0;
  list.push(...kept);
  deleteIfEmpty(db, key, list);
  return OK;
}

// --- LPOS ---

export function lpos(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const element = args[1] ?? '';

  let rank = 1;
  let count: number | null = null;
  let maxlen = 0;

  for (let i = 2; i < args.length; i += 2) {
    const option = (args[i] ?? '').toUpperCase();
    const valStr = args[i + 1];
    if (valStr === undefined) return SYNTAX_ERR;

    const parsed = parseInteger(valStr);
    if (parsed.error) return parsed.error;

    if (option === 'RANK') {
      if (parsed.value === 0) {
        return errorReply(
          'ERR',
          "RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative values meaning from the last match"
        );
      }
      rank = parsed.value;
    } else if (option === 'COUNT') {
      if (parsed.value < 0) {
        return errorReply('ERR', "COUNT can't be negative");
      }
      count = parsed.value;
    } else if (option === 'MAXLEN') {
      if (parsed.value < 0) {
        return errorReply('ERR', "MAXLEN can't be negative");
      }
      maxlen = parsed.value;
    } else {
      return SYNTAX_ERR;
    }
  }

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) {
    return count !== null ? EMPTY_ARRAY : NIL;
  }

  const results: number[] = [];
  const wantCount = count === 0 ? Infinity : (count ?? 1);
  let matchesSkipped = 0;
  const absRank = Math.abs(rank);
  const forward = rank > 0;

  if (forward) {
    const limit = maxlen > 0 ? Math.min(list.length, maxlen) : list.length;
    for (let i = 0; i < limit && results.length < wantCount; i++) {
      if (list[i] === element) {
        matchesSkipped++;
        if (matchesSkipped >= absRank) {
          results.push(i);
        }
      }
    }
  } else {
    const startIdx = list.length - 1;
    const limit = maxlen > 0 ? maxlen : list.length;
    let scanned = 0;
    for (
      let i = startIdx;
      i >= 0 && scanned < limit && results.length < wantCount;
      i--
    ) {
      scanned++;
      if (list[i] === element) {
        matchesSkipped++;
        if (matchesSkipped >= absRank) {
          results.push(i);
        }
      }
    }
  }

  if (count !== null) {
    return arrayReply(results.map((pos) => integerReply(pos)));
  }

  return results.length > 0 ? integerReply(results[0] ?? 0) : NIL;
}
