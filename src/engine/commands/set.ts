import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import {
  integerReply,
  bulkReply,
  arrayReply,
  errorReply,
  wrongArityError,
  ZERO,
  ONE,
  NIL,
  EMPTY_ARRAY,
  WRONGTYPE_ERR,
  NOT_INTEGER_ERR,
  SYNTAX_ERR,
} from '../types.ts';
import { parseInteger } from './incr.ts';
import { matchGlob } from '../glob-pattern.ts';
import { strByteLength, INT64_MIN, INT64_MAX, partialShuffle } from '../utils.ts';

// Default thresholds — match Redis defaults.
// TODO: read from ConfigStore when config is wired into CommandContext.
const DEFAULT_MAX_INTSET_ENTRIES = 512;
const DEFAULT_MAX_LISTPACK_ENTRIES = 128;
const DEFAULT_MAX_LISTPACK_VALUE = 64;

/**
 * Check if a string represents a valid integer for intset encoding.
 * Must be a canonical integer representation within 64-bit signed range.
 */
function isIntegerString(s: string): boolean {
  if (s.length === 0 || s.length > 20) return false;
  try {
    const n = BigInt(s);
    if (n < INT64_MIN || n > INT64_MAX) return false;
    // Canonical check: BigInt("007").toString() === "7" !== "007"
    return n.toString() === s;
  } catch {
    return false;
  }
}

/**
 * Check if all members of a set are valid integers for intset encoding.
 */
function allIntegers(s: Set<string>): boolean {
  for (const member of s) {
    if (!isIntegerString(member)) return false;
  }
  return true;
}

/**
 * Check if a set fits listpack encoding.
 */
function fitsListpack(
  s: Set<string>,
  maxEntries: number = DEFAULT_MAX_LISTPACK_ENTRIES,
  maxValue: number = DEFAULT_MAX_LISTPACK_VALUE
): boolean {
  if (s.size > maxEntries) return false;
  for (const member of s) {
    if (strByteLength(member) > maxValue) return false;
  }
  return true;
}

/**
 * Determine the best initial encoding for a set.
 */
function chooseInitialEncoding(
  s: Set<string>
): 'intset' | 'listpack' | 'hashtable' {
  if (s.size <= DEFAULT_MAX_INTSET_ENTRIES && allIntegers(s)) return 'intset';
  if (fitsListpack(s)) return 'listpack';
  return 'hashtable';
}

/**
 * Promote encoding if the current one no longer fits.
 * Encoding transitions are one-directional: intset → listpack → hashtable.
 * Never demotes.
 */
function updateEncoding(db: Database, key: string): void {
  const entry = db.get(key);
  if (!entry || entry.type !== 'set') return;

  const s = entry.value as Set<string>;

  if (entry.encoding === 'intset') {
    // Check if intset is still valid
    if (s.size <= DEFAULT_MAX_INTSET_ENTRIES && allIntegers(s)) return;
    // Promote: try listpack first, then hashtable
    entry.encoding = fitsListpack(s) ? 'listpack' : 'hashtable';
    return;
  }

  if (entry.encoding === 'listpack') {
    if (!fitsListpack(s)) {
      entry.encoding = 'hashtable';
    }
    return;
  }

  // hashtable — never demote
}

/**
 * Get or create a set entry. Returns the set and entry, or an error reply.
 * If the key doesn't exist, creates a new empty set.
 */
function getOrCreateSet(
  db: Database,
  key: string
): { set: Set<string>; error: null } | { set: null; error: Reply } {
  const entry = db.get(key);
  if (entry) {
    if (entry.type !== 'set') return { set: null, error: WRONGTYPE_ERR };
    return { set: entry.value as Set<string>, error: null };
  }
  const s = new Set<string>();
  db.set(key, 'set', 'intset', s);
  return { set: s, error: null };
}

/**
 * Get an existing set entry. Returns null if key doesn't exist.
 */
function getExistingSet(
  db: Database,
  key: string
): { set: Set<string> | null; error: Reply | null } {
  const entry = db.get(key);
  if (!entry) return { set: null, error: null };
  if (entry.type !== 'set') return { set: null, error: WRONGTYPE_ERR };
  return { set: entry.value as Set<string>, error: null };
}

// --- SADD ---

export function sadd(db: Database, args: string[]): Reply {
  if (args.length < 2) {
    return wrongArityError('sadd');
  }

  const key = args[0] ?? '';
  const { set: s, error } = getOrCreateSet(db, key);
  if (error) return error;

  let added = 0;
  for (let i = 1; i < args.length; i++) {
    const member = args[i] ?? '';
    if (!s.has(member)) {
      s.add(member);
      added++;
    }
  }

  if (added > 0) {
    updateEncoding(db, key);
  }

  return integerReply(added);
}

// --- SREM ---

export function srem(db: Database, args: string[]): Reply {
  if (args.length < 2) {
    return wrongArityError('srem');
  }

  const key = args[0] ?? '';

  const { set: s, error } = getExistingSet(db, key);
  if (error) return error;
  if (!s) return ZERO;

  let removed = 0;
  for (let i = 1; i < args.length; i++) {
    const member = args[i] ?? '';
    if (s.delete(member)) {
      removed++;
    }
  }

  if (s.size === 0) {
    db.delete(key);
  }

  return integerReply(removed);
}

// --- SISMEMBER ---

export function sismember(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const member = args[1] ?? '';

  const { set: s, error } = getExistingSet(db, key);
  if (error) return error;
  if (!s) return ZERO;

  return s.has(member) ? ONE : ZERO;
}

// --- SMISMEMBER ---

export function smismember(db: Database, args: string[]): Reply {
  if (args.length < 2) {
    return wrongArityError('smismember');
  }

  const key = args[0] ?? '';

  const { set: s, error } = getExistingSet(db, key);
  if (error) return error;

  const results: Reply[] = [];
  for (let i = 1; i < args.length; i++) {
    const member = args[i] ?? '';
    results.push(s && s.has(member) ? ONE : ZERO);
  }
  return arrayReply(results);
}

// --- SMEMBERS ---

export function smembers(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  const { set: s, error } = getExistingSet(db, key);
  if (error) return error;
  if (!s || s.size === 0) return EMPTY_ARRAY;

  const results: Reply[] = [];
  for (const member of s) {
    results.push(bulkReply(member));
  }
  return arrayReply(results);
}

// --- SCARD ---

export function scard(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  const { set: s, error } = getExistingSet(db, key);
  if (error) return error;
  if (!s) return ZERO;

  return integerReply(s.size);
}

// --- SMOVE ---

export function smove(db: Database, args: string[]): Reply {
  const source = args[0] ?? '';
  const destination = args[1] ?? '';
  const member = args[2] ?? '';

  // Check source
  const srcResult = getExistingSet(db, source);
  if (srcResult.error) return srcResult.error;
  if (!srcResult.set) return ZERO;

  // Check if member exists in source (must come before destination type check —
  // real Redis returns 0 for absent member even if destination is wrong type)
  if (!srcResult.set.has(member)) return ZERO;

  // Check destination type before modifying source
  const dstEntry = db.get(destination);
  if (dstEntry && dstEntry.type !== 'set') return WRONGTYPE_ERR;

  // Same key — member already exists, nothing to do
  if (source === destination) return ONE;

  // Remove from source
  srcResult.set.delete(member);
  if (srcResult.set.size === 0) {
    db.delete(source);
  }

  // Add to destination
  if (dstEntry) {
    (dstEntry.value as Set<string>).add(member);
    updateEncoding(db, destination);
  } else {
    const dstSet = new Set<string>();
    dstSet.add(member);
    db.set(destination, 'set', chooseInitialEncoding(dstSet), dstSet);
  }

  return ONE;
}

// --- SRANDMEMBER ---

export function srandmember(
  db: Database,
  args: string[],
  rng: () => number
): Reply {
  const key = args[0] ?? '';

  const { set: s, error } = getExistingSet(db, key);
  if (error) return error;

  // No count argument — return single member or nil
  if (args.length === 1) {
    if (!s || s.size === 0) return NIL;
    const members = Array.from(s);
    const idx = Math.floor(rng() * members.length);
    return bulkReply(members[idx] ?? '');
  }

  // Parse count
  const countStr = args[1] ?? '';
  const countParsed = parseInteger(countStr);
  if (countParsed === null) return NOT_INTEGER_ERR;
  const count = Number(countParsed);

  if (!s || s.size === 0) return EMPTY_ARRAY;
  if (count === 0) return EMPTY_ARRAY;

  const members = Array.from(s);
  const results: Reply[] = [];

  if (count > 0) {
    // Positive count: unique elements, at most set size
    const actual = Math.min(count, members.length);
    const shuffled = partialShuffle([...members], actual, rng);
    for (let i = 0; i < actual; i++) {
      results.push(bulkReply(shuffled[i] ?? ''));
    }
  } else {
    // Negative count: |count| elements, may repeat
    const absCount = Math.abs(count);
    for (let i = 0; i < absCount; i++) {
      const idx = Math.floor(rng() * members.length);
      results.push(bulkReply(members[idx] ?? ''));
    }
  }

  return arrayReply(results);
}

// --- SPOP ---

export function spop(db: Database, args: string[], rng: () => number): Reply {
  const key = args[0] ?? '';

  const { set: s, error } = getExistingSet(db, key);
  if (error) return error;

  // No count argument — return single member or nil
  if (args.length === 1) {
    if (!s || s.size === 0) return NIL;
    const members = Array.from(s);
    const idx = Math.floor(rng() * members.length);
    const member = members[idx] ?? '';
    s.delete(member);
    if (s.size === 0) db.delete(key);
    return bulkReply(member);
  }

  // Parse count
  const countStr = args[1] ?? '';
  const countParsed = parseInteger(countStr);
  if (countParsed === null) return NOT_INTEGER_ERR;
  const count = Number(countParsed);

  if (count < 0) {
    return errorReply('ERR', 'value is out of range, must be positive');
  }

  if (!s || s.size === 0) return EMPTY_ARRAY;
  if (count === 0) return EMPTY_ARRAY;

  const members = Array.from(s);
  const actual = Math.min(count, members.length);

  const shuffled = partialShuffle([...members], actual, rng);

  const results: Reply[] = [];
  for (let i = 0; i < actual; i++) {
    const member = shuffled[i] ?? '';
    s.delete(member);
    results.push(bulkReply(member));
  }

  if (s.size === 0) db.delete(key);

  return arrayReply(results);
}

// --- SSCAN ---

export function sscan(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const cursorStr = args[1] ?? '0';

  const cursor = parseInt(cursorStr, 10);
  if (isNaN(cursor) || cursor < 0) {
    return errorReply('ERR', 'invalid cursor');
  }

  // Check key type before parsing options
  const entry = db.get(key);
  if (entry && entry.type !== 'set') return WRONGTYPE_ERR;

  let matchPattern: string | null = null;
  let count = 10;

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
    } else {
      return SYNTAX_ERR;
    }
    i++;
  }

  if (!entry) {
    return arrayReply([bulkReply('0'), EMPTY_ARRAY]);
  }

  const s = entry.value as Set<string>;
  const allMembers = Array.from(s);

  if (allMembers.length === 0) {
    return arrayReply([bulkReply('0'), EMPTY_ARRAY]);
  }

  const results: Reply[] = [];
  let position = cursor;
  let scanned = 0;

  while (position < allMembers.length && scanned < count) {
    const member = allMembers[position] ?? '';
    position++;
    scanned++;

    if (matchPattern && !matchGlob(matchPattern, member)) continue;

    results.push(bulkReply(member));
  }

  const nextCursor = position >= allMembers.length ? 0 : position;

  return arrayReply([bulkReply(String(nextCursor)), arrayReply(results)]);
}
