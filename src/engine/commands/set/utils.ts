import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import { integerReply, ZERO, WRONGTYPE_ERR } from '../../types.ts';
import {
  strByteLength,
  INT64_MIN,
  INT64_MAX,
  DEFAULT_MAX_LISTPACK_ENTRIES,
  DEFAULT_MAX_LISTPACK_VALUE,
} from '../../utils.ts';

export const DEFAULT_MAX_INTSET_ENTRIES = 512;

export function isIntegerString(s: string): boolean {
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

export function allIntegers(s: Set<string>): boolean {
  for (const member of s) {
    if (!isIntegerString(member)) return false;
  }
  return true;
}

export function fitsListpack(
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

export function chooseInitialEncoding(
  s: Set<string>
): 'intset' | 'listpack' | 'hashtable' {
  if (s.size <= DEFAULT_MAX_INTSET_ENTRIES && allIntegers(s)) return 'intset';
  if (fitsListpack(s)) return 'listpack';
  return 'hashtable';
}

export function updateEncoding(db: Database, key: string): void {
  const entry = db.get(key);
  if (!entry || entry.type !== 'set') return;

  const s = entry.value as Set<string>;

  if (entry.encoding === 'intset') {
    if (s.size <= DEFAULT_MAX_INTSET_ENTRIES && allIntegers(s)) return;
    entry.encoding = fitsListpack(s) ? 'listpack' : 'hashtable';
    return;
  }

  if (entry.encoding === 'listpack') {
    if (!fitsListpack(s)) {
      entry.encoding = 'hashtable';
    }
    return;
  }
}

export function getOrCreateSet(
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

export function getExistingSet(
  db: Database,
  key: string
): { set: Set<string> | null; error: Reply | null } {
  const entry = db.get(key);
  if (!entry) return { set: null, error: null };
  if (entry.type !== 'set') return { set: null, error: WRONGTYPE_ERR };
  return { set: entry.value as Set<string>, error: null };
}

export function collectSets(
  db: Database,
  keys: string[]
): { sets: (Set<string> | null)[]; error: Reply | null } {
  const sets: (Set<string> | null)[] = [];
  for (const key of keys) {
    const result = getExistingSet(db, key);
    if (result.error) return { sets: [], error: result.error };
    sets.push(result.set);
  }
  return { sets, error: null };
}

export function findSmallest(sets: Set<string>[]): Set<string> {
  let smallest = sets[0] as Set<string>;
  for (let i = 1; i < sets.length; i++) {
    const s = sets[i] as Set<string>;
    if (s.size < smallest.size) {
      smallest = s;
    }
  }
  return smallest;
}

export function computeIntersection(
  sets: (Set<string> | null)[]
): Set<string> | null {
  for (const s of sets) {
    if (!s) return null;
  }
  const nonNull = sets as Set<string>[];
  const smallest = findSmallest(nonNull);
  const result = new Set<string>();
  for (const member of smallest) {
    let inAll = true;
    for (const s of nonNull) {
      if (s !== smallest && !s.has(member)) {
        inAll = false;
        break;
      }
    }
    if (inAll) result.add(member);
  }
  return result;
}

export function computeDifference(sets: (Set<string> | null)[]): Set<string> {
  const first = sets[0];
  if (!first) return new Set();
  const result = new Set<string>();
  for (const member of first) {
    let inOther = false;
    for (let i = 1; i < sets.length; i++) {
      const s = sets[i];
      if (s && s.has(member)) {
        inOther = true;
        break;
      }
    }
    if (!inOther) result.add(member);
  }
  return result;
}

export function storeSetResult(
  db: Database,
  destination: string,
  members: Set<string>
): Reply {
  if (members.size === 0) {
    db.delete(destination);
    return ZERO;
  }
  const encoding = chooseInitialEncoding(members);
  db.set(destination, 'set', encoding, members);
  db.removeExpiry(destination);
  return integerReply(members.size);
}
