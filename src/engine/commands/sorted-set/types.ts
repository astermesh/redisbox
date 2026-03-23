import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import { WRONGTYPE_ERR } from '../../types.ts';
import { SkipList } from '../../skip-list.ts';
import { formatFloat } from '../incr.ts';
import {
  strByteLength,
  DEFAULT_MAX_LISTPACK_ENTRIES,
  DEFAULT_MAX_LISTPACK_VALUE,
} from '../../utils.ts';

function fitsListpack(
  dict: Map<string, number>,
  maxEntries: number = DEFAULT_MAX_LISTPACK_ENTRIES,
  maxValue: number = DEFAULT_MAX_LISTPACK_VALUE
): boolean {
  if (dict.size > maxEntries) return false;
  for (const member of dict.keys()) {
    if (strByteLength(member) > maxValue) return false;
  }
  return true;
}

export function chooseEncoding(
  dict: Map<string, number>
): 'listpack' | 'skiplist' {
  return fitsListpack(dict) ? 'listpack' : 'skiplist';
}

export function updateEncoding(db: Database, key: string): void {
  const entry = db.get(key);
  if (!entry || entry.type !== 'zset') return;
  if (entry.encoding === 'skiplist') return; // never demote
  const zset = entry.value as SortedSetData;
  if (!fitsListpack(zset.dict)) {
    entry.encoding = 'skiplist';
  }
}

export function formatScore(n: number): string {
  if (n === Infinity) return 'inf';
  if (n === -Infinity) return '-inf';
  return formatFloat(n);
}

export interface SortedSetData {
  sl: SkipList;
  dict: Map<string, number>;
}

export function getOrCreateZset(
  db: Database,
  key: string,
  rng: () => number
): { zset: SortedSetData; error: null } | { zset: null; error: Reply } {
  const entry = db.get(key);
  if (entry) {
    if (entry.type !== 'zset') return { zset: null, error: WRONGTYPE_ERR };
    return { zset: entry.value as SortedSetData, error: null };
  }
  const zset: SortedSetData = {
    sl: new SkipList(rng),
    dict: new Map(),
  };
  db.set(key, 'zset', 'listpack', zset);
  return { zset, error: null };
}

export function getExistingZset(
  db: Database,
  key: string
): { zset: SortedSetData | null; error: Reply | null } {
  const entry = db.get(key);
  if (!entry) return { zset: null, error: null };
  if (entry.type !== 'zset') return { zset: null, error: WRONGTYPE_ERR };
  return { zset: entry.value as SortedSetData, error: null };
}
