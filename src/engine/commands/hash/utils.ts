import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import { errorReply, WRONGTYPE_ERR } from '../../types.ts';
import type { ConfigStore } from '../../../config-store.ts';
import {
  strByteLength,
  configInt,
  DEFAULT_MAX_LISTPACK_ENTRIES,
  DEFAULT_MAX_LISTPACK_VALUE,
} from '../../utils.ts';

export const HASH_NOT_INTEGER_ERR = errorReply(
  'ERR',
  'hash value is not an integer'
);
export const HASH_NOT_FLOAT_ERR = errorReply(
  'ERR',
  'hash value is not a valid float'
);

/**
 * Check if a hash should use listpack encoding based on current state.
 * Returns true if the hash is small enough for listpack.
 */
export function fitsListpack(
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
export function getOrCreateHash(
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
export function getExistingHash(
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
export function updateEncoding(
  db: Database,
  key: string,
  config?: ConfigStore
): void {
  const entry = db.get(key);
  if (!entry || entry.type !== 'hash') return;
  if (entry.encoding === 'hashtable') return; // never demote
  const hash = entry.value as Map<string, string>;
  const maxEntries = configInt(
    config,
    'hash-max-listpack-entries',
    DEFAULT_MAX_LISTPACK_ENTRIES
  );
  const maxValue = configInt(
    config,
    'hash-max-listpack-value',
    DEFAULT_MAX_LISTPACK_VALUE
  );
  if (!fitsListpack(hash, maxEntries, maxValue)) {
    entry.encoding = 'hashtable';
  }
}
