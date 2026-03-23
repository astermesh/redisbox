import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import { WRONGTYPE_ERR, NOT_INTEGER_ERR } from '../../types.ts';
import type { ConfigStore } from '../../../config-store.ts';

import {
  strByteLength,
  configInt,
  DEFAULT_MAX_LISTPACK_ENTRIES,
  DEFAULT_MAX_LISTPACK_VALUE,
} from '../../utils.ts';

/**
 * Map negative list-max-listpack-size fill factors to byte limits.
 * Matches Redis behavior for quicklist node sizing.
 */
const FILL_TO_BYTES: Record<number, number> = {
  [-1]: 4096,
  [-2]: 8192,
  [-3]: 16384,
  [-4]: 32768,
  [-5]: 65536,
};

/**
 * Check if a list should use listpack encoding based on current state.
 *
 * Supports Redis `list-max-listpack-size` semantics:
 * - Positive value: max number of entries in the listpack
 * - Negative values (-1 to -5): max total byte size per listpack node
 *
 * When called with legacy (maxEntries, maxValue) parameters, uses the
 * old entry-count + element-size logic for backward compatibility.
 */
export function fitsListpack(
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
 * Check if a list fits in a listpack using list-max-listpack-size semantics.
 */
export function fitsListpackBySize(
  items: string[],
  maxListpackSize: number
): boolean {
  if (maxListpackSize >= 0) {
    return items.length <= maxListpackSize;
  }
  const limit = FILL_TO_BYTES[maxListpackSize];
  if (limit === undefined) return false;
  let totalBytes = 0;
  for (const item of items) {
    totalBytes += strByteLength(item);
    if (totalBytes > limit) return false;
  }
  return true;
}

/**
 * Get or create a list entry. Returns the list array and entry, or an error reply.
 * If the key doesn't exist, creates a new empty list.
 */
export function getOrCreateList(
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
export function getExistingList(
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
export function updateEncoding(
  db: Database,
  key: string,
  config?: ConfigStore
): void {
  const entry = db.get(key);
  if (!entry || entry.type !== 'list') return;
  if (entry.encoding === 'quicklist') return; // never demote
  const items = entry.value as string[];
  if (config) {
    const maxListpackSize = configInt(config, 'list-max-listpack-size', -2);
    if (!fitsListpackBySize(items, maxListpackSize)) {
      entry.encoding = 'quicklist';
    }
  } else {
    if (!fitsListpack(items)) {
      entry.encoding = 'quicklist';
    }
  }
}

/**
 * Delete the key if the list is empty.
 */
export function deleteIfEmpty(
  db: Database,
  key: string,
  items: string[]
): void {
  if (items.length === 0) {
    db.delete(key);
  }
}

/**
 * Parse count argument for LPOP/RPOP. Returns parsed count or error reply.
 */
export function parseCount(
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

/**
 * Parse a string as an integer, returning NOT_INTEGER_ERR on failure.
 */
export function parseInteger(
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
export function resolveIndex(index: number, length: number): number {
  if (index < 0) index += length;
  return index;
}
