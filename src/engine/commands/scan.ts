import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import { bulkReply, arrayReply, EMPTY_ARRAY } from '../types.ts';
import { matchGlob } from '../glob-pattern.ts';
import type { CommandSpec } from '../command-table.ts';
import { parseScanCursor, parseScanOptions } from './scan-utils.ts';

export function keys(db: Database, args: string[]): Reply {
  const pattern = args[0] ?? '*';
  const result: Reply[] = [];

  for (const key of db.keys()) {
    if (!db.has(key)) continue;
    if (matchGlob(pattern, key)) {
      result.push(bulkReply(key));
    }
  }
  return arrayReply(result);
}

export function scan(db: Database, args: string[]): Reply {
  const { cursor, error: cursorErr } = parseScanCursor(args[0] ?? '0');
  if (cursorErr) return cursorErr;

  let typeFilter: string | null = null;

  const { options, error: optErr } = parseScanOptions(args, 1, (flag, a, i) => {
    if (flag === 'TYPE') {
      i++;
      typeFilter = (a[i] ?? '').toLowerCase();
      return i;
    }
    return null;
  });
  if (optErr) return optErr;

  const { matchPattern, count } = options;

  const allKeys: string[] = [];
  for (const key of db.keys()) {
    if (db.has(key)) {
      allKeys.push(key);
    }
  }

  if (allKeys.length === 0) {
    return arrayReply([bulkReply('0'), EMPTY_ARRAY]);
  }

  const result: Reply[] = [];
  let position = cursor;
  let scanned = 0;

  while (position < allKeys.length && scanned < count) {
    const key = allKeys[position] ?? '';
    position++;
    scanned++;

    if (typeFilter) {
      const entry = db.getWithoutTouch(key);
      if (!entry || entry.type !== typeFilter) continue;
    }

    if (matchPattern && !matchGlob(matchPattern, key)) continue;

    result.push(bulkReply(key));
  }

  const nextCursor = position >= allKeys.length ? 0 : position;

  return arrayReply([bulkReply(String(nextCursor)), arrayReply(result)]);
}

export const specs: CommandSpec[] = [
  {
    name: 'keys',
    handler: (ctx, args) => keys(ctx.db, args),
    arity: 2,
    flags: ['readonly'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@keyspace', '@read'],
  },
  {
    name: 'scan',
    handler: (ctx, args) => scan(ctx.db, args),
    arity: -2,
    flags: ['readonly'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@keyspace', '@read'],
  },
];
