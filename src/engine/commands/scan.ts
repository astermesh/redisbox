import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import {
  bulkReply,
  arrayReply,
  errorReply,
  EMPTY_ARRAY,
  SYNTAX_ERR,
  NOT_INTEGER_ERR,
} from '../types.ts';
import { matchGlob } from '../glob-pattern.ts';

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
  const cursor = parseInt(args[0] ?? '0', 10);
  if (isNaN(cursor) || cursor < 0) {
    return errorReply('ERR', 'invalid cursor');
  }

  let matchPattern: string | null = null;
  let count = 10;
  let typeFilter: string | null = null;

  let i = 1;
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
    } else if (flag === 'TYPE') {
      i++;
      typeFilter = (args[i] ?? '').toLowerCase();
    } else {
      return SYNTAX_ERR;
    }
    i++;
  }

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
