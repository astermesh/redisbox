import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import {
  bulkReply,
  errorReply,
  integerReply,
  WRONGTYPE_ERR,
  ZERO,
  OK,
  SYNTAX_ERR,
} from '../../types.ts';
import { RedisStream, parseStreamId, compareStreamIds } from '../../stream.ts';
import type { StreamId } from '../../stream.ts';
import { getStream, INVALID_STREAM_ID_ERR } from './utils.ts';

interface TrimOptions {
  strategy: 'maxlen' | 'minid';
  approximate: boolean;
  threshold: string;
}

/**
 * Parse trim options from XADD args starting at position `i`.
 * Returns the parsed options and the new position, or an error.
 */
export function parseTrimOptions(
  args: string[],
  i: number
): { options: TrimOptions; nextIdx: number } | { error: Reply } {
  const strategyArg = (args[i] ?? '').toUpperCase();
  let strategy: 'maxlen' | 'minid';

  if (strategyArg === 'MAXLEN') {
    strategy = 'maxlen';
  } else if (strategyArg === 'MINID') {
    strategy = 'minid';
  } else {
    return { error: SYNTAX_ERR };
  }
  i++;

  let approximate = false;
  const nextArg = args[i] ?? '';
  if (nextArg === '~') {
    approximate = true;
    i++;
  } else if (nextArg === '=') {
    approximate = false;
    i++;
  }

  const threshold = args[i];
  if (threshold === undefined) {
    return { error: SYNTAX_ERR };
  }
  i++;

  // Optional LIMIT count (only meaningful with ~, but always accepted)
  if (i < args.length && (args[i] as string).toUpperCase() === 'LIMIT') {
    i++;
    const limitVal = args[i];
    if (limitVal === undefined) {
      return { error: SYNTAX_ERR };
    }
    const n = Number(limitVal);
    if (!Number.isInteger(n) || n < 0) {
      return {
        error: errorReply('ERR', 'value is not an integer or out of range'),
      };
    }
    i++;
  }

  return { options: { strategy, approximate, threshold }, nextIdx: i };
}

/**
 * Apply trim options to a stream.
 */
export function applyTrim(
  stream: RedisStream,
  options: TrimOptions
): Reply | null {
  if (options.strategy === 'maxlen') {
    const maxlen = Number(options.threshold);
    if (!Number.isInteger(maxlen) || maxlen < 0) {
      return errorReply('ERR', 'value is not an integer or out of range');
    }
    stream.trimByMaxlen(maxlen, options.approximate);
  } else {
    // MINID
    const minId = parseStreamId(options.threshold);
    if (!minId) {
      return errorReply(
        'ERR',
        'Invalid stream ID specified as stream command argument'
      );
    }
    stream.trimByMinid(minId, options.approximate);
  }
  return null;
}

/**
 * XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] *|id field value [field value ...]
 */
export function xadd(db: Database, clockMs: number, args: string[]): Reply {
  if (args.length < 4) {
    return errorReply('ERR', "wrong number of arguments for 'xadd' command");
  }

  const key = args[0] as string;
  let i = 1;
  let nomkstream = false;
  let trimOptions: TrimOptions | null = null;

  // Parse optional flags before the ID
  while (i < args.length) {
    const arg = (args[i] as string).toUpperCase();

    if (arg === 'NOMKSTREAM') {
      nomkstream = true;
      i++;
      continue;
    }

    if (arg === 'MAXLEN' || arg === 'MINID') {
      const result = parseTrimOptions(args, i);
      if ('error' in result) return result.error;
      trimOptions = result.options;
      i = result.nextIdx;
      continue;
    }

    // Not a recognized option — must be the ID
    break;
  }

  // args[i] is the ID
  const idArg = args[i];
  if (idArg === undefined) {
    return errorReply('ERR', "wrong number of arguments for 'xadd' command");
  }
  i++;

  // Remaining args are field-value pairs
  const fieldArgs = args.slice(i);
  if (fieldArgs.length === 0 || fieldArgs.length % 2 !== 0) {
    return errorReply('ERR', "wrong number of arguments for 'xadd' command");
  }

  // Check existing key type
  const existing = getStream(db, key);
  if (existing.error) return existing.error;

  let stream = existing.stream;

  // NOMKSTREAM: if key doesn't exist, don't create it
  if (!stream && nomkstream) {
    return bulkReply(null);
  }

  // Create stream if needed
  if (!stream) {
    stream = new RedisStream();
    db.set(key, 'stream', 'stream', stream);
  }

  // Resolve the ID
  const resolved = stream.resolveNextId(idArg, clockMs);
  if ('error' in resolved) {
    const errMsg = resolved.error;
    const dashIdx = errMsg.indexOf(' ');
    const prefix = errMsg.substring(0, dashIdx);
    const message = errMsg.substring(dashIdx + 1);
    return errorReply(prefix, message);
  }

  // Build fields array
  const fields: [string, string][] = [];
  for (let j = 0; j < fieldArgs.length; j += 2) {
    fields.push([fieldArgs[j] as string, fieldArgs[j + 1] as string]);
  }

  // Add entry
  const idStr = stream.addEntry(resolved, fields);

  // Apply trimming if specified
  if (trimOptions) {
    const trimErr = applyTrim(stream, trimOptions);
    if (trimErr) return trimErr;
  }

  return bulkReply(idStr);
}

/**
 * XLEN key
 */
export function xlen(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const entry = db.get(key);
  if (!entry) return ZERO;
  if (entry.type !== 'stream') return WRONGTYPE_ERR;
  const stream = entry.value as RedisStream;
  return integerReply(stream.length);
}

/**
 * XDEL key id [id ...]
 */
export function xdel(db: Database, args: string[]): Reply {
  if (args.length < 2) {
    return errorReply('ERR', "wrong number of arguments for 'xdel' command");
  }

  const key = args[0] as string;
  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;
  if (!lookup.stream) return ZERO;

  const ids: StreamId[] = [];
  for (let i = 1; i < args.length; i++) {
    const parsed = parseStreamId(args[i] as string);
    if (!parsed) return INVALID_STREAM_ID_ERR;
    ids.push(parsed);
  }

  const deleted = lookup.stream.deleteEntries(ids);
  return integerReply(deleted);
}

/**
 * XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]
 */
export function xtrim(db: Database, args: string[]): Reply {
  if (args.length < 3) {
    return errorReply('ERR', "wrong number of arguments for 'xtrim' command");
  }

  const key = args[0] as string;
  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;
  if (!lookup.stream) return ZERO;

  const result = parseTrimOptions(args, 1);
  if ('error' in result) return result.error;

  // Ensure no extra args after trim options
  if (result.nextIdx < args.length) {
    return SYNTAX_ERR;
  }

  const stream = lookup.stream;
  const lengthBefore = stream.length;
  const trimErr = applyTrim(stream, result.options);
  if (trimErr) return trimErr;
  return integerReply(lengthBefore - stream.length);
}

/**
 * XSETID key last-id [ENTRIESADDED entries-added] [MAXDELETEDID max-deleted-id]
 */
export function xsetid(db: Database, args: string[]): Reply {
  if (args.length < 2) {
    return errorReply('ERR', "wrong number of arguments for 'xsetid' command");
  }

  const key = args[0] as string;
  const idArg = args[1] as string;

  const newId = parseStreamId(idArg);
  if (!newId) return INVALID_STREAM_ID_ERR;

  let entriesAdded = -1;
  let maxDeletedId: StreamId | null = null;

  let i = 2;
  while (i < args.length) {
    const upper = (args[i] as string).toUpperCase();
    if (upper === 'ENTRIESADDED') {
      i++;
      const val = args[i];
      if (val === undefined) return SYNTAX_ERR;
      const n = Number(val);
      if (!Number.isInteger(n) || n < 0) {
        return errorReply('ERR', 'value is not an integer or out of range');
      }
      entriesAdded = n;
      i++;
    } else if (upper === 'MAXDELETEDID') {
      i++;
      const val = args[i];
      if (val === undefined) return SYNTAX_ERR;
      const parsed = parseStreamId(val);
      if (!parsed) return INVALID_STREAM_ID_ERR;
      maxDeletedId = parsed;
      i++;
    } else {
      return SYNTAX_ERR;
    }
  }

  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;

  let stream = lookup.stream;
  if (!stream) {
    stream = new RedisStream();
    db.set(key, 'stream', 'stream', stream);
  }

  // The new ID must be >= current last ID
  if (compareStreamIds(newId, stream.lastId) < 0) {
    return errorReply(
      'ERR',
      'The ID specified in XSETID is smaller than the target stream top item'
    );
  }

  stream.setLastId(newId);

  if (entriesAdded >= 0) {
    stream.setEntriesAdded(entriesAdded);
  }
  if (maxDeletedId) {
    stream.setMaxDeletedEntryId(maxDeletedId);
  }

  return OK;
}
