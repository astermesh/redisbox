import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import {
  bulkReply,
  integerReply,
  arrayReply,
  wrongArityError,
  OK,
  NIL,
  ZERO,
  ONE,
  EMPTY_ARRAY,
  WRONGTYPE_ERR,
  NOT_INTEGER_ERR,
  NOT_FLOAT_ERR,
  INF_NAN_ERR,
  OVERFLOW_ERR,
  SYNTAX_ERR,
} from '../../types.ts';
import type { CommandSpec } from '../../command-table.ts';
import { parseInteger, parseFloat64, formatFloat } from '../incr.ts';
import { notify, EVENT_FLAGS } from '../../pubsub/notify.ts';
import { matchGlob } from '../../glob-pattern.ts';
import { partialShuffle, INT64_MAX, INT64_MIN } from '../../utils.ts';
import { parseScanCursor, parseScanOptions } from '../scan-utils.ts';
import type { ConfigStore } from '../../../config-store.ts';
import {
  getOrCreateHash,
  getExistingHash,
  updateEncoding,
  HASH_NOT_INTEGER_ERR,
  HASH_NOT_FLOAT_ERR,
} from './utils.ts';

// --- HSET ---

export function hset(
  db: Database,
  args: string[],
  config?: ConfigStore
): Reply {
  if (args.length < 3 || (args.length - 1) % 2 !== 0) {
    return wrongArityError('hset');
  }

  const key = args[0] ?? '';
  const { hash, error } = getOrCreateHash(db, key);
  if (error) return error;

  let added = 0;
  for (let i = 1; i < args.length; i += 2) {
    const field = args[i] ?? '';
    const value = args[i + 1] ?? '';
    db.tryExpireField(key, field);
    if (!hash.has(field)) added++;
    hash.set(field, value);
    db.removeFieldExpiry(key, field);
  }

  updateEncoding(db, key, config);
  return integerReply(added);
}

// --- HGET ---

export function hget(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const field = args[1] ?? '';

  const { hash, error } = getExistingHash(db, key);
  if (error) return error;
  if (!hash) return NIL;

  // Lazy field expiration
  db.tryExpireField(key, field);

  const value = hash.get(field);
  return value !== undefined ? bulkReply(value) : NIL;
}

// --- HMSET ---

export function hmset(
  db: Database,
  args: string[],
  config?: ConfigStore
): Reply {
  if (args.length < 3 || (args.length - 1) % 2 !== 0) {
    return wrongArityError('hmset');
  }

  const key = args[0] ?? '';
  const { hash, error } = getOrCreateHash(db, key);
  if (error) return error;

  for (let i = 1; i < args.length; i += 2) {
    const field = args[i] ?? '';
    const value = args[i + 1] ?? '';
    hash.set(field, value);
    db.removeFieldExpiry(key, field);
  }

  updateEncoding(db, key, config);
  return OK;
}

// --- HMGET ---

export function hmget(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  const { hash, error } = getExistingHash(db, key);
  if (error) return error;

  const results: Reply[] = [];
  for (let i = 1; i < args.length; i++) {
    const field = args[i] ?? '';
    if (hash) {
      // Lazy field expiration
      db.tryExpireField(key, field);
      const value = hash.get(field);
      results.push(value !== undefined ? bulkReply(value) : NIL);
    } else {
      results.push(NIL);
    }
  }
  return arrayReply(results);
}

// --- HGETALL ---

export function hgetall(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  const { hash, error } = getExistingHash(db, key);
  if (error) return error;
  if (!hash || hash.size === 0) return EMPTY_ARRAY;

  // Lazy field expiration for all fields
  db.expireHashFields(key);
  if (hash.size === 0) return EMPTY_ARRAY;

  const results: Reply[] = [];
  for (const [field, value] of hash) {
    results.push(bulkReply(field));
    results.push(bulkReply(value));
  }
  return arrayReply(results);
}

// --- HDEL ---

export function hdel(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  const { hash, error } = getExistingHash(db, key);
  if (error) return error;
  if (!hash) return ZERO;

  let deleted = 0;
  for (let i = 1; i < args.length; i++) {
    const field = args[i] ?? '';
    if (hash.delete(field)) {
      db.removeFieldExpiry(key, field);
      deleted++;
    }
  }

  // If hash is now empty, delete the key
  if (hash.size === 0) {
    db.delete(key);
  }

  return integerReply(deleted);
}

// --- HEXISTS ---

export function hexists(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const field = args[1] ?? '';

  const { hash, error } = getExistingHash(db, key);
  if (error) return error;
  if (!hash) return ZERO;

  // Lazy field expiration
  db.tryExpireField(key, field);

  return hash.has(field) ? ONE : ZERO;
}

// --- HLEN ---

export function hlen(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  const { hash, error } = getExistingHash(db, key);
  if (error) return error;
  if (!hash) return ZERO;

  return integerReply(hash.size);
}

// --- HKEYS ---

export function hkeys(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  const { hash, error } = getExistingHash(db, key);
  if (error) return error;
  if (!hash || hash.size === 0) return EMPTY_ARRAY;

  // Lazy field expiration for all fields
  db.expireHashFields(key);
  if (hash.size === 0) return EMPTY_ARRAY;

  const results: Reply[] = [];
  for (const field of hash.keys()) {
    results.push(bulkReply(field));
  }
  return arrayReply(results);
}

// --- HVALS ---

export function hvals(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  const { hash, error } = getExistingHash(db, key);
  if (error) return error;
  if (!hash || hash.size === 0) return EMPTY_ARRAY;

  // Lazy field expiration for all fields
  db.expireHashFields(key);
  if (hash.size === 0) return EMPTY_ARRAY;

  const results: Reply[] = [];
  for (const value of hash.values()) {
    results.push(bulkReply(value));
  }
  return arrayReply(results);
}

// --- HSETNX ---

export function hsetnx(
  db: Database,
  args: string[],
  config?: ConfigStore
): Reply {
  const key = args[0] ?? '';
  const field = args[1] ?? '';
  const value = args[2] ?? '';

  const { hash, error } = getOrCreateHash(db, key);
  if (error) return error;

  db.tryExpireField(key, field);
  if (hash.has(field)) return ZERO;

  hash.set(field, value);
  updateEncoding(db, key, config);
  return ONE;
}

// --- HINCRBY ---

export function hincrby(
  db: Database,
  args: string[],
  config?: ConfigStore
): Reply {
  const key = args[0] ?? '';
  const field = args[1] ?? '';
  const incrStr = args[2] ?? '';

  const delta = parseInteger(incrStr);
  if (delta === null) return NOT_INTEGER_ERR;

  const { hash, error } = getOrCreateHash(db, key);
  if (error) return error;

  db.tryExpireField(key, field);
  const currentStr = hash.get(field) ?? '0';
  const current = parseInteger(currentStr);
  if (current === null) return HASH_NOT_INTEGER_ERR;

  const result = current + delta;
  if (result > INT64_MAX || result < INT64_MIN) return OVERFLOW_ERR;

  hash.set(field, result.toString());
  db.removeFieldExpiry(key, field);
  updateEncoding(db, key, config);

  const replyValue =
    result >= -9007199254740991n && result <= 9007199254740991n
      ? Number(result)
      : result;
  return integerReply(replyValue);
}

// --- HINCRBYFLOAT ---

export function hincrbyfloat(
  db: Database,
  args: string[],
  config?: ConfigStore
): Reply {
  const key = args[0] ?? '';
  const field = args[1] ?? '';
  const incrStr = args[2] ?? '';

  const incrParsed = parseFloat64(incrStr);
  if (incrParsed === null) return NOT_FLOAT_ERR;
  if (incrParsed.isInf) return INF_NAN_ERR;

  const { hash, error } = getOrCreateHash(db, key);
  if (error) return error;

  db.tryExpireField(key, field);
  const currentStr = hash.get(field) ?? '0';
  const currentParsed = parseFloat64(currentStr);
  if (currentParsed === null) return HASH_NOT_FLOAT_ERR;
  if (currentParsed.isInf) return HASH_NOT_FLOAT_ERR;

  const result = currentParsed.value + incrParsed.value;
  if (!isFinite(result)) return INF_NAN_ERR;

  const strResult = formatFloat(result);
  hash.set(field, strResult);
  db.removeFieldExpiry(key, field);
  updateEncoding(db, key, config);

  return bulkReply(strResult);
}

// --- HRANDFIELD ---

export function hrandfield(
  db: Database,
  args: string[],
  rng: () => number
): Reply {
  const key = args[0] ?? '';

  const { hash, error } = getExistingHash(db, key);
  if (error) return error;

  // Bulk-expire all expired fields before random selection (Redis behavior)
  if (hash) db.expireHashFields(key);

  // No count argument — return single field or nil
  if (args.length === 1) {
    if (!hash || hash.size === 0) return NIL;
    const fields = Array.from(hash.keys());
    const idx = Math.floor(rng() * fields.length);
    return bulkReply(fields[idx] ?? '');
  }

  // Parse count
  const countStr = args[1] ?? '';
  const countParsed = parseInteger(countStr);
  if (countParsed === null) return NOT_INTEGER_ERR;
  const count = Number(countParsed);

  // Check for WITHVALUES
  let withValues = false;
  if (args.length > 2) {
    const flag = (args[2] ?? '').toUpperCase();
    if (flag !== 'WITHVALUES') return SYNTAX_ERR;
    if (args.length > 3) return SYNTAX_ERR;
    withValues = true;
  }

  if (!hash || hash.size === 0) return EMPTY_ARRAY;

  if (count === 0) return EMPTY_ARRAY;

  const fields = Array.from(hash.keys());
  const results: Reply[] = [];

  if (count > 0) {
    // Positive count: unique elements, at most hash size
    const actual = Math.min(count, fields.length);
    const shuffled = partialShuffle([...fields], actual, rng);
    for (let i = 0; i < actual; i++) {
      const f = shuffled[i] ?? '';
      results.push(bulkReply(f));
      if (withValues) {
        results.push(bulkReply(hash.get(f) ?? ''));
      }
    }
  } else {
    // Negative count: |count| elements, may repeat
    const absCount = Math.abs(count);
    for (let i = 0; i < absCount; i++) {
      const idx = Math.floor(rng() * fields.length);
      const f = fields[idx] ?? '';
      results.push(bulkReply(f));
      if (withValues) {
        results.push(bulkReply(hash.get(f) ?? ''));
      }
    }
  }

  return arrayReply(results);
}

// --- HSCAN ---

export function hscan(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const { cursor, error: cursorErr } = parseScanCursor(args[1] ?? '0');
  if (cursorErr) return cursorErr;

  // Check key type before parsing options
  const entry = db.get(key);
  if (entry && entry.type !== 'hash') return WRONGTYPE_ERR;

  let noValues = false;

  const { options, error: optErr } = parseScanOptions(
    args,
    2,
    (flag, _a, i) => {
      if (flag === 'NOVALUES') {
        noValues = true;
        return i;
      }
      return null;
    }
  );
  if (optErr) return optErr;

  const { matchPattern, count } = options;

  if (!entry) {
    return arrayReply([bulkReply('0'), EMPTY_ARRAY]);
  }

  const hash = entry.value as Map<string, string>;
  const allFields = Array.from(hash.keys());

  if (allFields.length === 0) {
    return arrayReply([bulkReply('0'), EMPTY_ARRAY]);
  }

  const results: Reply[] = [];
  let position = cursor;
  let scanned = 0;

  while (position < allFields.length && scanned < count) {
    const field = allFields[position] ?? '';
    position++;
    scanned++;

    // Skip expired fields (lazy field expiration)
    if (db.tryExpireField(key, field)) continue;

    if (matchPattern && !matchGlob(matchPattern, field)) continue;

    results.push(bulkReply(field));
    if (!noValues) {
      results.push(bulkReply(hash.get(field) ?? ''));
    }
  }

  const nextCursor = position >= allFields.length ? 0 : position;

  return arrayReply([bulkReply(String(nextCursor)), arrayReply(results)]);
}

export const specs: CommandSpec[] = [
  {
    name: 'hset',
    handler: (ctx, args) => {
      const reply = hset(ctx.db, args, ctx.config);
      if (reply.kind === 'integer') {
        notify(ctx, EVENT_FLAGS.HASH, 'hset', args[0] ?? '');
      }
      return reply;
    },
    arity: -4,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@hash', '@fast'],
  },
  {
    name: 'hget',
    handler: (ctx, args) => hget(ctx.db, args),
    arity: 3,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@hash', '@fast'],
  },
  {
    name: 'hmset',
    handler: (ctx, args) => {
      const reply = hmset(ctx.db, args, ctx.config);
      if (reply === OK) {
        notify(ctx, EVENT_FLAGS.HASH, 'hset', args[0] ?? '');
      }
      return reply;
    },
    arity: -4,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@hash', '@fast'],
  },
  {
    name: 'hmget',
    handler: (ctx, args) => hmget(ctx.db, args),
    arity: -3,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@hash', '@fast'],
  },
  {
    name: 'hgetall',
    handler: (ctx, args) => hgetall(ctx.db, args),
    arity: 2,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@hash'],
  },
  {
    name: 'hdel',
    handler: (ctx, args) => {
      const reply = hdel(ctx.db, args);
      if (reply.kind === 'integer' && (reply.value as number) > 0) {
        notify(ctx, EVENT_FLAGS.HASH, 'hdel', args[0] ?? '');
      }
      return reply;
    },
    arity: -3,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@hash', '@fast'],
  },
  {
    name: 'hexists',
    handler: (ctx, args) => hexists(ctx.db, args),
    arity: 3,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@hash', '@fast'],
  },
  {
    name: 'hlen',
    handler: (ctx, args) => hlen(ctx.db, args),
    arity: 2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@hash', '@fast'],
  },
  {
    name: 'hkeys',
    handler: (ctx, args) => hkeys(ctx.db, args),
    arity: 2,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@hash'],
  },
  {
    name: 'hvals',
    handler: (ctx, args) => hvals(ctx.db, args),
    arity: 2,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@hash'],
  },
  {
    name: 'hsetnx',
    handler: (ctx, args) => {
      const reply = hsetnx(ctx.db, args, ctx.config);
      if (reply === ONE) {
        notify(ctx, EVENT_FLAGS.HASH, 'hset', args[0] ?? '');
      }
      return reply;
    },
    arity: 4,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@hash', '@fast'],
  },
  {
    name: 'hincrby',
    handler: (ctx, args) => {
      const reply = hincrby(ctx.db, args, ctx.config);
      if (reply.kind === 'integer') {
        notify(ctx, EVENT_FLAGS.HASH, 'hincrby', args[0] ?? '');
      }
      return reply;
    },
    arity: 4,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@hash', '@fast'],
  },
  {
    name: 'hincrbyfloat',
    handler: (ctx, args) => {
      const reply = hincrbyfloat(ctx.db, args, ctx.config);
      if (reply.kind === 'bulk' && reply.value !== null) {
        notify(ctx, EVENT_FLAGS.HASH, 'hincrbyfloat', args[0] ?? '');
      }
      return reply;
    },
    arity: 4,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@hash', '@fast'],
  },
  {
    name: 'hrandfield',
    handler: (ctx, args) => hrandfield(ctx.db, args, ctx.engine.rng),
    arity: -2,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@hash'],
  },
  {
    name: 'hscan',
    handler: (ctx, args) => hscan(ctx.db, args),
    arity: -3,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@hash'],
  },
];
