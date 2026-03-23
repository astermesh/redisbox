import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
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
} from '../../types.ts';
import { parseInteger } from '../incr.ts';
import { matchGlob } from '../../glob-pattern.ts';
import { partialShuffle } from '../../utils.ts';
import type { CommandSpec } from '../../command-table.ts';
import { notify, EVENT_FLAGS } from '../../notify.ts';
import { parseScanCursor, parseScanOptions } from '../scan-utils.ts';
import {
  getOrCreateSet,
  getExistingSet,
  updateEncoding,
  chooseInitialEncoding,
} from './utils.ts';
import { specs as opsSpecs } from './ops.ts';

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
  const { cursor, error: cursorErr } = parseScanCursor(args[1] ?? '0');
  if (cursorErr) return cursorErr;

  // Check key type before parsing options
  const entry = db.get(key);
  if (entry && entry.type !== 'set') return WRONGTYPE_ERR;

  const { options, error: optErr } = parseScanOptions(args, 2);
  if (optErr) return optErr;

  const { matchPattern, count } = options;

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

export const specs: CommandSpec[] = [
  {
    name: 'sadd',
    handler: (ctx, args) => {
      const reply = sadd(ctx.db, args);
      if (reply.kind === 'integer' && (reply.value as number) > 0) {
        notify(ctx, EVENT_FLAGS.SET, 'sadd', args[0] ?? '');
      }
      return reply;
    },
    arity: -3,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@set', '@fast'],
  },
  {
    name: 'srem',
    handler: (ctx, args) => {
      const reply = srem(ctx.db, args);
      if (reply.kind === 'integer' && (reply.value as number) > 0) {
        notify(ctx, EVENT_FLAGS.SET, 'srem', args[0] ?? '');
      }
      return reply;
    },
    arity: -3,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@set', '@fast'],
  },
  {
    name: 'sismember',
    handler: (ctx, args) => sismember(ctx.db, args),
    arity: 3,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@set', '@fast'],
  },
  {
    name: 'smismember',
    handler: (ctx, args) => smismember(ctx.db, args),
    arity: -3,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@set', '@fast'],
  },
  {
    name: 'smembers',
    handler: (ctx, args) => smembers(ctx.db, args),
    arity: 2,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@set'],
  },
  {
    name: 'scard',
    handler: (ctx, args) => scard(ctx.db, args),
    arity: 2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@set', '@fast'],
  },
  {
    name: 'smove',
    handler: (ctx, args) => {
      const reply = smove(ctx.db, args);
      if (reply === ONE) {
        notify(ctx, EVENT_FLAGS.SET, 'srem', args[0] ?? '');
        notify(ctx, EVENT_FLAGS.SET, 'sadd', args[1] ?? '');
      }
      return reply;
    },
    arity: 4,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 2,
    keyStep: 1,
    categories: ['@write', '@set', '@fast'],
  },
  {
    name: 'srandmember',
    handler: (ctx, args) => srandmember(ctx.db, args, ctx.engine.rng),
    arity: -2,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@set'],
  },
  {
    name: 'spop',
    handler: (ctx, args) => {
      const reply = spop(ctx.db, args, ctx.engine.rng);
      if (reply !== NIL && reply.kind !== 'error') {
        notify(ctx, EVENT_FLAGS.SET, 'spop', args[0] ?? '');
      }
      return reply;
    },
    arity: -2,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@set', '@fast'],
  },
  {
    name: 'sscan',
    handler: (ctx, args) => sscan(ctx.db, args),
    arity: -3,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@set'],
  },
  ...opsSpecs,
];
