import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import {
  integerReply,
  arrayReply,
  errorReply,
  NOT_INTEGER_ERR,
  WRONGTYPE_ERR,
} from '../types.ts';
import type { CommandSpec } from '../command-table.ts';

/**
 * Parse the FIELDS numfields field... portion of hash TTL commands.
 * startIndex is the position of the FIELDS keyword in the args array.
 */
function parseFields(
  args: string[],
  startIndex: number
): { fields: string[]; error: Reply | null } {
  const keyword = args[startIndex];
  if (!keyword || keyword.toUpperCase() !== 'FIELDS') {
    return {
      fields: [],
      error: errorReply(
        'ERR',
        'Mandatory argument FIELDS is missing or not at the right position'
      ),
    };
  }

  const numFieldsStr = args[startIndex + 1];
  if (numFieldsStr === undefined) {
    return { fields: [], error: NOT_INTEGER_ERR };
  }

  const numFields = parseInt(numFieldsStr, 10);
  if (isNaN(numFields)) {
    return { fields: [], error: NOT_INTEGER_ERR };
  }

  if (numFields <= 0) {
    return {
      fields: [],
      error: errorReply(
        'ERR',
        'Parameter `numfields` should be greater than 0'
      ),
    };
  }

  const fieldsStart = startIndex + 2;
  const actualFields = args.slice(fieldsStart);

  if (actualFields.length !== numFields) {
    return {
      fields: [],
      error: errorReply(
        'ERR',
        `Parameter \`numfields\` is more than the number of arguments`
      ),
    };
  }

  return { fields: actualFields, error: null };
}

/**
 * Parse optional NX/XX/GT/LT flags for hash expire commands.
 * Returns the flag found (if any) and the index where FIELDS keyword starts.
 */
function parseExpireFlags(
  args: string[],
  flagStartIndex: number
): {
  nx: boolean;
  xx: boolean;
  gt: boolean;
  lt: boolean;
  fieldsIndex: number;
  error: Reply | null;
} {
  let nx = false;
  let xx = false;
  let gt = false;
  let lt = false;
  let fieldsIndex = flagStartIndex;

  const candidate = args[flagStartIndex];
  if (candidate && candidate.toUpperCase() !== 'FIELDS') {
    switch (candidate.toUpperCase()) {
      case 'NX':
        nx = true;
        break;
      case 'XX':
        xx = true;
        break;
      case 'GT':
        gt = true;
        break;
      case 'LT':
        lt = true;
        break;
      default:
        return {
          nx,
          xx,
          gt,
          lt,
          fieldsIndex,
          error: errorReply('ERR', `Unsupported option ${candidate}`),
        };
    }
    fieldsIndex = flagStartIndex + 1;
  }

  if (nx && (xx || gt || lt)) {
    return {
      nx,
      xx,
      gt,
      lt,
      fieldsIndex,
      error: errorReply(
        'ERR',
        'NX and XX, GT or LT options at the same time are not compatible'
      ),
    };
  }

  return { nx, xx, gt, lt, fieldsIndex, error: null };
}

/**
 * Check if a new field expiry should be applied given flags.
 * Returns true if the expiry should be set.
 */
function shouldSetFieldExpiry(
  db: Database,
  key: string,
  field: string,
  newExpiryMs: number,
  flags: { nx: boolean; xx: boolean; gt: boolean; lt: boolean }
): boolean {
  const currentExpiry = db.getFieldExpiry(key, field);
  const hasExpiry = currentExpiry !== undefined;

  if (flags.nx && hasExpiry) return false;
  if (flags.xx && !hasExpiry) return false;
  if (flags.gt) {
    if (!hasExpiry) return false; // no TTL = infinite, nothing is greater
    return newExpiryMs > currentExpiry;
  }
  if (flags.lt) {
    if (!hasExpiry) return true; // no TTL = infinite, any finite is less
    return newExpiryMs < currentExpiry;
  }
  return true;
}

/**
 * Get existing hash for read operations.
 * Returns the hash map if key exists and is a hash, null if key doesn't exist,
 * or an error Reply if the key is the wrong type.
 */
function getHash(
  db: Database,
  key: string
): { hash: Map<string, string> | null; error: Reply | null } {
  const entry = db.get(key);
  if (!entry) return { hash: null, error: null };
  if (entry.type !== 'hash') return { hash: null, error: WRONGTYPE_ERR };
  return { hash: entry.value as Map<string, string>, error: null };
}

/**
 * Core implementation for HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT.
 * computeExpiry converts the raw time value to an absolute ms timestamp.
 */
function hexpireGeneric(
  db: Database,
  clock: () => number,
  args: string[],
  computeExpiry: (timeValue: number, now: number) => number
): Reply {
  const key = args[0] ?? '';
  const timeStr = args[1] ?? '';
  const timeValue = parseInt(timeStr, 10);
  if (isNaN(timeValue)) return NOT_INTEGER_ERR;
  if (timeValue < 0) {
    return errorReply('ERR', 'invalid expire time, must be >= 0');
  }

  const flagsParsed = parseExpireFlags(args, 2);
  if (flagsParsed.error) return flagsParsed.error;

  const fieldsParsed = parseFields(args, flagsParsed.fieldsIndex);
  if (fieldsParsed.error) return fieldsParsed.error;

  const { hash, error } = getHash(db, key);
  if (error) return error;

  const now = clock();
  const expiryMs = computeExpiry(timeValue, now);
  const results: Reply[] = [];

  for (const field of fieldsParsed.fields) {
    // Key doesn't exist or field doesn't exist
    if (!hash || !hash.has(field)) {
      results.push(integerReply(-2));
      continue;
    }

    // Expiry is in the past or at current time — delete field immediately
    if (expiryMs <= now) {
      hash.delete(field);
      db.removeFieldExpiry(key, field);
      // Delete key if hash is now empty
      if (hash.size === 0) {
        db.delete(key);
      }
      results.push(integerReply(2));
      continue;
    }

    // Check flags (NX/XX/GT/LT)
    if (!shouldSetFieldExpiry(db, key, field, expiryMs, flagsParsed)) {
      results.push(integerReply(0));
      continue;
    }

    db.setFieldExpiry(key, field, expiryMs);
    results.push(integerReply(1));
  }

  return arrayReply(results);
}

// --- HEXPIRE ---

export function hexpire(
  db: Database,
  clock: () => number,
  args: string[]
): Reply {
  return hexpireGeneric(
    db,
    clock,
    args,
    (seconds, now) => now + seconds * 1000
  );
}

// --- HPEXPIRE ---

export function hpexpire(
  db: Database,
  clock: () => number,
  args: string[]
): Reply {
  return hexpireGeneric(db, clock, args, (ms, now) => now + ms);
}

// --- HEXPIREAT ---

export function hexpireat(
  db: Database,
  clock: () => number,
  args: string[]
): Reply {
  return hexpireGeneric(db, clock, args, (timestamp) => timestamp * 1000);
}

// --- HPEXPIREAT ---

export function hpexpireat(
  db: Database,
  clock: () => number,
  args: string[]
): Reply {
  return hexpireGeneric(db, clock, args, (timestampMs) => timestampMs);
}

/**
 * Core implementation for HTTL/HPTTL/HEXPIRETIME/HPEXPIRETIME/HPERSIST.
 * These commands only need key + FIELDS numfields field...
 * resultFn computes the per-field result given the field expiry and current time.
 */
function hfieldReadGeneric(
  db: Database,
  clock: () => number,
  args: string[],
  resultFn: (
    hash: Map<string, string>,
    field: string,
    expiryMs: number | undefined,
    now: number
  ) => number
): Reply {
  const key = args[0] ?? '';

  const fieldsParsed = parseFields(args, 1);
  if (fieldsParsed.error) return fieldsParsed.error;

  const { hash, error } = getHash(db, key);
  if (error) return error;

  const now = clock();
  const results: Reply[] = [];

  for (const field of fieldsParsed.fields) {
    if (!hash || !hash.has(field)) {
      results.push(integerReply(-2));
      continue;
    }

    const expiryMs = db.getFieldExpiry(key, field);
    results.push(integerReply(resultFn(hash, field, expiryMs, now)));
  }

  return arrayReply(results);
}

// --- HTTL ---

export function httl(db: Database, clock: () => number, args: string[]): Reply {
  return hfieldReadGeneric(db, clock, args, (_hash, _field, expiryMs, now) => {
    if (expiryMs === undefined) return -1;
    const remaining = Math.max(0, expiryMs - now);
    return Math.ceil(remaining / 1000);
  });
}

// --- HPTTL ---

export function hpttl(
  db: Database,
  clock: () => number,
  args: string[]
): Reply {
  return hfieldReadGeneric(db, clock, args, (_hash, _field, expiryMs, now) => {
    if (expiryMs === undefined) return -1;
    return Math.max(0, expiryMs - now);
  });
}

// --- HPERSIST ---

export function hpersist(
  db: Database,
  clock: () => number,
  args: string[]
): Reply {
  const key = args[0] ?? '';

  const fieldsParsed = parseFields(args, 1);
  if (fieldsParsed.error) return fieldsParsed.error;

  const { hash, error } = getHash(db, key);
  if (error) return error;

  const results: Reply[] = [];

  for (const field of fieldsParsed.fields) {
    if (!hash || !hash.has(field)) {
      results.push(integerReply(-2));
      continue;
    }

    const expiryMs = db.getFieldExpiry(key, field);
    if (expiryMs === undefined) {
      results.push(integerReply(-1));
    } else {
      db.removeFieldExpiry(key, field);
      results.push(integerReply(1));
    }
  }

  return arrayReply(results);
}

// --- HEXPIRETIME ---

export function hexpiretime(
  db: Database,
  clock: () => number,
  args: string[]
): Reply {
  return hfieldReadGeneric(db, clock, args, (_hash, _field, expiryMs) => {
    if (expiryMs === undefined) return -1;
    return Math.ceil(expiryMs / 1000);
  });
}

// --- HPEXPIRETIME ---

export function hpexpiretime(
  db: Database,
  clock: () => number,
  args: string[]
): Reply {
  return hfieldReadGeneric(db, clock, args, (_hash, _field, expiryMs) => {
    if (expiryMs === undefined) return -1;
    return expiryMs;
  });
}

export const specs: CommandSpec[] = [
  {
    name: 'hexpire',
    handler: (ctx, args) => hexpire(ctx.db, ctx.engine.clock, args),
    arity: -6,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@hash', '@fast'],
  },
  {
    name: 'hpexpire',
    handler: (ctx, args) => hpexpire(ctx.db, ctx.engine.clock, args),
    arity: -6,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@hash', '@fast'],
  },
  {
    name: 'hexpireat',
    handler: (ctx, args) => hexpireat(ctx.db, ctx.engine.clock, args),
    arity: -6,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@hash', '@fast'],
  },
  {
    name: 'hpexpireat',
    handler: (ctx, args) => hpexpireat(ctx.db, ctx.engine.clock, args),
    arity: -6,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@hash', '@fast'],
  },
  {
    name: 'httl',
    handler: (ctx, args) => httl(ctx.db, ctx.engine.clock, args),
    arity: -5,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@hash', '@fast'],
  },
  {
    name: 'hpttl',
    handler: (ctx, args) => hpttl(ctx.db, ctx.engine.clock, args),
    arity: -5,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@hash', '@fast'],
  },
  {
    name: 'hpersist',
    handler: (ctx, args) => hpersist(ctx.db, ctx.engine.clock, args),
    arity: -5,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@hash', '@fast'],
  },
  {
    name: 'hexpiretime',
    handler: (ctx, args) => hexpiretime(ctx.db, ctx.engine.clock, args),
    arity: -5,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@hash', '@fast'],
  },
  {
    name: 'hpexpiretime',
    handler: (ctx, args) => hpexpiretime(ctx.db, ctx.engine.clock, args),
    arity: -5,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@hash', '@fast'],
  },
];
