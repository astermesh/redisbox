import type { Database } from '../database.ts';
import type { RedisEncoding, Reply } from '../types.ts';
import {
  bulkReply,
  integerReply,
  arrayReply,
  errorReply,
  OK,
  NIL,
  ZERO,
  ONE,
  WRONGTYPE_ERR,
  SYNTAX_ERR,
  NOT_INTEGER_ERR,
  STRING_EXCEEDS_512MB_ERR,
  OFFSET_OUT_OF_RANGE_ERR,
  wrongArityError,
  invalidExpireTimeError,
} from '../types.ts';
import type { CommandSpec } from '../command-table.ts';
import { notify, EVENT_FLAGS } from '../notify.ts';

import { INT64_MAX, INT64_MIN, strByteLength } from '../utils.ts';

const INT_PATTERN = /^-?[1-9]\d*$|^0$/;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function strToBytes(s: string): Uint8Array {
  return textEncoder.encode(s);
}

function bytesToStr(b: Uint8Array): string {
  return textDecoder.decode(b);
}

function parseIntArg(s: string): { value: number; error: Reply | null } {
  const val = parseInt(s, 10);
  if (isNaN(val) || String(val) !== s) {
    return { value: 0, error: NOT_INTEGER_ERR };
  }
  return { value: val, error: null };
}

export function determineStringEncoding(value: string): RedisEncoding {
  if (INT_PATTERN.test(value)) {
    try {
      const n = BigInt(value);
      if (n >= INT64_MIN && n <= INT64_MAX) {
        return 'int';
      }
    } catch {
      // not a valid bigint — fall through
    }
  }

  return strByteLength(value) <= 44 ? 'embstr' : 'raw';
}

export function get(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const entry = db.get(key);
  if (!entry) return NIL;
  if (entry.type !== 'string') return WRONGTYPE_ERR;
  return bulkReply(entry.value as string);
}

interface SetFlags {
  ex: number | null;
  px: number | null;
  exat: number | null;
  pxat: number | null;
  nx: boolean;
  xx: boolean;
  keepttl: boolean;
  getOld: boolean;
}

function hasTtlFlag(flags: SetFlags): boolean {
  return (
    flags.ex !== null ||
    flags.px !== null ||
    flags.exat !== null ||
    flags.pxat !== null
  );
}

function parseTtlValue(
  args: string[],
  i: number
): { val: number; error: Reply | null } {
  if (i >= args.length) {
    return { val: 0, error: SYNTAX_ERR };
  }
  const val = parseInt(args[i] ?? '', 10);
  if (isNaN(val) || String(val) !== args[i]) {
    return { val: 0, error: NOT_INTEGER_ERR };
  }
  if (val <= 0) {
    return {
      val: 0,
      error: invalidExpireTimeError('set'),
    };
  }
  return { val, error: null };
}

function parseSetFlags(args: string[]): {
  flags: SetFlags;
  error: Reply | null;
} {
  const flags: SetFlags = {
    ex: null,
    px: null,
    exat: null,
    pxat: null,
    nx: false,
    xx: false,
    keepttl: false,
    getOld: false,
  };

  let i = 0;
  while (i < args.length) {
    const flag = (args[i] ?? '').toUpperCase();
    switch (flag) {
      case 'EX': {
        if (hasTtlFlag(flags) || flags.keepttl)
          return { flags, error: SYNTAX_ERR };
        i++;
        const { val, error } = parseTtlValue(args, i);
        if (error) return { flags, error };
        flags.ex = val;
        break;
      }
      case 'PX': {
        if (hasTtlFlag(flags) || flags.keepttl)
          return { flags, error: SYNTAX_ERR };
        i++;
        const { val, error } = parseTtlValue(args, i);
        if (error) return { flags, error };
        flags.px = val;
        break;
      }
      case 'EXAT': {
        if (hasTtlFlag(flags) || flags.keepttl)
          return { flags, error: SYNTAX_ERR };
        i++;
        const { val, error } = parseTtlValue(args, i);
        if (error) return { flags, error };
        flags.exat = val;
        break;
      }
      case 'PXAT': {
        if (hasTtlFlag(flags) || flags.keepttl)
          return { flags, error: SYNTAX_ERR };
        i++;
        const { val, error } = parseTtlValue(args, i);
        if (error) return { flags, error };
        flags.pxat = val;
        break;
      }
      case 'NX':
        if (flags.nx || flags.xx) return { flags, error: SYNTAX_ERR };
        flags.nx = true;
        break;
      case 'XX':
        if (flags.xx || flags.nx) return { flags, error: SYNTAX_ERR };
        flags.xx = true;
        break;
      case 'KEEPTTL':
        if (flags.keepttl || hasTtlFlag(flags))
          return { flags, error: SYNTAX_ERR };
        flags.keepttl = true;
        break;
      case 'GET':
        flags.getOld = true;
        break;
      default:
        return { flags, error: SYNTAX_ERR };
    }
    i++;
  }

  if (flags.getOld && flags.nx) {
    return {
      flags,
      error: errorReply(
        'ERR',
        'NX and GET options at the same time are not compatible'
      ),
    };
  }

  return { flags, error: null };
}

export function set(db: Database, clock: () => number, args: string[]): Reply {
  const key = args[0] ?? '';
  const value = args[1] ?? '';
  const flagArgs = args.slice(2);

  const { flags, error } = parseSetFlags(flagArgs);
  if (error) return error;

  // handle GET flag — must check type before anything else
  let oldValue: string | null = null;
  if (flags.getOld) {
    const existing = db.get(key);
    if (existing) {
      if (existing.type !== 'string') return WRONGTYPE_ERR;
      oldValue = existing.value as string;
    }
  }

  // NX/XX checks
  if (flags.nx) {
    if (db.has(key)) {
      return NIL;
    }
  }
  if (flags.xx) {
    if (!db.has(key)) {
      return NIL;
    }
  }

  // preserve TTL before overwriting
  const existingExpiry = flags.keepttl ? db.getExpiry(key) : undefined;

  // determine encoding and set the value
  const encoding = determineStringEncoding(value);
  db.set(key, 'string', encoding, value);

  // handle TTL
  if (flags.ex !== null) {
    db.setExpiry(key, clock() + flags.ex * 1000);
  } else if (flags.px !== null) {
    db.setExpiry(key, clock() + flags.px);
  } else if (flags.exat !== null) {
    db.setExpiry(key, flags.exat * 1000);
  } else if (flags.pxat !== null) {
    db.setExpiry(key, flags.pxat);
  } else if (flags.keepttl && existingExpiry !== undefined) {
    db.setExpiry(key, existingExpiry);
  } else {
    // no TTL flags and not KEEPTTL → remove any existing expiry
    db.removeExpiry(key);
  }

  if (flags.getOld) {
    return bulkReply(oldValue);
  }
  return OK;
}

// --- MGET ---

export function mget(db: Database, args: string[]): Reply {
  const results: Reply[] = [];
  for (const key of args) {
    const entry = db.get(key);
    if (!entry || entry.type !== 'string') {
      results.push(NIL);
    } else {
      results.push(bulkReply(entry.value as string));
    }
  }
  return arrayReply(results);
}

// --- MSET ---

export function mset(db: Database, args: string[]): Reply {
  if (args.length % 2 !== 0) {
    return wrongArityError('mset');
  }
  for (let i = 0; i < args.length; i += 2) {
    const key = args[i] ?? '';
    const value = args[i + 1] ?? '';
    const encoding = determineStringEncoding(value);
    db.set(key, 'string', encoding, value);
    db.removeExpiry(key);
  }
  return OK;
}

// --- MSETNX ---

export function msetnx(db: Database, args: string[]): Reply {
  if (args.length % 2 !== 0) {
    return wrongArityError('msetnx');
  }
  // Check if any key exists
  for (let i = 0; i < args.length; i += 2) {
    if (db.has(args[i] ?? '')) return ZERO;
  }
  // Set all keys
  for (let i = 0; i < args.length; i += 2) {
    const key = args[i] ?? '';
    const value = args[i + 1] ?? '';
    const encoding = determineStringEncoding(value);
    db.set(key, 'string', encoding, value);
  }
  return ONE;
}

// --- APPEND ---

export function append(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const appendValue = args[1] ?? '';

  const entry = db.get(key);
  if (entry && entry.type !== 'string') return WRONGTYPE_ERR;

  if (!entry) {
    // Key doesn't exist: create with determined encoding
    const encoding = determineStringEncoding(appendValue);
    db.set(key, 'string', encoding, appendValue);
    return integerReply(strByteLength(appendValue));
  }

  // Key exists: append and always use raw encoding
  const existingValue = entry.value as string;
  const newValue = existingValue + appendValue;
  db.set(key, 'string', 'raw', newValue);

  return integerReply(strByteLength(newValue));
}

// --- STRLEN ---

export function strlen(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const entry = db.get(key);
  if (!entry) return ZERO;
  if (entry.type !== 'string') return WRONGTYPE_ERR;
  return integerReply(strByteLength(entry.value as string));
}

// --- SETRANGE ---

export function setrange(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const { value: offset, error } = parseIntArg(args[1] ?? '');
  if (error) return error;
  if (offset < 0) return OFFSET_OUT_OF_RANGE_ERR;

  const newValueBytes = strToBytes(args[2] ?? '');

  // Empty value special case
  if (newValueBytes.length === 0) {
    const entry = db.get(key);
    if (!entry) return ZERO;
    if (entry.type !== 'string') return WRONGTYPE_ERR;
    return integerReply(strByteLength(entry.value as string));
  }

  const entry = db.get(key);
  if (entry && entry.type !== 'string') return WRONGTYPE_ERR;

  const existingBytes = entry
    ? strToBytes(entry.value as string)
    : new Uint8Array(0);
  const requiredLen = Math.max(
    existingBytes.length,
    offset + newValueBytes.length
  );

  if (requiredLen > 512 * 1024 * 1024) {
    return STRING_EXCEEDS_512MB_ERR;
  }

  const result = new Uint8Array(requiredLen);
  result.set(existingBytes);
  result.set(newValueBytes, offset);

  db.set(key, 'string', 'raw', bytesToStr(result));

  return integerReply(requiredLen);
}

// --- GETRANGE / SUBSTR ---

export function getrange(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const { value: start, error: startErr } = parseIntArg(args[1] ?? '');
  if (startErr) return startErr;
  const { value: end, error: endErr } = parseIntArg(args[2] ?? '');
  if (endErr) return endErr;

  const entry = db.get(key);
  if (!entry) return bulkReply('');
  if (entry.type !== 'string') return WRONGTYPE_ERR;

  const bytes = strToBytes(entry.value as string);
  const len = bytes.length;
  if (len === 0) return bulkReply('');

  const s = start < 0 ? Math.max(len + start, 0) : start;
  let e = end < 0 ? Math.max(len + end, 0) : end;
  if (e >= len) e = len - 1;
  if (s > e) return bulkReply('');

  return bulkReply(bytesToStr(bytes.slice(s, e + 1)));
}

// --- GETEX ---

export function getex(
  db: Database,
  clock: () => number,
  args: string[]
): Reply {
  const key = args[0] ?? '';

  // Parse options first (validate before executing)
  let mode: 'none' | 'ex' | 'px' | 'exat' | 'pxat' | 'persist' = 'none';
  let ttlValue = 0;

  let i = 1;
  while (i < args.length) {
    const opt = (args[i] ?? '').toUpperCase();
    switch (opt) {
      case 'EX':
      case 'PX':
      case 'EXAT':
      case 'PXAT': {
        if (mode !== 'none') return SYNTAX_ERR;
        mode = opt.toLowerCase() as 'ex' | 'px' | 'exat' | 'pxat';
        i++;
        if (i >= args.length) return SYNTAX_ERR;
        const { value: val, error: parseErr } = parseIntArg(args[i] ?? '');
        if (parseErr) return parseErr;
        if (val <= 0) return invalidExpireTimeError('getex');
        ttlValue = val;
        break;
      }
      case 'PERSIST':
        if (mode !== 'none') return SYNTAX_ERR;
        mode = 'persist';
        break;
      default:
        return SYNTAX_ERR;
    }
    i++;
  }

  // Get the value
  const entry = db.get(key);
  if (!entry) return NIL;
  if (entry.type !== 'string') return WRONGTYPE_ERR;

  // Apply TTL changes
  switch (mode) {
    case 'ex':
      db.setExpiry(key, clock() + ttlValue * 1000);
      break;
    case 'px':
      db.setExpiry(key, clock() + ttlValue);
      break;
    case 'exat':
      db.setExpiry(key, ttlValue * 1000);
      break;
    case 'pxat':
      db.setExpiry(key, ttlValue);
      break;
    case 'persist':
      db.removeExpiry(key);
      break;
  }

  return bulkReply(entry.value as string);
}

// --- GETDEL ---

export function getdel(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const entry = db.get(key);
  if (!entry) return NIL;
  if (entry.type !== 'string') return WRONGTYPE_ERR;
  const value = entry.value as string;
  db.delete(key);
  return bulkReply(value);
}

// --- GETSET ---

export function getset(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const newValue = args[1] ?? '';

  const entry = db.get(key);
  if (entry && entry.type !== 'string') return WRONGTYPE_ERR;

  const oldValue = entry ? (entry.value as string) : null;

  const encoding = determineStringEncoding(newValue);
  db.set(key, 'string', encoding, newValue);
  db.removeExpiry(key);

  return bulkReply(oldValue);
}

// --- SETNX ---

export function setnx(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const value = args[1] ?? '';

  if (db.has(key)) return ZERO;

  const encoding = determineStringEncoding(value);
  db.set(key, 'string', encoding, value);
  return ONE;
}

// --- SETEX ---

export function setex(
  db: Database,
  clock: () => number,
  args: string[]
): Reply {
  const key = args[0] ?? '';
  const { value: seconds, error } = parseIntArg(args[1] ?? '');
  if (error) return error;
  if (seconds <= 0) return invalidExpireTimeError('setex');
  const value = args[2] ?? '';

  const encoding = determineStringEncoding(value);
  db.set(key, 'string', encoding, value);
  db.setExpiry(key, clock() + seconds * 1000);

  return OK;
}

// --- PSETEX ---

export function psetex(
  db: Database,
  clock: () => number,
  args: string[]
): Reply {
  const key = args[0] ?? '';
  const { value: ms, error } = parseIntArg(args[1] ?? '');
  if (error) return error;
  if (ms <= 0) return invalidExpireTimeError('psetex');
  const value = args[2] ?? '';

  const encoding = determineStringEncoding(value);
  db.set(key, 'string', encoding, value);
  db.setExpiry(key, clock() + ms);

  return OK;
}

// --- LCS ---

export function lcs(db: Database, args: string[]): Reply {
  const key1 = args[0] ?? '';
  const key2 = args[1] ?? '';

  const entry1 = db.get(key1);
  const entry2 = db.get(key2);

  if (entry1 && entry1.type !== 'string') return WRONGTYPE_ERR;
  if (entry2 && entry2.type !== 'string') return WRONGTYPE_ERR;

  const s1 = entry1 ? (entry1.value as string) : '';
  const s2 = entry2 ? (entry2.value as string) : '';

  // Parse options
  let wantLen = false;
  let wantIdx = false;
  let minMatchLen = 0;
  let withMatchLen = false;

  let i = 2;
  while (i < args.length) {
    const opt = (args[i] ?? '').toUpperCase();
    switch (opt) {
      case 'LEN':
        wantLen = true;
        break;
      case 'IDX':
        wantIdx = true;
        break;
      case 'MINMATCHLEN': {
        i++;
        if (i >= args.length) return SYNTAX_ERR;
        const { value: val, error: parseErr } = parseIntArg(args[i] ?? '');
        if (parseErr) return parseErr;
        minMatchLen = Math.max(val, 0);
        break;
      }
      case 'WITHMATCHLEN':
        withMatchLen = true;
        break;
      default:
        return SYNTAX_ERR;
    }
    i++;
  }

  // Compute LCS using DP — use flat array for efficient access
  const m = s1.length;
  const n = s2.length;
  const w = n + 1;

  const dp = new Int32Array((m + 1) * w);
  const at = (r: number, c: number): number => dp[r * w + c] ?? 0;

  for (let r = 1; r <= m; r++) {
    for (let c = 1; c <= n; c++) {
      if (s1[r - 1] === s2[c - 1]) {
        dp[r * w + c] = at(r - 1, c - 1) + 1;
      } else {
        dp[r * w + c] = Math.max(at(r - 1, c), at(r, c - 1));
      }
    }
  }

  const lcsLen = at(m, n);

  // If only length requested (and not IDX)
  if (wantLen && !wantIdx) {
    return integerReply(lcsLen);
  }

  // Backtrack to find the LCS positions
  const positions: [number, number][] = [];
  {
    let r = m,
      c = n;
    while (r > 0 && c > 0) {
      if (s1[r - 1] === s2[c - 1]) {
        positions.push([r - 1, c - 1]);
        r--;
        c--;
      } else if (at(r - 1, c) >= at(r, c - 1)) {
        r--;
      } else {
        c--;
      }
    }
    positions.reverse();
  }

  if (!wantIdx) {
    // Build LCS string
    const lcsStr = positions.map(([a]) => s1[a]).join('');
    return bulkReply(lcsStr);
  }

  // Build IDX output — group consecutive matching positions into ranges
  interface Match {
    aStart: number;
    aEnd: number;
    bStart: number;
    bEnd: number;
  }

  const rawMatches: Match[] = [];
  for (const [a, b] of positions) {
    const last = rawMatches[rawMatches.length - 1];
    if (last && a === last.aEnd + 1 && b === last.bEnd + 1) {
      last.aEnd = a;
      last.bEnd = b;
    } else {
      rawMatches.push({ aStart: a, aEnd: a, bStart: b, bEnd: b });
    }
  }

  // Filter by MINMATCHLEN
  const filteredMatches = rawMatches.filter(
    (match) => match.aEnd - match.aStart + 1 >= minMatchLen
  );

  // Build array reply — matches are in reverse order in Redis
  const matchReplies: Reply[] = filteredMatches.reverse().map((match) => {
    const parts: Reply[] = [
      arrayReply([integerReply(match.aStart), integerReply(match.aEnd)]),
      arrayReply([integerReply(match.bStart), integerReply(match.bEnd)]),
    ];
    if (withMatchLen) {
      parts.push(integerReply(match.aEnd - match.aStart + 1));
    }
    return arrayReply(parts);
  });

  return arrayReply([
    bulkReply('matches'),
    arrayReply(matchReplies),
    bulkReply('len'),
    integerReply(lcsLen),
  ]);
}

export const specs: CommandSpec[] = [
  {
    name: 'get',
    handler: (ctx, args) => get(ctx.db, args),
    arity: 2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@string', '@fast'],
  },
  {
    name: 'set',
    handler: (ctx, args) => {
      const reply = set(ctx.db, ctx.engine.clock, args);
      if (reply === OK || (reply.kind === 'bulk' && reply !== NIL)) {
        notify(ctx, EVENT_FLAGS.STRING, 'set', args[0] ?? '');
      }
      return reply;
    },
    arity: -3,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@string'],
  },
  {
    name: 'mget',
    handler: (ctx, args) => mget(ctx.db, args),
    arity: -2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@read', '@string', '@fast'],
  },
  {
    name: 'mset',
    handler: (ctx, args) => {
      const reply = mset(ctx.db, args);
      if (reply === OK) {
        for (let i = 0; i < args.length; i += 2) {
          notify(ctx, EVENT_FLAGS.STRING, 'set', args[i] ?? '');
        }
      }
      return reply;
    },
    arity: -3,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 2,
    categories: ['@write', '@string'],
  },
  {
    name: 'msetnx',
    handler: (ctx, args) => {
      const reply = msetnx(ctx.db, args);
      if (reply === ONE) {
        for (let i = 0; i < args.length; i += 2) {
          notify(ctx, EVENT_FLAGS.STRING, 'set', args[i] ?? '');
        }
      }
      return reply;
    },
    arity: -3,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 2,
    categories: ['@write', '@string'],
  },
  {
    name: 'append',
    handler: (ctx, args) => {
      const reply = append(ctx.db, args);
      if (reply.kind === 'integer') {
        notify(ctx, EVENT_FLAGS.STRING, 'append', args[0] ?? '');
      }
      return reply;
    },
    arity: 3,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@string', '@fast'],
  },
  {
    name: 'strlen',
    handler: (ctx, args) => strlen(ctx.db, args),
    arity: 2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@string', '@fast'],
  },
  {
    name: 'setrange',
    handler: (ctx, args) => {
      const reply = setrange(ctx.db, args);
      if (reply.kind === 'integer') {
        notify(ctx, EVENT_FLAGS.STRING, 'setrange', args[0] ?? '');
      }
      return reply;
    },
    arity: 4,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@string'],
  },
  {
    name: 'getrange',
    handler: (ctx, args) => getrange(ctx.db, args),
    arity: 4,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@string'],
  },
  {
    name: 'substr',
    handler: (ctx, args) => getrange(ctx.db, args),
    arity: 4,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@string'],
  },
  {
    name: 'getex',
    handler: (ctx, args) => {
      const reply = getex(ctx.db, ctx.engine.clock, args);
      if (reply !== NIL && reply.kind === 'bulk') {
        // Determine event name based on options
        const opt = args.length > 1 ? (args[1] ?? '').toUpperCase() : '';
        if (opt === 'EX' || opt === 'EXAT') {
          notify(ctx, EVENT_FLAGS.STRING, 'expire', args[0] ?? '');
        } else if (opt === 'PX' || opt === 'PXAT') {
          notify(ctx, EVENT_FLAGS.STRING, 'pexpire', args[0] ?? '');
        } else if (opt === 'PERSIST') {
          notify(ctx, EVENT_FLAGS.STRING, 'persist', args[0] ?? '');
        } else {
          notify(ctx, EVENT_FLAGS.STRING, 'getex', args[0] ?? '');
        }
      }
      return reply;
    },
    arity: -2,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@string', '@fast'],
  },
  {
    name: 'getdel',
    handler: (ctx, args) => {
      const reply = getdel(ctx.db, args);
      if (reply !== NIL && reply.kind === 'bulk') {
        notify(ctx, EVENT_FLAGS.STRING, 'getdel', args[0] ?? '');
      }
      return reply;
    },
    arity: 2,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@string', '@fast'],
  },
  {
    name: 'getset',
    handler: (ctx, args) => {
      const reply = getset(ctx.db, args);
      if (reply.kind === 'bulk') {
        notify(ctx, EVENT_FLAGS.STRING, 'set', args[0] ?? '');
      }
      return reply;
    },
    arity: 3,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@string', '@fast'],
  },
  {
    name: 'setnx',
    handler: (ctx, args) => {
      const reply = setnx(ctx.db, args);
      if (reply === ONE) {
        notify(ctx, EVENT_FLAGS.STRING, 'set', args[0] ?? '');
      }
      return reply;
    },
    arity: 3,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@string', '@fast'],
  },
  {
    name: 'setex',
    handler: (ctx, args) => {
      const reply = setex(ctx.db, ctx.engine.clock, args);
      if (reply === OK) {
        notify(ctx, EVENT_FLAGS.STRING, 'set', args[0] ?? '');
      }
      return reply;
    },
    arity: 4,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@string'],
  },
  {
    name: 'psetex',
    handler: (ctx, args) => {
      const reply = psetex(ctx.db, ctx.engine.clock, args);
      if (reply === OK) {
        notify(ctx, EVENT_FLAGS.STRING, 'set', args[0] ?? '');
      }
      return reply;
    },
    arity: 4,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@string'],
  },
  {
    name: 'lcs',
    handler: (ctx, args) => lcs(ctx.db, args),
    arity: -3,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 2,
    keyStep: 1,
    categories: ['@read', '@string'],
  },
];
