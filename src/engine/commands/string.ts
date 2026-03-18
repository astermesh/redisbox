import type { Database } from '../database.ts';
import type { RedisEncoding, Reply } from '../types.ts';
import {
  bulkReply,
  errorReply,
  OK,
  NIL,
  wrongTypeError,
} from '../types.ts';

const INT64_MAX = BigInt('9223372036854775807');
const INT64_MIN = BigInt('-9223372036854775808');

const INT_PATTERN = /^-?[1-9]\d*$|^0$/;

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

  const byteLength = new TextEncoder().encode(value).length;
  return byteLength <= 44 ? 'embstr' : 'raw';
}

export function get(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const entry = db.get(key);
  if (!entry) return NIL;
  if (entry.type !== 'string') return wrongTypeError();
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

const SYNTAX_ERR = errorReply('ERR', 'syntax error');

function hasTtlFlag(flags: SetFlags): boolean {
  return flags.ex !== null || flags.px !== null || flags.exat !== null || flags.pxat !== null;
}

function parseTtlValue(args: string[], i: number): { val: number; error: Reply | null } {
  if (i >= args.length) {
    return { val: 0, error: SYNTAX_ERR };
  }
  const val = parseInt(args[i] ?? '', 10);
  if (isNaN(val) || String(val) !== args[i]) {
    return { val: 0, error: errorReply('ERR', 'value is not an integer or out of range') };
  }
  if (val <= 0) {
    return { val: 0, error: errorReply('ERR', 'invalid expire time in \'set\' command') };
  }
  return { val, error: null };
}

function parseSetFlags(args: string[]): { flags: SetFlags; error: Reply | null } {
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
        if (hasTtlFlag(flags) || flags.keepttl) return { flags, error: SYNTAX_ERR };
        i++;
        const { val, error } = parseTtlValue(args, i);
        if (error) return { flags, error };
        flags.ex = val;
        break;
      }
      case 'PX': {
        if (hasTtlFlag(flags) || flags.keepttl) return { flags, error: SYNTAX_ERR };
        i++;
        const { val, error } = parseTtlValue(args, i);
        if (error) return { flags, error };
        flags.px = val;
        break;
      }
      case 'EXAT': {
        if (hasTtlFlag(flags) || flags.keepttl) return { flags, error: SYNTAX_ERR };
        i++;
        const { val, error } = parseTtlValue(args, i);
        if (error) return { flags, error };
        flags.exat = val;
        break;
      }
      case 'PXAT': {
        if (hasTtlFlag(flags) || flags.keepttl) return { flags, error: SYNTAX_ERR };
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
        if (flags.keepttl || hasTtlFlag(flags)) return { flags, error: SYNTAX_ERR };
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
    return { flags, error: errorReply('ERR', 'NX and GET options at the same time are not compatible') };
  }

  return { flags, error: null };
}

export function set(
  db: Database,
  clock: () => number,
  args: string[]
): Reply {
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
      if (existing.type !== 'string') return wrongTypeError();
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
