import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import { integerReply, errorReply, ONE, ZERO } from '../types.ts';

const NO_TTL = integerReply(-1);
const NO_KEY = integerReply(-2);

function parseFlags(args: string[]): {
  nx: boolean;
  xx: boolean;
  gt: boolean;
  lt: boolean;
  error: Reply | null;
} {
  let nx = false;
  let xx = false;
  let gt = false;
  let lt = false;
  for (const arg of args) {
    switch (arg.toUpperCase()) {
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
          error: errorReply('ERR', `Unsupported option ${arg}`),
        };
    }
  }
  if (nx && (xx || gt || lt)) {
    return {
      nx,
      xx,
      gt,
      lt,
      error: errorReply(
        'ERR',
        'NX and XX, GT or LT options at the same time are not compatible'
      ),
    };
  }
  return { nx, xx, gt, lt, error: null };
}

function shouldSetExpiry(
  db: Database,
  key: string,
  newExpiryMs: number,
  flags: { nx: boolean; xx: boolean; gt: boolean; lt: boolean }
): boolean {
  const currentExpiry = db.getExpiry(key);
  const hasExpiry = currentExpiry !== undefined;

  if (flags.nx && hasExpiry) return false;
  if (flags.xx && !hasExpiry) return false;
  if (flags.gt) {
    if (!hasExpiry) return true;
    return newExpiryMs > currentExpiry;
  }
  if (flags.lt) {
    if (!hasExpiry) return true;
    return newExpiryMs < currentExpiry;
  }
  return true;
}

export function expire(
  db: Database,
  clock: () => number,
  args: string[]
): Reply {
  const key = args[0] ?? '';
  const seconds = parseInt(args[1] ?? '', 10);
  if (isNaN(seconds)) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }

  if (!db.has(key)) return ZERO;

  const flags = parseFlags(args.slice(2));
  if (flags.error) return flags.error;

  const expiryMs = clock() + seconds * 1000;
  if (!shouldSetExpiry(db, key, expiryMs, flags)) return ZERO;

  db.setExpiry(key, expiryMs);
  return ONE;
}

export function pexpire(
  db: Database,
  clock: () => number,
  args: string[]
): Reply {
  const key = args[0] ?? '';
  const ms = parseInt(args[1] ?? '', 10);
  if (isNaN(ms)) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }

  if (!db.has(key)) return ZERO;

  const flags = parseFlags(args.slice(2));
  if (flags.error) return flags.error;

  const expiryMs = clock() + ms;
  if (!shouldSetExpiry(db, key, expiryMs, flags)) return ZERO;

  db.setExpiry(key, expiryMs);
  return ONE;
}

export function expireat(
  db: Database,
  _clock: () => number,
  args: string[]
): Reply {
  const key = args[0] ?? '';
  const timestamp = parseInt(args[1] ?? '', 10);
  if (isNaN(timestamp)) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }

  if (!db.has(key)) return ZERO;

  const flags = parseFlags(args.slice(2));
  if (flags.error) return flags.error;

  const expiryMs = timestamp * 1000;
  if (!shouldSetExpiry(db, key, expiryMs, flags)) return ZERO;

  db.setExpiry(key, expiryMs);
  return ONE;
}

export function pexpireat(
  db: Database,
  _clock: () => number,
  args: string[]
): Reply {
  const key = args[0] ?? '';
  const timestampMs = parseInt(args[1] ?? '', 10);
  if (isNaN(timestampMs)) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }

  if (!db.has(key)) return ZERO;

  const flags = parseFlags(args.slice(2));
  if (flags.error) return flags.error;

  if (!shouldSetExpiry(db, key, timestampMs, flags)) return ZERO;

  db.setExpiry(key, timestampMs);
  return ONE;
}

export function ttl(db: Database, clock: () => number, args: string[]): Reply {
  const key = args[0] ?? '';
  if (!db.has(key)) return NO_KEY;
  const expiryMs = db.getExpiry(key);
  if (expiryMs === undefined) return NO_TTL;
  const ttlMs = Math.max(0, expiryMs - clock());
  return integerReply(Math.floor((ttlMs + 500) / 1000));
}

export function pttl(db: Database, clock: () => number, args: string[]): Reply {
  const key = args[0] ?? '';
  if (!db.has(key)) return NO_KEY;
  const expiryMs = db.getExpiry(key);
  if (expiryMs === undefined) return NO_TTL;
  return integerReply(expiryMs - clock());
}

export function expiretime(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  if (!db.has(key)) return NO_KEY;
  const expiryMs = db.getExpiry(key);
  if (expiryMs === undefined) return NO_TTL;
  return integerReply(Math.floor((expiryMs + 500) / 1000));
}

export function pexpiretime(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  if (!db.has(key)) return NO_KEY;
  const expiryMs = db.getExpiry(key);
  if (expiryMs === undefined) return NO_TTL;
  return integerReply(expiryMs);
}
