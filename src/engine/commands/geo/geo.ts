import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import {
  bulkReply,
  integerReply,
  arrayReply,
  errorReply,
  NIL,
} from '../../types.ts';
import { formatFloat } from '../incr.ts';
import type { CommandSpec } from '../../command-table.ts';
import {
  type SortedSetData,
  updateEncoding,
  getOrCreateZset,
  getExistingZset,
} from '../sorted-set/index.ts';
import { notify, EVENT_FLAGS } from '../../notify.ts';
import {
  UNIT_ERR,
  geohashEncode,
  geohashToString,
  haversineDistance,
  parseUnit,
  formatDist,
  validateCoords,
} from './codec.ts';
import { getMemberPos } from './search.ts';
import {
  geosearch,
  geosearchstore,
  georadius,
  georadiusbymember,
  georadius_ro,
  georadiusbymember_ro,
} from './search.ts';
import { parseFloat64 } from '../incr.ts';

// --- Helpers ---

function removeIfEmpty(db: Database, key: string, zset: SortedSetData): void {
  if (zset.dict.size === 0) {
    db.delete(key);
  }
}

// --- GEOADD ---

export function geoadd(db: Database, args: string[], rng: () => number): Reply {
  if (args.length < 4) {
    return errorReply('ERR', "wrong number of arguments for 'geoadd' command");
  }

  const key = args[0] as string;
  let i = 1;

  // Parse flags
  let nx = false;
  let xx = false;
  let ch = false;

  while (i < args.length) {
    const flag = (args[i] as string).toUpperCase();
    if (flag === 'NX') {
      nx = true;
      i++;
    } else if (flag === 'XX') {
      xx = true;
      i++;
    } else if (flag === 'CH') {
      ch = true;
      i++;
    } else {
      break;
    }
  }

  if (nx && xx) {
    return errorReply(
      'ERR',
      'XX and NX options at the same time are not compatible'
    );
  }

  // Remaining args must be lon lat member triples
  const remaining = args.length - i;
  if (remaining < 3 || remaining % 3 !== 0) {
    return errorReply('ERR', "wrong number of arguments for 'geoadd' command");
  }

  // Parse and validate all triples first
  const triples: { lon: number; lat: number; member: string; hash: number }[] =
    [];
  for (; i < args.length; i += 3) {
    const lonParsed = parseFloat64(args[i] as string);
    const latParsed = parseFloat64(args[i + 1] as string);
    if (!lonParsed || !latParsed) {
      return errorReply('ERR', 'value is not a valid float');
    }
    const lon = lonParsed.value;
    const lat = latParsed.value;
    const member = args[i + 2] as string;

    const err = validateCoords(lon, lat);
    if (err) return err;

    const hash = geohashEncode(lon, lat);
    triples.push({ lon, lat, member, hash });
  }

  const { zset, error } = getOrCreateZset(db, key, rng);
  if (error) return error;

  let added = 0;
  let updated = 0;

  for (const { member, hash } of triples) {
    const existing = zset.dict.get(member);

    if (existing !== undefined) {
      if (nx) continue;
      if (hash !== existing) {
        zset.sl.delete(existing, member);
        zset.sl.insert(hash, member);
        zset.dict.set(member, hash);
        updated++;
      }
    } else {
      if (xx) continue;
      zset.sl.insert(hash, member);
      zset.dict.set(member, hash);
      added++;
    }
  }

  removeIfEmpty(db, key, zset);
  updateEncoding(db, key);

  return integerReply(ch ? added + updated : added);
}

// --- GEOPOS ---

export function geopos(db: Database, args: string[]): Reply {
  if (args.length < 1) {
    return errorReply('ERR', "wrong number of arguments for 'geopos' command");
  }

  const key = args[0] as string;
  const { zset, error } = getExistingZset(db, key);
  if (error) return error;

  const results: Reply[] = [];
  for (let i = 1; i < args.length; i++) {
    const member = args[i] as string;
    if (!zset) {
      results.push(NIL);
      continue;
    }
    const pos = getMemberPos(zset, member);
    if (!pos) {
      results.push(NIL);
    } else {
      results.push(
        arrayReply([
          bulkReply(formatFloat(pos[0])),
          bulkReply(formatFloat(pos[1])),
        ])
      );
    }
  }

  return arrayReply(results);
}

// --- GEODIST ---

export function geodist(db: Database, args: string[]): Reply {
  if (args.length < 3 || args.length > 4) {
    return errorReply('ERR', "wrong number of arguments for 'geodist' command");
  }

  const key = args[0] as string;
  const member1 = args[1] as string;
  const member2 = args[2] as string;
  const unitStr = args.length === 4 ? (args[3] as string) : 'm';

  const unitFactor = parseUnit(unitStr);
  if (unitFactor === null) return UNIT_ERR;

  const { zset, error } = getExistingZset(db, key);
  if (error) return error;
  if (!zset) return NIL;

  const pos1 = getMemberPos(zset, member1);
  const pos2 = getMemberPos(zset, member2);
  if (!pos1 || !pos2) return NIL;

  const dist = haversineDistance(pos1[0], pos1[1], pos2[0], pos2[1]);
  return bulkReply(formatDist(dist, unitFactor));
}

// --- GEOHASH ---

export function geohash(db: Database, args: string[]): Reply {
  if (args.length < 1) {
    return errorReply('ERR', "wrong number of arguments for 'geohash' command");
  }

  const key = args[0] as string;
  const { zset, error } = getExistingZset(db, key);
  if (error) return error;

  const results: Reply[] = [];
  for (let i = 1; i < args.length; i++) {
    const member = args[i] as string;
    if (!zset) {
      results.push(NIL);
      continue;
    }
    const score = zset.dict.get(member);
    if (score === undefined) {
      results.push(NIL);
    } else {
      results.push(bulkReply(geohashToString(score)));
    }
  }

  return arrayReply(results);
}

// --- Command specs ---

export const specs: CommandSpec[] = [
  {
    name: 'geoadd',
    handler: (ctx, args) => {
      const reply = geoadd(ctx.db, args, ctx.engine.rng);
      if (reply.kind === 'integer' && (reply.value as number) >= 0) {
        notify(ctx, EVENT_FLAGS.SORTEDSET, 'zadd', args[0] ?? '');
      }
      return reply;
    },
    arity: -5,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@geo'],
  },
  {
    name: 'geopos',
    handler: (ctx, args) => geopos(ctx.db, args),
    arity: -2,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@geo'],
  },
  {
    name: 'geodist',
    handler: (ctx, args) => geodist(ctx.db, args),
    arity: -4,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@geo'],
  },
  {
    name: 'geohash',
    handler: (ctx, args) => geohash(ctx.db, args),
    arity: -2,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@geo'],
  },
  {
    name: 'geosearch',
    handler: (ctx, args) => geosearch(ctx.db, args),
    arity: -7,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@geo'],
  },
  {
    name: 'geosearchstore',
    handler: (ctx, args) => {
      const reply = geosearchstore(ctx.db, args, ctx.engine.rng);
      if (reply.kind === 'integer') {
        notify(ctx, EVENT_FLAGS.SORTEDSET, 'geosearchstore', args[0] ?? '');
      }
      return reply;
    },
    arity: -8,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 2,
    keyStep: 1,
    categories: ['@write', '@geo'],
  },
  {
    name: 'georadius',
    handler: (ctx, args) => georadius(ctx.db, args, ctx.engine.rng),
    arity: -6,
    flags: ['write', 'movablekeys'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@geo'],
  },
  {
    name: 'georadiusbymember',
    handler: (ctx, args) => georadiusbymember(ctx.db, args, ctx.engine.rng),
    arity: -5,
    flags: ['write', 'movablekeys'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@geo'],
  },
  {
    name: 'georadius_ro',
    handler: (ctx, args) => georadius_ro(ctx.db, args),
    arity: -6,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@geo'],
  },
  {
    name: 'georadiusbymember_ro',
    handler: (ctx, args) => georadiusbymember_ro(ctx.db, args),
    arity: -5,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@geo'],
  },
];
