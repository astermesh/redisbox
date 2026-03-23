/**
 * MEMORY command implementation.
 *
 * Subcommands: USAGE, DOCTOR, HELP, MALLOC-STATS, PURGE, STATS
 */

import type { Database } from '../database.ts';
import type { RedisEngine } from '../engine.ts';
import type { Reply } from '../types.ts';
import {
  arrayReply,
  bulkReply,
  integerReply,
  statusReply,
  errorReply,
  wrongArityError,
  unknownSubcommandError,
  NOT_INTEGER_ERR,
} from '../types.ts';
import { estimateKeyMemoryWithSamples } from '../memory/memory.ts';
import type { CommandSpec } from '../command-table.ts';

const DEFAULT_SAMPLES = 5;

// ---------------------------------------------------------------------------
// Subcommand handlers
// ---------------------------------------------------------------------------

export function memoryUsage(db: Database, args: string[]): Reply {
  if (args.length < 1 || args.length > 3) {
    return wrongArityError('memory|usage');
  }

  const key = args[0] ?? '';
  let samples = DEFAULT_SAMPLES;

  if (args.length === 3) {
    const opt = (args[1] ?? '').toUpperCase();
    if (opt !== 'SAMPLES') {
      return errorReply('ERR', 'syntax error');
    }
    const n = parseInt(args[2] ?? '', 10);
    if (isNaN(n) || args[2] !== String(n)) {
      return NOT_INTEGER_ERR;
    }
    if (n < 0) {
      return NOT_INTEGER_ERR;
    }
    samples = n;
  } else if (args.length === 2) {
    return errorReply('ERR', 'syntax error');
  }

  const entry = db.get(key);
  if (!entry) return bulkReply(null);

  const hasExpiry = db.getExpiry(key) !== undefined;
  const bytes = estimateKeyMemoryWithSamples(key, entry, hasExpiry, samples);
  return integerReply(bytes);
}

export function memoryDoctor(): Reply {
  return bulkReply(
    "Hi Sam, I can't find any memory issue in your instance. " +
      'I can only account for what occurs on this base.\n'
  );
}

export function memoryMallocStats(): Reply {
  return bulkReply('Memory allocator stats not available in this engine');
}

export function memoryPurge(): Reply {
  return statusReply('OK');
}

export function memoryStats(engine: RedisEngine): Reply {
  const used = engine.usedMemory();

  // Process memory (Node.js supplement)
  let rss = 0;
  let heapUsed = 0;
  let heapTotal = 0;
  if (typeof process !== 'undefined' && process.memoryUsage) {
    const mem = process.memoryUsage();
    rss = mem.rss;
    heapUsed = mem.heapUsed;
    heapTotal = mem.heapTotal;
  }

  const keyCount = countAllKeys(engine);

  const stats: Reply[] = [
    bulkReply('peak.allocated'),
    integerReply(used),
    bulkReply('total.allocated'),
    integerReply(used),
    bulkReply('startup.allocated'),
    integerReply(0),
    bulkReply('replication.backlog'),
    integerReply(0),
    bulkReply('clients.slaves'),
    integerReply(0),
    bulkReply('clients.normal'),
    integerReply(0),
    bulkReply('cluster.links'),
    integerReply(0),
    bulkReply('aof.buffer'),
    integerReply(0),
    bulkReply('lua.caches'),
    integerReply(0),
    bulkReply('functions.caches'),
    integerReply(0),
  ];

  // Per-database entries — only for non-empty databases (matches real Redis)
  for (let i = 0; i < engine.databases.length; i++) {
    const db = engine.databases[i];
    if (db && db.size > 0) {
      stats.push(
        bulkReply(`db.${i}`),
        arrayReply([
          bulkReply('overhead.hashtable.main'),
          integerReply(0),
          bulkReply('overhead.hashtable.expires'),
          integerReply(0),
          bulkReply('overhead.hashtable.slot-to-key'),
          integerReply(0),
        ])
      );
    }
  }

  stats.push(
    bulkReply('overhead.total'),
    integerReply(0),
    bulkReply('keys.count'),
    integerReply(keyCount),
    bulkReply('keys.bytes-per-key'),
    integerReply(avgBytesPerKey(engine, used)),
    bulkReply('dataset.bytes'),
    integerReply(used),
    bulkReply('dataset.percentage'),
    bulkReply(used > 0 ? '100.00%' : '0.00%'),
    bulkReply('peak.percentage'),
    bulkReply(used > 0 ? '100.00%' : '0.00%'),
    bulkReply('allocator.allocated'),
    integerReply(heapUsed),
    bulkReply('allocator.active'),
    integerReply(heapTotal),
    bulkReply('allocator.resident'),
    integerReply(rss),
    bulkReply('allocator-fragmentation.ratio'),
    bulkReply(heapUsed > 0 ? (heapTotal / heapUsed).toFixed(2) : '0.00'),
    bulkReply('allocator-fragmentation.bytes'),
    integerReply(Math.max(0, heapTotal - heapUsed)),
    bulkReply('allocator-rss.ratio'),
    bulkReply(heapTotal > 0 ? (rss / heapTotal).toFixed(2) : '0.00'),
    bulkReply('allocator-rss.bytes'),
    integerReply(Math.max(0, rss - heapTotal)),
    bulkReply('rss-overhead.ratio'),
    bulkReply('1.00'),
    bulkReply('rss-overhead.bytes'),
    integerReply(0),
    bulkReply('fragmentation'),
    bulkReply(used > 0 ? (rss / used).toFixed(2) : '0.00'),
    bulkReply('fragmentation.bytes'),
    integerReply(Math.max(0, rss - used))
  );

  return arrayReply(stats);
}

export function memoryHelp(): Reply {
  return arrayReply([
    bulkReply('DOCTOR'),
    bulkReply('    Return memory problems reports.'),
    bulkReply('MALLOC-STATS'),
    bulkReply(
      '    Return internal statistics report from the memory allocator.'
    ),
    bulkReply('PURGE'),
    bulkReply(
      '    Attempt to purge dirty pages for reclamation by the allocator.'
    ),
    bulkReply('STATS'),
    bulkReply('    Return information about the memory usage of the server.'),
    bulkReply('USAGE <key> [SAMPLES <count>]'),
    bulkReply(
      '    Return memory in bytes used by <key> and its value. Nested values are'
    ),
    bulkReply(
      '    sampled up to <count> times (default: 5, 0 means sample all).'
    ),
  ]);
}

// ---------------------------------------------------------------------------
// Main dispatcher
// ---------------------------------------------------------------------------

export function memory(
  db: Database,
  engine: RedisEngine,
  args: string[]
): Reply {
  if (args.length === 0) {
    return wrongArityError('memory');
  }

  const subcommand = (args[0] ?? '').toUpperCase();
  const subArgs = args.slice(1);

  switch (subcommand) {
    case 'USAGE':
      return memoryUsage(db, subArgs);
    case 'DOCTOR':
      if (subArgs.length !== 0) return wrongArityError('memory|doctor');
      return memoryDoctor();
    case 'MALLOC-STATS':
      if (subArgs.length !== 0) return wrongArityError('memory|malloc-stats');
      return memoryMallocStats();
    case 'PURGE':
      if (subArgs.length !== 0) return wrongArityError('memory|purge');
      return memoryPurge();
    case 'STATS':
      if (subArgs.length !== 0) return wrongArityError('memory|stats');
      return memoryStats(engine);
    case 'HELP':
      return memoryHelp();
    default:
      return unknownSubcommandError('memory', (args[0] ?? '').toLowerCase());
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function countAllKeys(engine: RedisEngine): number {
  let total = 0;
  for (const db of engine.databases) {
    total += db.size;
  }
  return total;
}

function avgBytesPerKey(engine: RedisEngine, totalUsed: number): number {
  const count = countAllKeys(engine);
  return count === 0 ? 0 : Math.round(totalUsed / count);
}

export const specs: CommandSpec[] = [
  {
    name: 'memory',
    handler: (ctx, args) => memory(ctx.db, ctx.engine, args),
    arity: -2,
    flags: ['readonly'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@slow'],
    subcommands: [
      {
        name: 'usage',
        handler: (ctx, args) => memoryUsage(ctx.db, args),
        arity: -3,
        flags: ['readonly'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@read', '@slow'],
      },
      {
        name: 'doctor',
        handler: () => memoryDoctor(),
        arity: 2,
        flags: ['readonly'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow'],
      },
      {
        name: 'malloc-stats',
        handler: () => memoryMallocStats(),
        arity: 2,
        flags: ['readonly'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow'],
      },
      {
        name: 'purge',
        handler: () => memoryPurge(),
        arity: 2,
        flags: [],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow'],
      },
      {
        name: 'stats',
        handler: (ctx) => memoryStats(ctx.engine),
        arity: 2,
        flags: ['readonly'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow'],
      },
      {
        name: 'help',
        handler: () => memoryHelp(),
        arity: 2,
        flags: ['loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow'],
      },
    ],
  },
];
