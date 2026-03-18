import { CommandTable } from './command-table.ts';
import type {
  CommandDefinition,
  CommandFlag,
  CommandHandler,
} from './command-table.ts';
import * as generic from './commands/generic.ts';
import * as ttl from './commands/ttl.ts';
import * as scan from './commands/scan.ts';
import * as sort from './commands/sort.ts';
import * as string from './commands/string.ts';
import * as incr from './commands/incr.ts';

interface CommandSpec {
  name: string;
  handler: CommandHandler;
  arity: number;
  flags: CommandFlag[];
  firstKey: number;
  lastKey: number;
  keyStep: number;
  categories: string[];
  subcommands?: CommandSpec[];
}

function toDefinition(spec: CommandSpec): CommandDefinition {
  const def: CommandDefinition = {
    name: spec.name,
    handler: spec.handler,
    arity: spec.arity,
    flags: new Set(spec.flags),
    firstKey: spec.firstKey,
    lastKey: spec.lastKey,
    keyStep: spec.keyStep,
    categories: new Set(spec.categories),
  };
  if (spec.subcommands) {
    def.subcommands = new Map();
    for (const sub of spec.subcommands) {
      def.subcommands.set(sub.name.toLowerCase(), toDefinition(sub));
    }
  }
  return def;
}

/**
 * All currently implemented commands.
 * Arity follows Redis convention: includes command name.
 * Positive = exact, negative = minimum.
 */
const commandSpecs: CommandSpec[] = [
  // --- Generic (keyspace) commands ---
  {
    name: 'del',
    handler: (ctx, args) => generic.del(ctx.db, args),
    arity: -2,
    flags: ['write'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@keyspace', '@write'],
  },
  {
    name: 'unlink',
    handler: (ctx, args) => generic.unlink(ctx.db, args),
    arity: -2,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@keyspace', '@write'],
  },
  {
    name: 'exists',
    handler: (ctx, args) => generic.exists(ctx.db, args),
    arity: -2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@keyspace', '@read'],
  },
  {
    name: 'type',
    handler: (ctx, args) => generic.type(ctx.db, args),
    arity: 2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@keyspace', '@read'],
  },
  {
    name: 'rename',
    handler: (ctx, args) => generic.rename(ctx.db, args),
    arity: 3,
    flags: ['write'],
    firstKey: 1,
    lastKey: 2,
    keyStep: 1,
    categories: ['@keyspace', '@write'],
  },
  {
    name: 'renamenx',
    handler: (ctx, args) => generic.renamenx(ctx.db, args),
    arity: 3,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 2,
    keyStep: 1,
    categories: ['@keyspace', '@write'],
  },
  {
    name: 'persist',
    handler: (ctx, args) => generic.persist(ctx.db, args),
    arity: 2,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@keyspace', '@write'],
  },
  {
    name: 'randomkey',
    handler: (ctx) => generic.randomkey(ctx.db),
    arity: 1,
    flags: ['readonly'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@keyspace', '@read'],
  },
  {
    name: 'touch',
    handler: (ctx, args) => generic.touch(ctx.db, args),
    arity: -2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@keyspace', '@read'],
  },
  {
    name: 'copy',
    handler: (ctx, args) => generic.copy(ctx.engine, ctx.db, args),
    arity: -3,
    flags: ['write'],
    firstKey: 1,
    lastKey: 2,
    keyStep: 1,
    categories: ['@keyspace', '@write'],
  },
  {
    name: 'object',
    handler: (ctx, args) => generic.object(ctx.db, ctx.engine.clock, args),
    arity: -2,
    flags: ['readonly'],
    firstKey: 2,
    lastKey: 2,
    keyStep: 1,
    categories: ['@keyspace', '@read'],
    subcommands: [
      {
        name: 'encoding',
        handler: (ctx, args) => generic.objectEncoding(ctx.db, args),
        arity: 3,
        flags: ['readonly'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@keyspace', '@read'],
      },
      {
        name: 'refcount',
        handler: (ctx, args) => generic.objectRefcount(ctx.db, args),
        arity: 3,
        flags: ['readonly'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@keyspace', '@read'],
      },
      {
        name: 'idletime',
        handler: (ctx, args) =>
          generic.objectIdletimeWithClock(ctx.db, ctx.engine.clock, args),
        arity: 3,
        flags: ['readonly'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@keyspace', '@read'],
      },
      {
        name: 'freq',
        handler: (ctx, args) => generic.objectFreq(ctx.db, args),
        arity: 3,
        flags: ['readonly'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@keyspace', '@read'],
      },
      {
        name: 'help',
        handler: () => generic.objectHelp(),
        arity: 2,
        flags: ['readonly'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@keyspace', '@read'],
      },
    ],
  },
  {
    name: 'wait',
    handler: () => generic.wait(),
    arity: 3,
    flags: ['readonly'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@generic'],
  },
  {
    name: 'dump',
    handler: () => generic.dump(),
    arity: 2,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@keyspace', '@read'],
  },
  {
    name: 'restore',
    handler: () => generic.restore(),
    arity: -4,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@keyspace', '@write'],
  },

  // --- TTL commands ---
  {
    name: 'expire',
    handler: (ctx, args) => ttl.expire(ctx.db, ctx.engine.clock, args),
    arity: -3,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@keyspace', '@write'],
  },
  {
    name: 'pexpire',
    handler: (ctx, args) => ttl.pexpire(ctx.db, ctx.engine.clock, args),
    arity: -3,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@keyspace', '@write'],
  },
  {
    name: 'expireat',
    handler: (ctx, args) => ttl.expireat(ctx.db, ctx.engine.clock, args),
    arity: -3,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@keyspace', '@write'],
  },
  {
    name: 'pexpireat',
    handler: (ctx, args) => ttl.pexpireat(ctx.db, ctx.engine.clock, args),
    arity: -3,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@keyspace', '@write'],
  },
  {
    name: 'ttl',
    handler: (ctx, args) => ttl.ttl(ctx.db, ctx.engine.clock, args),
    arity: 2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@keyspace', '@read'],
  },
  {
    name: 'pttl',
    handler: (ctx, args) => ttl.pttl(ctx.db, ctx.engine.clock, args),
    arity: 2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@keyspace', '@read'],
  },
  {
    name: 'expiretime',
    handler: (ctx, args) => ttl.expiretime(ctx.db, args),
    arity: 2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@keyspace', '@read'],
  },
  {
    name: 'pexpiretime',
    handler: (ctx, args) => ttl.pexpiretime(ctx.db, args),
    arity: 2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@keyspace', '@read'],
  },

  // --- Scan commands ---
  {
    name: 'keys',
    handler: (ctx, args) => scan.keys(ctx.db, args),
    arity: 2,
    flags: ['readonly'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@keyspace', '@read'],
  },
  {
    name: 'scan',
    handler: (ctx, args) => scan.scan(ctx.db, args),
    arity: -2,
    flags: ['readonly'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@keyspace', '@read'],
  },

  // --- Sort commands ---
  {
    name: 'sort',
    handler: (ctx, args) => sort.sort(ctx.db, args),
    arity: -2,
    flags: ['write', 'denyoom', 'movablekeys'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@set', '@sortedset', '@list'],
  },
  {
    name: 'sort_ro',
    handler: (ctx, args) => sort.sortRo(ctx.db, args),
    arity: -2,
    flags: ['readonly', 'movablekeys'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@set', '@sortedset', '@list'],
  },

  // --- String commands ---
  {
    name: 'get',
    handler: (ctx, args) => string.get(ctx.db, args),
    arity: 2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@string', '@fast'],
  },
  {
    name: 'set',
    handler: (ctx, args) => string.set(ctx.db, ctx.engine.clock, args),
    arity: -3,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@string'],
  },

  // --- INCR/DECR family ---
  {
    name: 'incr',
    handler: (ctx, args) => incr.incr(ctx.db, args),
    arity: 2,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@string', '@fast'],
  },
  {
    name: 'decr',
    handler: (ctx, args) => incr.decr(ctx.db, args),
    arity: 2,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@string', '@fast'],
  },
  {
    name: 'incrby',
    handler: (ctx, args) => incr.incrby(ctx.db, args),
    arity: 3,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@string', '@fast'],
  },
  {
    name: 'decrby',
    handler: (ctx, args) => incr.decrby(ctx.db, args),
    arity: 3,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@string', '@fast'],
  },
  {
    name: 'incrbyfloat',
    handler: (ctx, args) => incr.incrbyfloat(ctx.db, args),
    arity: 3,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@string', '@fast'],
  },
];

/**
 * Create a new CommandTable with all implemented commands registered.
 */
export function createCommandTable(): CommandTable {
  const table = new CommandTable();
  for (const spec of commandSpecs) {
    table.register(toDefinition(spec));
  }
  return table;
}
