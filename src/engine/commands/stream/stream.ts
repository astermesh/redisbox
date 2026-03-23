import type { CommandSpec } from '../../command-table.ts';
import { OK } from '../../types.ts';
import { notify, EVENT_FLAGS } from '../../pubsub/notify.ts';
import { getStream } from './utils.ts';
import { xadd, xlen, xdel, xtrim, xsetid } from './write.ts';
import { xrange, xrevrange, xread } from './read.ts';
import { xgroup } from './group.ts';
import { xreadgroup, xclaim, xautoclaim, xack, xpending } from './consumer.ts';
import { xinfo } from './info.ts';

export const specs: CommandSpec[] = [
  {
    name: 'xadd',
    handler: (ctx, args) => {
      const key = args[0] ?? '';
      // Capture entry count before to detect trimming
      const hasTrim = args.some((a) => {
        const u = a.toUpperCase();
        return u === 'MAXLEN' || u === 'MINID';
      });
      const pre = getStream(ctx.db, key);
      const lengthBefore = pre.stream ? pre.stream.length : 0;
      const reply = xadd(ctx.db, ctx.engine.clock(), args);
      if (reply.kind === 'bulk' && reply.value !== null) {
        notify(ctx, EVENT_FLAGS.STREAM, 'xadd', key);
        // Emit secondary xtrim if trimming actually removed entries
        if (hasTrim) {
          const post = getStream(ctx.db, key);
          const lengthAfter = post.stream ? post.stream.length : 0;
          // After adding 1 entry, if length didn't increase by 1, trimming removed entries
          if (lengthAfter < lengthBefore + 1) {
            notify(ctx, EVENT_FLAGS.STREAM, 'xtrim', key);
          }
        }
      }
      return reply;
    },
    arity: -5,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@stream', '@fast'],
  },
  {
    name: 'xlen',
    handler: (ctx, args) => xlen(ctx.db, args),
    arity: 2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@stream', '@fast'],
  },
  {
    name: 'xrange',
    handler: (ctx, args) => xrange(ctx.db, args),
    arity: -4,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@stream', '@slow'],
  },
  {
    name: 'xrevrange',
    handler: (ctx, args) => xrevrange(ctx.db, args),
    arity: -4,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@stream', '@slow'],
  },
  {
    name: 'xread',
    handler: (ctx, args) => xread(ctx.db, args),
    arity: -4,
    flags: ['readonly', 'blocking', 'movablekeys'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@read', '@stream', '@slow', '@blocking'],
  },
  {
    name: 'xgroup',
    handler: (ctx, args) => xgroup(ctx, args),
    arity: -2,
    flags: ['write'],
    firstKey: 2,
    lastKey: 2,
    keyStep: 1,
    categories: ['@write', '@stream', '@slow'],
    subcommands: [
      {
        name: 'xgroup|create',
        handler: (ctx, args) => xgroup(ctx, ['CREATE', ...args]),
        arity: -5,
        flags: ['write'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@write', '@stream', '@slow'],
      },
      {
        name: 'xgroup|setid',
        handler: (ctx, args) => xgroup(ctx, ['SETID', ...args]),
        arity: -5,
        flags: ['write'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@write', '@stream', '@slow'],
      },
      {
        name: 'xgroup|destroy',
        handler: (ctx, args) => xgroup(ctx, ['DESTROY', ...args]),
        arity: 4,
        flags: ['write'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@write', '@stream', '@slow'],
      },
      {
        name: 'xgroup|delconsumer',
        handler: (ctx, args) => xgroup(ctx, ['DELCONSUMER', ...args]),
        arity: 5,
        flags: ['write'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@write', '@stream', '@slow'],
      },
      {
        name: 'xgroup|createconsumer',
        handler: (ctx, args) => xgroup(ctx, ['CREATECONSUMER', ...args]),
        arity: 5,
        flags: ['write'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@write', '@stream', '@slow'],
      },
    ],
  },
  {
    name: 'xreadgroup',
    handler: (ctx, args) => xreadgroup(ctx.db, ctx.engine.clock(), args),
    arity: -7,
    flags: ['write', 'blocking', 'movablekeys'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@write', '@stream', '@slow', '@blocking'],
  },
  {
    name: 'xack',
    handler: (ctx, args) => xack(ctx.db, args),
    arity: -4,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@stream', '@fast'],
  },
  {
    name: 'xpending',
    handler: (ctx, args) => xpending(ctx.db, ctx.engine.clock(), args),
    arity: -3,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@stream', '@slow'],
  },
  {
    name: 'xdel',
    handler: (ctx, args) => {
      const reply = xdel(ctx.db, args);
      if (reply.kind === 'integer' && (reply.value as number) > 0) {
        notify(ctx, EVENT_FLAGS.STREAM, 'xdel', args[0] ?? '');
      }
      return reply;
    },
    arity: -3,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@stream', '@fast'],
  },
  {
    name: 'xtrim',
    handler: (ctx, args) => {
      const reply = xtrim(ctx.db, args);
      if (reply.kind === 'integer' && (reply.value as number) > 0) {
        notify(ctx, EVENT_FLAGS.STREAM, 'xtrim', args[0] ?? '');
      }
      return reply;
    },
    arity: -4,
    flags: ['write'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@stream', '@slow'],
  },
  {
    name: 'xsetid',
    handler: (ctx, args) => {
      const reply = xsetid(ctx.db, args);
      if (reply === OK) {
        notify(ctx, EVENT_FLAGS.STREAM, 'xsetid', args[0] ?? '');
      }
      return reply;
    },
    arity: -3,
    flags: ['write'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@stream', '@slow'],
  },
  {
    name: 'xclaim',
    handler: (ctx, args) => xclaim(ctx.db, ctx.engine.clock(), args),
    arity: -6,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@stream', '@fast'],
  },
  {
    name: 'xautoclaim',
    handler: (ctx, args) => xautoclaim(ctx.db, ctx.engine.clock(), args),
    arity: -7,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@stream', '@fast'],
  },
  {
    name: 'xinfo',
    handler: (ctx, args) => xinfo(ctx, args),
    arity: -2,
    flags: ['readonly'],
    firstKey: 2,
    lastKey: 2,
    keyStep: 1,
    categories: ['@read', '@stream', '@slow'],
    subcommands: [
      {
        name: 'xinfo|stream',
        handler: (ctx, args) => xinfo(ctx, ['STREAM', ...args]),
        arity: -3,
        flags: ['readonly'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@read', '@stream', '@slow'],
      },
      {
        name: 'xinfo|groups',
        handler: (ctx, args) => xinfo(ctx, ['GROUPS', ...args]),
        arity: 3,
        flags: ['readonly'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@read', '@stream', '@slow'],
      },
      {
        name: 'xinfo|consumers',
        handler: (ctx, args) => xinfo(ctx, ['CONSUMERS', ...args]),
        arity: 4,
        flags: ['readonly'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@read', '@stream', '@slow'],
      },
      {
        name: 'xinfo|help',
        handler: (ctx, args) => xinfo(ctx, ['HELP', ...args]),
        arity: 2,
        flags: ['readonly'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@read', '@stream', '@slow'],
      },
    ],
  },
];
