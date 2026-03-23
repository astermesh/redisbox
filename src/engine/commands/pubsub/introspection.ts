/**
 * PUBSUB introspection subcommands: CHANNELS, NUMSUB, NUMPAT, SHARDCHANNELS,
 * SHARDNUMSUB, HELP, and the PUBSUB command dispatcher.
 */

import type { CommandContext, Reply } from '../../types.ts';
import {
  arrayReply,
  bulkReply,
  integerReply,
  unknownSubcommandError,
  wrongArityError,
  EMPTY_ARRAY,
  ZERO,
} from '../../types.ts';
import type { CommandSpec } from '../../command-table.ts';

const PUBSUB_HELP_LINES = [
  'PUBSUB <subcommand> [<arg> [value] [opt] ...]. subcommands are:',
  'CHANNELS [<pattern>]',
  '    Return channels that have at least one subscriber matching the optional pattern.',
  'HELP',
  '    Return subcommand help summary.',
  'NUMPAT',
  '    Return the number of unique pattern subscriptions.',
  'NUMSUB [<channel> [<channel> ...]]',
  '    Return the number of subscribers for the specified channels.',
  'SHARDCHANNELS [<pattern>]',
  '    Return shard channels that have at least one subscriber matching the optional pattern.',
  'SHARDNUMSUB [<channel> [<channel> ...]]',
  '    Return the number of subscribers for the specified shard channels.',
];

export function pubsubChannels(ctx: CommandContext, args: string[]): Reply {
  const pubsub = ctx.pubsub;
  if (!pubsub) return EMPTY_ARRAY;
  const pattern = args[0];
  const channels = pubsub.activeChannels(pattern);
  channels.sort();
  return arrayReply(channels.map((ch) => bulkReply(ch)));
}

export function pubsubNumsub(ctx: CommandContext, args: string[]): Reply {
  const pubsub = ctx.pubsub;
  if (!pubsub) return EMPTY_ARRAY;
  if (args.length === 0) return EMPTY_ARRAY;
  const pairs = pubsub.numSub(args);
  const result: Reply[] = [];
  for (const [ch, count] of pairs) {
    result.push(bulkReply(ch));
    result.push(integerReply(count));
  }
  return arrayReply(result);
}

export function pubsubShardchannels(
  ctx: CommandContext,
  args: string[]
): Reply {
  const pubsub = ctx.pubsub;
  if (!pubsub) return EMPTY_ARRAY;
  const pattern = args[0];
  const channels = pubsub.activeShardChannels(pattern);
  channels.sort();
  return arrayReply(channels.map((ch) => bulkReply(ch)));
}

export function pubsubShardnumsub(ctx: CommandContext, args: string[]): Reply {
  const pubsub = ctx.pubsub;
  if (!pubsub) return EMPTY_ARRAY;
  if (args.length === 0) return EMPTY_ARRAY;
  const pairs = pubsub.shardNumSub(args);
  const result: Reply[] = [];
  for (const [ch, count] of pairs) {
    result.push(bulkReply(ch));
    result.push(integerReply(count));
  }
  return arrayReply(result);
}

export function pubsubNumpat(ctx: CommandContext): Reply {
  const pubsub = ctx.pubsub;
  if (!pubsub) return ZERO;
  return integerReply(pubsub.numPat());
}

export function pubsubHelp(): Reply {
  return arrayReply(PUBSUB_HELP_LINES.map((l) => bulkReply(l)));
}

/**
 * PUBSUB subcommand dispatcher.
 */
export function pubsubCommand(ctx: CommandContext, args: string[]): Reply {
  if (args.length === 0) {
    return wrongArityError('pubsub');
  }

  const subcommand = (args[0] ?? '').toUpperCase();
  const subArgs = args.slice(1);

  switch (subcommand) {
    case 'CHANNELS':
      if (subArgs.length > 1) {
        return wrongArityError('pubsub|channels');
      }
      return pubsubChannels(ctx, subArgs);
    case 'NUMSUB':
      return pubsubNumsub(ctx, subArgs);
    case 'NUMPAT':
      if (subArgs.length !== 0) {
        return wrongArityError('pubsub|numpat');
      }
      return pubsubNumpat(ctx);
    case 'SHARDCHANNELS':
      if (subArgs.length > 1) {
        return wrongArityError('pubsub|shardchannels');
      }
      return pubsubShardchannels(ctx, subArgs);
    case 'SHARDNUMSUB':
      return pubsubShardnumsub(ctx, subArgs);
    case 'HELP':
      return pubsubHelp();
    default:
      return unknownSubcommandError('pubsub', (args[0] ?? '').toLowerCase());
  }
}

export const specs: CommandSpec[] = [
  {
    name: 'pubsub',
    handler: (ctx, args) => pubsubCommand(ctx, args),
    arity: -2,
    flags: ['pubsub', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@pubsub', '@slow'],
    subcommands: [
      {
        name: 'channels',
        handler: (ctx, args) => pubsubChannels(ctx, args),
        arity: -2,
        flags: ['pubsub', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@pubsub', '@slow'],
      },
      {
        name: 'numsub',
        handler: (ctx, args) => pubsubNumsub(ctx, args),
        arity: -2,
        flags: ['pubsub', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@pubsub', '@slow'],
      },
      {
        name: 'numpat',
        handler: (ctx) => pubsubNumpat(ctx),
        arity: 2,
        flags: ['pubsub', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@pubsub', '@slow'],
      },
      {
        name: 'shardchannels',
        handler: (ctx, args) => pubsubShardchannels(ctx, args),
        arity: -2,
        flags: ['pubsub', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@pubsub', '@slow'],
      },
      {
        name: 'shardnumsub',
        handler: (ctx, args) => pubsubShardnumsub(ctx, args),
        arity: -2,
        flags: ['pubsub', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@pubsub', '@slow'],
      },
      {
        name: 'help',
        handler: () => pubsubHelp(),
        arity: 2,
        flags: ['pubsub', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@pubsub', '@slow'],
      },
    ],
  },
];
