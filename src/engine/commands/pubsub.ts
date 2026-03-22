/**
 * Pub/Sub command handlers: SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE,
 * PUBLISH, PUBSUB (introspection), SSUBSCRIBE, SUNSUBSCRIBE, SPUBLISH (sharded).
 *
 * SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE send one response per
 * channel/pattern (not one aggregated response).
 * Uses the 'multi' reply kind to emit multiple top-level array replies.
 *
 * PUBLISH delivers messages to all channel and pattern subscribers and returns
 * the number of recipients.
 *
 * In non-cluster mode, sharded pub/sub (SSUBSCRIBE, SUNSUBSCRIBE, SPUBLISH)
 * behaves identically to regular pub/sub.
 */

import type { CommandContext, Reply } from '../types.ts';
import {
  arrayReply,
  bulkReply,
  integerReply,
  multiReply,
  unknownSubcommandError,
  wrongArityError,
  EMPTY_ARRAY,
  ZERO,
} from '../types.ts';
import type { CommandSpec } from '../command-table.ts';

/**
 * SUBSCRIBE channel [channel ...]
 *
 * Subscribes the client to the specified channels. For each channel,
 * sends a reply: [subscribe, channelName, totalSubscriptionCount].
 * The count includes both channel and pattern subscriptions.
 */
export function subscribe(ctx: CommandContext, args: string[]): Reply {
  const pubsub = ctx.pubsub;
  const client = ctx.client;

  if (!pubsub || !client) {
    return multiReply([]);
  }

  const replies: Reply[] = [];

  for (const channel of args) {
    pubsub.subscribe(client.id, channel);
    client.flagSubscribed = true;

    const count = pubsub.subscriptionCount(client.id);
    replies.push(
      arrayReply([
        bulkReply('subscribe'),
        bulkReply(channel),
        integerReply(count),
      ])
    );
  }

  return multiReply(replies);
}

/**
 * UNSUBSCRIBE [channel ...]
 *
 * Unsubscribes the client from the specified channels, or from all channels
 * if none are specified. For each channel, sends a reply:
 * [unsubscribe, channelName, remainingSubscriptionCount].
 * The count includes both channel and pattern subscriptions.
 *
 * When remaining count reaches 0, the client exits subscriber mode.
 */
export function unsubscribe(ctx: CommandContext, args: string[]): Reply {
  const pubsub = ctx.pubsub;
  const client = ctx.client;

  if (!pubsub || !client) {
    return multiReply([
      arrayReply([bulkReply('unsubscribe'), bulkReply(null), integerReply(0)]),
    ]);
  }

  // UNSUBSCRIBE without arguments: unsubscribe from all channels
  if (args.length === 0) {
    const channels = pubsub.unsubscribeAll(client.id);

    // If no subscriptions, still send one reply with null channel
    if (channels.length === 0) {
      const remaining = pubsub.subscriptionCount(client.id);
      client.flagSubscribed = remaining > 0;
      return multiReply([
        arrayReply([
          bulkReply('unsubscribe'),
          bulkReply(null),
          integerReply(remaining),
        ]),
      ]);
    }

    const patternCount = pubsub.patternCount(client.id);
    const replies: Reply[] = [];
    for (let i = 0; i < channels.length; i++) {
      const remaining = channels.length - 1 - i + patternCount;
      const ch = channels[i] ?? '';
      replies.push(
        arrayReply([
          bulkReply('unsubscribe'),
          bulkReply(ch),
          integerReply(remaining),
        ])
      );
    }

    client.flagSubscribed = pubsub.subscriptionCount(client.id) > 0;
    return multiReply(replies);
  }

  // UNSUBSCRIBE with specific channels
  const replies: Reply[] = [];

  for (const channel of args) {
    pubsub.unsubscribe(client.id, channel);
    const remaining = pubsub.subscriptionCount(client.id);
    replies.push(
      arrayReply([
        bulkReply('unsubscribe'),
        bulkReply(channel),
        integerReply(remaining),
      ])
    );
  }

  client.flagSubscribed = pubsub.subscriptionCount(client.id) > 0;
  return multiReply(replies);
}

/**
 * PSUBSCRIBE pattern [pattern ...]
 *
 * Subscribes the client to the specified patterns. For each pattern,
 * sends a reply: [psubscribe, pattern, totalSubscriptionCount].
 * The count includes both channel and pattern subscriptions.
 */
export function psubscribe(ctx: CommandContext, args: string[]): Reply {
  const pubsub = ctx.pubsub;
  const client = ctx.client;

  if (!pubsub || !client) {
    return multiReply([]);
  }

  const replies: Reply[] = [];

  for (const pattern of args) {
    pubsub.psubscribe(client.id, pattern);
    client.flagSubscribed = true;

    const count = pubsub.subscriptionCount(client.id);
    replies.push(
      arrayReply([
        bulkReply('psubscribe'),
        bulkReply(pattern),
        integerReply(count),
      ])
    );
  }

  return multiReply(replies);
}

/**
 * PUNSUBSCRIBE [pattern ...]
 *
 * Unsubscribes the client from the specified patterns, or from all patterns
 * if none are specified. For each pattern, sends a reply:
 * [punsubscribe, pattern, remainingSubscriptionCount].
 * The count includes both channel and pattern subscriptions.
 *
 * When remaining count reaches 0, the client exits subscriber mode.
 */
export function punsubscribe(ctx: CommandContext, args: string[]): Reply {
  const pubsub = ctx.pubsub;
  const client = ctx.client;

  if (!pubsub || !client) {
    return multiReply([
      arrayReply([bulkReply('punsubscribe'), bulkReply(null), integerReply(0)]),
    ]);
  }

  // PUNSUBSCRIBE without arguments: unsubscribe from all patterns
  if (args.length === 0) {
    const patterns = pubsub.punsubscribeAll(client.id);

    // If no pattern subscriptions, still send one reply with null pattern
    if (patterns.length === 0) {
      const remaining = pubsub.subscriptionCount(client.id);
      client.flagSubscribed = remaining > 0;
      return multiReply([
        arrayReply([
          bulkReply('punsubscribe'),
          bulkReply(null),
          integerReply(remaining),
        ]),
      ]);
    }

    const channelCount = pubsub.channelCount(client.id);
    const replies: Reply[] = [];
    for (let i = 0; i < patterns.length; i++) {
      const remaining = patterns.length - 1 - i + channelCount;
      const pat = patterns[i] ?? '';
      replies.push(
        arrayReply([
          bulkReply('punsubscribe'),
          bulkReply(pat),
          integerReply(remaining),
        ])
      );
    }

    client.flagSubscribed = pubsub.subscriptionCount(client.id) > 0;
    return multiReply(replies);
  }

  // PUNSUBSCRIBE with specific patterns
  const replies: Reply[] = [];

  for (const pattern of args) {
    pubsub.punsubscribe(client.id, pattern);
    const remaining = pubsub.subscriptionCount(client.id);
    replies.push(
      arrayReply([
        bulkReply('punsubscribe'),
        bulkReply(pattern),
        integerReply(remaining),
      ])
    );
  }

  client.flagSubscribed = pubsub.subscriptionCount(client.id) > 0;
  return multiReply(replies);
}

/**
 * PUBLISH channel message
 *
 * Posts a message to the given channel. Returns the number of clients
 * that received the message (channel subscribers + pattern subscribers).
 */
export function publish(ctx: CommandContext, args: string[]): Reply {
  const pubsub = ctx.pubsub;
  if (!pubsub) {
    return ZERO;
  }

  const channel = args[0] ?? '';
  const message = args[1] ?? '';
  const count = pubsub.publish(channel, message);
  return integerReply(count);
}

// --- PUBSUB introspection ---

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

// --- Sharded pub/sub (non-cluster mode: identical to regular pub/sub) ---

/**
 * SSUBSCRIBE channel [channel ...]
 *
 * Subscribes the client to the specified shard channels.
 * Shard channels are tracked separately from regular channels.
 * Reply type is 'ssubscribe'.
 */
export function ssubscribe(ctx: CommandContext, args: string[]): Reply {
  const pubsub = ctx.pubsub;
  const client = ctx.client;

  if (!pubsub || !client) {
    return multiReply([]);
  }

  const replies: Reply[] = [];

  for (const channel of args) {
    pubsub.ssubscribe(client.id, channel);
    client.flagSubscribed = true;

    const count = pubsub.subscriptionCount(client.id);
    replies.push(
      arrayReply([
        bulkReply('ssubscribe'),
        bulkReply(channel),
        integerReply(count),
      ])
    );
  }

  return multiReply(replies);
}

/**
 * SUNSUBSCRIBE [channel ...]
 *
 * Unsubscribes the client from shard channels only (not regular channels).
 * Reply type is 'sunsubscribe'.
 */
export function sunsubscribe(ctx: CommandContext, args: string[]): Reply {
  const pubsub = ctx.pubsub;
  const client = ctx.client;

  if (!pubsub || !client) {
    return multiReply([
      arrayReply([bulkReply('sunsubscribe'), bulkReply(null), integerReply(0)]),
    ]);
  }

  // SUNSUBSCRIBE without arguments: unsubscribe from all shard channels
  if (args.length === 0) {
    const channels = pubsub.sunsubscribeAll(client.id);

    if (channels.length === 0) {
      const remaining = pubsub.subscriptionCount(client.id);
      client.flagSubscribed = remaining > 0;
      return multiReply([
        arrayReply([
          bulkReply('sunsubscribe'),
          bulkReply(null),
          integerReply(remaining),
        ]),
      ]);
    }

    const replies: Reply[] = [];
    for (let i = 0; i < channels.length; i++) {
      // Calculate remaining: rest of shard channels to unsub + regular channels + patterns
      const shardRemaining = channels.length - 1 - i;
      const remaining =
        shardRemaining +
        pubsub.channelCount(client.id) +
        pubsub.patternCount(client.id);
      const ch = channels[i] ?? '';
      replies.push(
        arrayReply([
          bulkReply('sunsubscribe'),
          bulkReply(ch),
          integerReply(remaining),
        ])
      );
    }

    client.flagSubscribed = pubsub.subscriptionCount(client.id) > 0;
    return multiReply(replies);
  }

  // SUNSUBSCRIBE with specific shard channels
  const replies: Reply[] = [];

  for (const channel of args) {
    pubsub.sunsubscribe(client.id, channel);
    const remaining = pubsub.subscriptionCount(client.id);
    replies.push(
      arrayReply([
        bulkReply('sunsubscribe'),
        bulkReply(channel),
        integerReply(remaining),
      ])
    );
  }

  client.flagSubscribed = pubsub.subscriptionCount(client.id) > 0;
  return multiReply(replies);
}

/**
 * SPUBLISH channel message
 *
 * Publishes a message to a shard channel. Only delivers to shard channel
 * subscribers (via SSUBSCRIBE). Does not deliver to pattern subscribers.
 * Message type is 'smessage'.
 */
export function spublish(ctx: CommandContext, args: string[]): Reply {
  const pubsub = ctx.pubsub;
  if (!pubsub) {
    return ZERO;
  }

  const channel = args[0] ?? '';
  const message = args[1] ?? '';
  const count = pubsub.shardPublish(channel, message);
  return integerReply(count);
}

export const specs: CommandSpec[] = [
  {
    name: 'subscribe',
    handler: (ctx, args) => subscribe(ctx, args),
    arity: -2,
    flags: ['pubsub', 'noscript', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@pubsub', '@slow'],
  },
  {
    name: 'unsubscribe',
    handler: (ctx, args) => unsubscribe(ctx, args),
    arity: -1,
    flags: ['pubsub', 'noscript', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@pubsub', '@slow'],
  },
  {
    name: 'psubscribe',
    handler: (ctx, args) => psubscribe(ctx, args),
    arity: -2,
    flags: ['pubsub', 'noscript', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@pubsub', '@slow'],
  },
  {
    name: 'punsubscribe',
    handler: (ctx, args) => punsubscribe(ctx, args),
    arity: -1,
    flags: ['pubsub', 'noscript', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@pubsub', '@slow'],
  },
  {
    name: 'publish',
    handler: (ctx, args) => publish(ctx, args),
    arity: 3,
    flags: ['pubsub', 'loading', 'stale', 'fast'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@pubsub', '@fast'],
  },
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
  {
    name: 'ssubscribe',
    handler: (ctx, args) => ssubscribe(ctx, args),
    arity: -2,
    flags: ['pubsub', 'noscript', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@pubsub', '@slow'],
  },
  {
    name: 'sunsubscribe',
    handler: (ctx, args) => sunsubscribe(ctx, args),
    arity: -1,
    flags: ['pubsub', 'noscript', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@pubsub', '@slow'],
  },
  {
    name: 'spublish',
    handler: (ctx, args) => spublish(ctx, args),
    arity: 3,
    flags: ['pubsub', 'loading', 'stale', 'fast'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@pubsub', '@fast'],
  },
];
