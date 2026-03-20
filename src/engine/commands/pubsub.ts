/**
 * Pub/Sub command handlers: SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH.
 *
 * SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE send one response per
 * channel/pattern (not one aggregated response).
 * Uses the 'multi' reply kind to emit multiple top-level array replies.
 *
 * PUBLISH delivers messages to all channel and pattern subscribers and returns
 * the number of recipients.
 */

import type { CommandContext, Reply } from '../types.ts';
import {
  arrayReply,
  bulkReply,
  integerReply,
  multiReply,
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
];
