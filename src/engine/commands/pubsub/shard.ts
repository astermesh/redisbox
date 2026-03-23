/**
 * Sharded pub/sub commands: SSUBSCRIBE, SUNSUBSCRIBE, SPUBLISH.
 *
 * In non-cluster mode, sharded pub/sub behaves identically to regular
 * pub/sub but uses separate channel tracking and message types.
 */

import type { CommandContext, Reply } from '../../types.ts';
import {
  arrayReply,
  bulkReply,
  integerReply,
  multiReply,
  ZERO,
} from '../../types.ts';
import type { CommandSpec } from '../../command-table.ts';

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
