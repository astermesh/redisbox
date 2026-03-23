/**
 * Pattern subscription commands: PSUBSCRIBE, PUNSUBSCRIBE.
 *
 * PSUBSCRIBE/PUNSUBSCRIBE send one response per pattern (not one aggregated
 * response). Uses the 'multi' reply kind to emit multiple top-level array
 * replies.
 */

import type { CommandContext, Reply } from '../../types.ts';
import {
  arrayReply,
  bulkReply,
  integerReply,
  multiReply,
} from '../../types.ts';
import type { CommandSpec } from '../../command-table.ts';

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

export const specs: CommandSpec[] = [
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
];
