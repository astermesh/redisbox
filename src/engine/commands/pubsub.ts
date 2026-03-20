/**
 * Pub/Sub command handlers: SUBSCRIBE, UNSUBSCRIBE, PUBLISH.
 *
 * SUBSCRIBE/UNSUBSCRIBE send one response per channel (not one aggregated response).
 * Uses the 'multi' reply kind to emit multiple top-level array replies.
 *
 * PUBLISH delivers messages to all channel subscribers and returns the
 * number of recipients.
 */

import type { CommandContext, Reply } from '../types.ts';
import {
  arrayReply,
  bulkReply,
  integerReply,
  multiReply,
  ZERO,
} from '../types.ts';

/**
 * SUBSCRIBE channel [channel ...]
 *
 * Subscribes the client to the specified channels. For each channel,
 * sends a reply: [subscribe, channelName, totalSubscriptionCount].
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

    const count = pubsub.channelCount(client.id);
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
      client.flagSubscribed = false;
      return multiReply([
        arrayReply([
          bulkReply('unsubscribe'),
          bulkReply(null),
          integerReply(0),
        ]),
      ]);
    }

    const replies: Reply[] = [];
    for (let i = 0; i < channels.length; i++) {
      const remaining = channels.length - 1 - i;
      const ch = channels[i] ?? '';
      replies.push(
        arrayReply([
          bulkReply('unsubscribe'),
          bulkReply(ch),
          integerReply(remaining),
        ])
      );
    }

    client.flagSubscribed = pubsub.channelCount(client.id) > 0;
    return multiReply(replies);
  }

  // UNSUBSCRIBE with specific channels
  const replies: Reply[] = [];

  for (const channel of args) {
    pubsub.unsubscribe(client.id, channel);
    const remaining = pubsub.channelCount(client.id);
    replies.push(
      arrayReply([
        bulkReply('unsubscribe'),
        bulkReply(channel),
        integerReply(remaining),
      ])
    );
  }

  client.flagSubscribed = pubsub.channelCount(client.id) > 0;
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
