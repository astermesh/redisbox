import { describe, it, expect } from 'vitest';
import * as cmd from './pubsub.ts';
import { RedisEngine } from '../engine.ts';
import { ClientState } from '../../server/client-state.ts';
import type { CommandContext, Reply } from '../types.ts';
import { PubSubManager } from '../pubsub-manager.ts';
import type { CommandSpec } from '../command-table.ts';

function createCtx(opts?: { clientId?: number }): {
  ctx: CommandContext;
  client: ClientState;
  pubsub: PubSubManager;
} {
  const engine = new RedisEngine({ clock: () => 1000 });
  const client = new ClientState(opts?.clientId ?? 42, 500);
  const pubsub = engine.pubsub;
  return {
    ctx: {
      db: engine.db(0),
      engine,
      client,
      pubsub,
    },
    client,
    pubsub,
  };
}

function createMultiClientCtx(): {
  engine: RedisEngine;
  pubsub: PubSubManager;
  createClient: (id: number) => { ctx: CommandContext; client: ClientState };
  sent: { clientId: number; reply: Reply }[];
} {
  const engine = new RedisEngine({ clock: () => 1000 });
  const pubsub = engine.pubsub;
  const sent: { clientId: number; reply: Reply }[] = [];
  pubsub.setSender((clientId, reply) => sent.push({ clientId, reply }));

  return {
    engine,
    pubsub,
    createClient: (id: number) => {
      const client = new ClientState(id, 500);
      const ctx: CommandContext = {
        db: engine.db(0),
        engine,
        client,
        pubsub,
      };
      return { ctx, client };
    },
    sent,
  };
}

/** Extract the inner replies from a multi reply */
function multiReplies(reply: Reply): Reply[] {
  expect(reply.kind).toBe('multi');
  if (reply.kind !== 'multi') throw new Error('not multi');
  return reply.value;
}

describe('SUBSCRIBE', () => {
  it('subscribes to a single channel', () => {
    const { ctx } = createCtx();
    const reply = cmd.subscribe(ctx, ['news']);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(1);
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'subscribe' },
        { kind: 'bulk', value: 'news' },
        { kind: 'integer', value: 1 },
      ],
    });
  });

  it('subscribes to multiple channels with incrementing count', () => {
    const { ctx } = createCtx();
    const reply = cmd.subscribe(ctx, ['ch1', 'ch2', 'ch3']);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(3);

    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'subscribe' },
        { kind: 'bulk', value: 'ch1' },
        { kind: 'integer', value: 1 },
      ],
    });
    expect(replies[1]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'subscribe' },
        { kind: 'bulk', value: 'ch2' },
        { kind: 'integer', value: 2 },
      ],
    });
    expect(replies[2]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'subscribe' },
        { kind: 'bulk', value: 'ch3' },
        { kind: 'integer', value: 3 },
      ],
    });
  });

  it('sets flagSubscribed on the client', () => {
    const { ctx, client } = createCtx();
    expect(client.flagSubscribed).toBe(false);
    cmd.subscribe(ctx, ['news']);
    expect(client.flagSubscribed).toBe(true);
  });

  it('subscribing to same channel twice does not increment count', () => {
    const { ctx } = createCtx();
    cmd.subscribe(ctx, ['news']);
    const reply = cmd.subscribe(ctx, ['news']);
    const replies = multiReplies(reply);
    // Count stays at 1 since the channel was already subscribed
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'subscribe' },
        { kind: 'bulk', value: 'news' },
        { kind: 'integer', value: 1 },
      ],
    });
  });

  it('tracks subscriptions in the pubsub manager', () => {
    const { ctx, client, pubsub } = createCtx();
    cmd.subscribe(ctx, ['ch1', 'ch2']);
    expect(pubsub.channelCount(client.id)).toBe(2);
    expect(pubsub.subscribers('ch1').has(client.id)).toBe(true);
    expect(pubsub.subscribers('ch2').has(client.id)).toBe(true);
  });
});

describe('UNSUBSCRIBE', () => {
  it('unsubscribes from a single channel', () => {
    const { ctx } = createCtx();
    cmd.subscribe(ctx, ['news']);
    const reply = cmd.unsubscribe(ctx, ['news']);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(1);
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'unsubscribe' },
        { kind: 'bulk', value: 'news' },
        { kind: 'integer', value: 0 },
      ],
    });
  });

  it('unsubscribes from multiple channels with decrementing count', () => {
    const { ctx } = createCtx();
    cmd.subscribe(ctx, ['ch1', 'ch2', 'ch3']);
    const reply = cmd.unsubscribe(ctx, ['ch1', 'ch2', 'ch3']);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(3);

    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'unsubscribe' },
        { kind: 'bulk', value: 'ch1' },
        { kind: 'integer', value: 2 },
      ],
    });
    expect(replies[1]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'unsubscribe' },
        { kind: 'bulk', value: 'ch2' },
        { kind: 'integer', value: 1 },
      ],
    });
    expect(replies[2]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'unsubscribe' },
        { kind: 'bulk', value: 'ch3' },
        { kind: 'integer', value: 0 },
      ],
    });
  });

  it('clears flagSubscribed when count reaches 0', () => {
    const { ctx, client } = createCtx();
    cmd.subscribe(ctx, ['news']);
    expect(client.flagSubscribed).toBe(true);
    cmd.unsubscribe(ctx, ['news']);
    expect(client.flagSubscribed).toBe(false);
  });

  it('keeps flagSubscribed when some subscriptions remain', () => {
    const { ctx, client } = createCtx();
    cmd.subscribe(ctx, ['ch1', 'ch2']);
    cmd.unsubscribe(ctx, ['ch1']);
    expect(client.flagSubscribed).toBe(true);
  });

  it('unsubscribe without args removes all channels', () => {
    const { ctx, client, pubsub } = createCtx();
    cmd.subscribe(ctx, ['ch1', 'ch2', 'ch3']);
    const reply = cmd.unsubscribe(ctx, []);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(3);
    expect(pubsub.channelCount(client.id)).toBe(0);
    expect(client.flagSubscribed).toBe(false);

    // Last reply should have count 0
    const lastReply = replies.at(-1);
    expect(lastReply).toBeDefined();
    expect(lastReply?.kind).toBe('array');
    if (lastReply && lastReply.kind === 'array') {
      expect(lastReply.value[2]).toEqual({ kind: 'integer', value: 0 });
    }
  });

  it('unsubscribe without args and no subscriptions sends null channel reply', () => {
    const { ctx } = createCtx();
    const reply = cmd.unsubscribe(ctx, []);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(1);
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'unsubscribe' },
        { kind: 'bulk', value: null },
        { kind: 'integer', value: 0 },
      ],
    });
  });

  it('unsubscribing from non-subscribed channel still returns reply', () => {
    const { ctx } = createCtx();
    const reply = cmd.unsubscribe(ctx, ['nonexistent']);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(1);
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'unsubscribe' },
        { kind: 'bulk', value: 'nonexistent' },
        { kind: 'integer', value: 0 },
      ],
    });
  });

  it('cleans up pubsub manager state', () => {
    const { ctx, client, pubsub } = createCtx();
    cmd.subscribe(ctx, ['news']);
    cmd.unsubscribe(ctx, ['news']);
    expect(pubsub.channelCount(client.id)).toBe(0);
    expect(pubsub.subscribers('news').size).toBe(0);
  });
});

describe('SUBSCRIBE + UNSUBSCRIBE integration', () => {
  it('subscribe then partial unsubscribe preserves remaining', () => {
    const { ctx, client, pubsub } = createCtx();
    cmd.subscribe(ctx, ['a', 'b', 'c']);
    expect(pubsub.channelCount(client.id)).toBe(3);

    cmd.unsubscribe(ctx, ['b']);
    expect(pubsub.channelCount(client.id)).toBe(2);
    expect(client.flagSubscribed).toBe(true);
    expect(pubsub.clientSubscriptions(client.id).has('a')).toBe(true);
    expect(pubsub.clientSubscriptions(client.id).has('b')).toBe(false);
    expect(pubsub.clientSubscriptions(client.id).has('c')).toBe(true);
  });

  it('multiple clients subscribe to same channel independently', () => {
    const engine = new RedisEngine({ clock: () => 1000 });
    const pubsub = engine.pubsub;

    const client1 = new ClientState(1, 500);
    const client2 = new ClientState(2, 500);

    const ctx1: CommandContext = {
      db: engine.db(0),
      engine,
      client: client1,
      pubsub,
    };
    const ctx2: CommandContext = {
      db: engine.db(0),
      engine,
      client: client2,
      pubsub,
    };

    cmd.subscribe(ctx1, ['news']);
    cmd.subscribe(ctx2, ['news']);

    expect(pubsub.subscribers('news').size).toBe(2);

    cmd.unsubscribe(ctx1, ['news']);
    expect(pubsub.subscribers('news').size).toBe(1);
    expect(pubsub.subscribers('news').has(2)).toBe(true);
  });

  it('re-subscribe after unsubscribe works', () => {
    const { ctx, client, pubsub } = createCtx();
    cmd.subscribe(ctx, ['news']);
    cmd.unsubscribe(ctx, ['news']);
    expect(client.flagSubscribed).toBe(false);

    const reply = cmd.subscribe(ctx, ['news']);
    const replies = multiReplies(reply);
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'subscribe' },
        { kind: 'bulk', value: 'news' },
        { kind: 'integer', value: 1 },
      ],
    });
    expect(client.flagSubscribed).toBe(true);
    expect(pubsub.channelCount(client.id)).toBe(1);
  });
});

describe('PSUBSCRIBE', () => {
  it('subscribes to a single pattern', () => {
    const { ctx } = createCtx();
    const reply = cmd.psubscribe(ctx, ['news.*']);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(1);
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'psubscribe' },
        { kind: 'bulk', value: 'news.*' },
        { kind: 'integer', value: 1 },
      ],
    });
  });

  it('subscribes to multiple patterns with incrementing count', () => {
    const { ctx } = createCtx();
    const reply = cmd.psubscribe(ctx, ['p1', 'p2', 'p3']);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(3);

    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'psubscribe' },
        { kind: 'bulk', value: 'p1' },
        { kind: 'integer', value: 1 },
      ],
    });
    expect(replies[1]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'psubscribe' },
        { kind: 'bulk', value: 'p2' },
        { kind: 'integer', value: 2 },
      ],
    });
    expect(replies[2]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'psubscribe' },
        { kind: 'bulk', value: 'p3' },
        { kind: 'integer', value: 3 },
      ],
    });
  });

  it('sets flagSubscribed on the client', () => {
    const { ctx, client } = createCtx();
    expect(client.flagSubscribed).toBe(false);
    cmd.psubscribe(ctx, ['news.*']);
    expect(client.flagSubscribed).toBe(true);
  });

  it('subscribing to same pattern twice does not increment count', () => {
    const { ctx } = createCtx();
    cmd.psubscribe(ctx, ['news.*']);
    const reply = cmd.psubscribe(ctx, ['news.*']);
    const replies = multiReplies(reply);
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'psubscribe' },
        { kind: 'bulk', value: 'news.*' },
        { kind: 'integer', value: 1 },
      ],
    });
  });

  it('count includes both channel and pattern subscriptions', () => {
    const { ctx } = createCtx();
    cmd.subscribe(ctx, ['ch1', 'ch2']);
    const reply = cmd.psubscribe(ctx, ['p1']);
    const replies = multiReplies(reply);
    // 2 channels + 1 pattern = 3
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'psubscribe' },
        { kind: 'bulk', value: 'p1' },
        { kind: 'integer', value: 3 },
      ],
    });
  });

  it('tracks pattern subscriptions in the pubsub manager', () => {
    const { ctx, client, pubsub } = createCtx();
    cmd.psubscribe(ctx, ['p1', 'p2']);
    expect(pubsub.patternCount(client.id)).toBe(2);
    expect(pubsub.patternSubscribers('p1').has(client.id)).toBe(true);
    expect(pubsub.patternSubscribers('p2').has(client.id)).toBe(true);
  });
});

describe('PUNSUBSCRIBE', () => {
  it('unsubscribes from a single pattern', () => {
    const { ctx } = createCtx();
    cmd.psubscribe(ctx, ['news.*']);
    const reply = cmd.punsubscribe(ctx, ['news.*']);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(1);
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'punsubscribe' },
        { kind: 'bulk', value: 'news.*' },
        { kind: 'integer', value: 0 },
      ],
    });
  });

  it('unsubscribes from multiple patterns with decrementing count', () => {
    const { ctx } = createCtx();
    cmd.psubscribe(ctx, ['p1', 'p2', 'p3']);
    const reply = cmd.punsubscribe(ctx, ['p1', 'p2', 'p3']);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(3);

    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'punsubscribe' },
        { kind: 'bulk', value: 'p1' },
        { kind: 'integer', value: 2 },
      ],
    });
    expect(replies[1]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'punsubscribe' },
        { kind: 'bulk', value: 'p2' },
        { kind: 'integer', value: 1 },
      ],
    });
    expect(replies[2]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'punsubscribe' },
        { kind: 'bulk', value: 'p3' },
        { kind: 'integer', value: 0 },
      ],
    });
  });

  it('clears flagSubscribed when count reaches 0', () => {
    const { ctx, client } = createCtx();
    cmd.psubscribe(ctx, ['news.*']);
    expect(client.flagSubscribed).toBe(true);
    cmd.punsubscribe(ctx, ['news.*']);
    expect(client.flagSubscribed).toBe(false);
  });

  it('keeps flagSubscribed when channel subscriptions remain', () => {
    const { ctx, client } = createCtx();
    cmd.subscribe(ctx, ['ch1']);
    cmd.psubscribe(ctx, ['p1']);
    cmd.punsubscribe(ctx, ['p1']);
    expect(client.flagSubscribed).toBe(true);
  });

  it('punsubscribe without args removes all patterns', () => {
    const { ctx, client, pubsub } = createCtx();
    cmd.psubscribe(ctx, ['p1', 'p2', 'p3']);
    const reply = cmd.punsubscribe(ctx, []);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(3);
    expect(pubsub.patternCount(client.id)).toBe(0);
    expect(client.flagSubscribed).toBe(false);

    // Last reply should have count 0
    const lastReply = replies.at(-1);
    expect(lastReply).toBeDefined();
    expect(lastReply?.kind).toBe('array');
    if (lastReply && lastReply.kind === 'array') {
      expect(lastReply.value[2]).toEqual({ kind: 'integer', value: 0 });
    }
  });

  it('punsubscribe without args and no pattern subscriptions sends null pattern reply', () => {
    const { ctx } = createCtx();
    const reply = cmd.punsubscribe(ctx, []);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(1);
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'punsubscribe' },
        { kind: 'bulk', value: null },
        { kind: 'integer', value: 0 },
      ],
    });
  });

  it('unsubscribing from non-subscribed pattern still returns reply', () => {
    const { ctx } = createCtx();
    const reply = cmd.punsubscribe(ctx, ['nonexistent']);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(1);
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'punsubscribe' },
        { kind: 'bulk', value: 'nonexistent' },
        { kind: 'integer', value: 0 },
      ],
    });
  });

  it('count includes remaining channel subscriptions', () => {
    const { ctx } = createCtx();
    cmd.subscribe(ctx, ['ch1']);
    cmd.psubscribe(ctx, ['p1']);
    const reply = cmd.punsubscribe(ctx, ['p1']);
    const replies = multiReplies(reply);
    // 1 channel + 0 patterns = 1
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'punsubscribe' },
        { kind: 'bulk', value: 'p1' },
        { kind: 'integer', value: 1 },
      ],
    });
  });
});

describe('PSUBSCRIBE + PUNSUBSCRIBE integration', () => {
  it('re-subscribe after punsubscribe works', () => {
    const { ctx, client, pubsub } = createCtx();
    cmd.psubscribe(ctx, ['news.*']);
    cmd.punsubscribe(ctx, ['news.*']);
    expect(client.flagSubscribed).toBe(false);

    const reply = cmd.psubscribe(ctx, ['news.*']);
    const replies = multiReplies(reply);
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'psubscribe' },
        { kind: 'bulk', value: 'news.*' },
        { kind: 'integer', value: 1 },
      ],
    });
    expect(client.flagSubscribed).toBe(true);
    expect(pubsub.patternCount(client.id)).toBe(1);
  });
});

describe('PUBLISH with pattern subscribers', () => {
  it('delivers pmessage to pattern subscribers', () => {
    const { createClient, sent } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: publisherCtx } = createClient(2);

    cmd.psubscribe(ctx1, ['news.*']);
    cmd.publish(publisherCtx, ['news.breaking', 'alert']);

    expect(sent).toHaveLength(1);
    expect(sent[0]?.reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'pmessage' },
        { kind: 'bulk', value: 'news.*' },
        { kind: 'bulk', value: 'news.breaking' },
        { kind: 'bulk', value: 'alert' },
      ],
    });
  });

  it('returns count including both channel and pattern subscribers', () => {
    const { createClient } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: ctx2 } = createClient(2);
    const { ctx: publisherCtx } = createClient(3);

    cmd.subscribe(ctx1, ['news']);
    cmd.psubscribe(ctx2, ['new*']);

    const reply = cmd.publish(publisherCtx, ['news', 'hello']);
    expect(reply).toEqual({ kind: 'integer', value: 2 });
  });

  it('client with both channel and pattern sub gets both messages', () => {
    const { createClient, sent } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: publisherCtx } = createClient(2);

    cmd.subscribe(ctx1, ['news']);
    cmd.psubscribe(ctx1, ['new*']);

    const reply = cmd.publish(publisherCtx, ['news', 'hello']);
    expect(reply).toEqual({ kind: 'integer', value: 2 });
    expect(sent).toHaveLength(2);
  });
});

describe('SUBSCRIBE count includes pattern subscriptions', () => {
  it('subscribe count includes existing pattern subscriptions', () => {
    const { ctx } = createCtx();
    cmd.psubscribe(ctx, ['p1', 'p2']);
    const reply = cmd.subscribe(ctx, ['ch1']);
    const replies = multiReplies(reply);
    // 2 patterns + 1 channel = 3
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'subscribe' },
        { kind: 'bulk', value: 'ch1' },
        { kind: 'integer', value: 3 },
      ],
    });
  });

  it('unsubscribe count includes remaining pattern subscriptions', () => {
    const { ctx } = createCtx();
    cmd.psubscribe(ctx, ['p1']);
    cmd.subscribe(ctx, ['ch1']);
    const reply = cmd.unsubscribe(ctx, ['ch1']);
    const replies = multiReplies(reply);
    // 1 pattern + 0 channels = 1
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'unsubscribe' },
        { kind: 'bulk', value: 'ch1' },
        { kind: 'integer', value: 1 },
      ],
    });
  });

  it('flagSubscribed stays true when only pattern subscriptions remain after unsubscribe', () => {
    const { ctx, client } = createCtx();
    cmd.psubscribe(ctx, ['p1']);
    cmd.subscribe(ctx, ['ch1']);
    cmd.unsubscribe(ctx, ['ch1']);
    expect(client.flagSubscribed).toBe(true);
  });
});

describe('PUBLISH', () => {
  it('returns 0 when no subscribers on channel', () => {
    const { ctx } = createCtx();
    const reply = cmd.publish(ctx, ['news', 'hello']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns count of channel subscribers', () => {
    const { createClient } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: ctx2 } = createClient(2);
    const { ctx: publisherCtx } = createClient(3);

    cmd.subscribe(ctx1, ['news']);
    cmd.subscribe(ctx2, ['news']);

    const reply = cmd.publish(publisherCtx, ['news', 'hello']);
    expect(reply).toEqual({ kind: 'integer', value: 2 });
  });

  it('delivers message to all channel subscribers', () => {
    const { createClient, sent } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: ctx2 } = createClient(2);
    const { ctx: publisherCtx } = createClient(3);

    cmd.subscribe(ctx1, ['news']);
    cmd.subscribe(ctx2, ['news']);

    cmd.publish(publisherCtx, ['news', 'hello world']);

    expect(sent).toHaveLength(2);

    const msg1 = sent.find((s) => s.clientId === 1);
    const msg2 = sent.find((s) => s.clientId === 2);

    const expectedMessage = {
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'message' },
        { kind: 'bulk', value: 'news' },
        { kind: 'bulk', value: 'hello world' },
      ],
    };

    expect(msg1?.reply).toEqual(expectedMessage);
    expect(msg2?.reply).toEqual(expectedMessage);
  });

  it('does not deliver to unsubscribed clients', () => {
    const { createClient, sent } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: publisherCtx } = createClient(2);

    cmd.subscribe(ctx1, ['news']);
    cmd.unsubscribe(ctx1, ['news']);

    cmd.publish(publisherCtx, ['news', 'hello']);
    expect(sent).toHaveLength(0);
  });

  it('only delivers to subscribers of the target channel', () => {
    const { createClient, sent } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: ctx2 } = createClient(2);
    const { ctx: publisherCtx } = createClient(3);

    cmd.subscribe(ctx1, ['sports']);
    cmd.subscribe(ctx2, ['news']);

    cmd.publish(publisherCtx, ['news', 'hello']);

    expect(sent).toHaveLength(1);
    expect(sent[0]?.clientId).toBe(2);
  });

  it('works without pubsub in context', () => {
    const engine = new RedisEngine({ clock: () => 1000 });
    const ctx: CommandContext = {
      db: engine.db(0),
      engine,
    };
    const reply = cmd.publish(ctx, ['news', 'hello']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('publisher can also be a subscriber and receive its own message', () => {
    const { createClient, sent } = createMultiClientCtx();
    const { ctx } = createClient(1);

    cmd.subscribe(ctx, ['news']);

    cmd.publish(ctx, ['news', 'self']);
    expect(sent).toHaveLength(1);
    expect(sent[0]?.clientId).toBe(1);
  });
});

// --- PUBSUB introspection ---

describe('PUBSUB CHANNELS', () => {
  it('returns empty array when no channels have subscribers', () => {
    const { ctx } = createCtx();
    const reply = cmd.pubsubChannels(ctx, []);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns all active channels', () => {
    const { ctx } = createCtx();
    cmd.subscribe(ctx, ['news', 'sports', 'weather']);
    const reply = cmd.pubsubChannels(ctx, []);
    expect(reply.kind).toBe('array');
    if (reply.kind === 'array') {
      const names = reply.value.map((r) =>
        r.kind === 'bulk' ? r.value : null
      );
      expect(names).toEqual(['news', 'sports', 'weather']);
    }
  });

  it('filters channels by glob pattern', () => {
    const { ctx } = createCtx();
    cmd.subscribe(ctx, ['news.uk', 'news.us', 'sports.uk']);
    const reply = cmd.pubsubChannels(ctx, ['news.*']);
    expect(reply.kind).toBe('array');
    if (reply.kind === 'array') {
      const names = reply.value.map((r) =>
        r.kind === 'bulk' ? r.value : null
      );
      expect(names).toEqual(['news.uk', 'news.us']);
    }
  });

  it('returns empty array when pattern matches nothing', () => {
    const { ctx } = createCtx();
    cmd.subscribe(ctx, ['news']);
    const reply = cmd.pubsubChannels(ctx, ['xyz*']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns sorted channels', () => {
    const { ctx } = createCtx();
    cmd.subscribe(ctx, ['z-chan', 'a-chan', 'm-chan']);
    const reply = cmd.pubsubChannels(ctx, []);
    if (reply.kind === 'array') {
      const names = reply.value.map((r) =>
        r.kind === 'bulk' ? r.value : null
      );
      expect(names).toEqual(['a-chan', 'm-chan', 'z-chan']);
    }
  });
});

describe('PUBSUB NUMSUB', () => {
  it('returns empty array when no channels given', () => {
    const { ctx } = createCtx();
    const reply = cmd.pubsubNumsub(ctx, []);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns channel name and subscriber count pairs', () => {
    const { createClient } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: ctx2 } = createClient(2);

    cmd.subscribe(ctx1, ['news']);
    cmd.subscribe(ctx2, ['news']);
    cmd.subscribe(ctx1, ['sports']);

    const reply = cmd.pubsubNumsub(ctx1, ['news', 'sports', 'nonexistent']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'news' },
        { kind: 'integer', value: 2 },
        { kind: 'bulk', value: 'sports' },
        { kind: 'integer', value: 1 },
        { kind: 'bulk', value: 'nonexistent' },
        { kind: 'integer', value: 0 },
      ],
    });
  });
});

describe('PUBSUB NUMPAT', () => {
  it('returns 0 when no pattern subscriptions', () => {
    const { ctx } = createCtx();
    const reply = cmd.pubsubNumpat(ctx);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns count of unique patterns', () => {
    const { createClient } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: ctx2 } = createClient(2);

    cmd.psubscribe(ctx1, ['news.*', 'sports.*']);
    cmd.psubscribe(ctx2, ['news.*']); // same pattern, different client

    const reply = cmd.pubsubNumpat(ctx1);
    expect(reply).toEqual({ kind: 'integer', value: 2 });
  });
});

describe('PUBSUB HELP', () => {
  it('returns array of help lines', () => {
    const reply = cmd.pubsubHelp();
    expect(reply.kind).toBe('array');
    if (reply.kind === 'array') {
      expect(reply.value.length).toBeGreaterThan(0);
      expect(reply.value[0]).toEqual({
        kind: 'bulk',
        value:
          'PUBSUB <subcommand> [<arg> [value] [opt] ...]. subcommands are:',
      });
    }
  });
});

describe('PUBSUB command dispatcher', () => {
  it('returns wrong arity error with no subcommand', () => {
    const { ctx } = createCtx();
    const reply = cmd.pubsubCommand(ctx, []);
    expect(reply.kind).toBe('error');
    if (reply.kind === 'error') {
      expect(reply.message).toContain("'pubsub'");
    }
  });

  it('dispatches CHANNELS subcommand', () => {
    const { ctx } = createCtx();
    cmd.subscribe(ctx, ['news']);
    const reply = cmd.pubsubCommand(ctx, ['CHANNELS']);
    expect(reply.kind).toBe('array');
    if (reply.kind === 'array') {
      expect(reply.value).toHaveLength(1);
    }
  });

  it('dispatches NUMSUB subcommand', () => {
    const { ctx } = createCtx();
    cmd.subscribe(ctx, ['news']);
    const reply = cmd.pubsubCommand(ctx, ['NUMSUB', 'news']);
    expect(reply.kind).toBe('array');
    if (reply.kind === 'array') {
      expect(reply.value).toHaveLength(2);
    }
  });

  it('dispatches NUMPAT subcommand', () => {
    const { ctx } = createCtx();
    const reply = cmd.pubsubCommand(ctx, ['NUMPAT']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('dispatches SHARDCHANNELS subcommand', () => {
    const { ctx } = createCtx();
    cmd.ssubscribe(ctx, ['news']);
    const reply = cmd.pubsubCommand(ctx, ['SHARDCHANNELS']);
    expect(reply.kind).toBe('array');
    if (reply.kind === 'array') {
      expect(reply.value).toHaveLength(1);
    }
  });

  it('dispatches SHARDNUMSUB subcommand', () => {
    const { ctx } = createCtx();
    cmd.ssubscribe(ctx, ['news']);
    const reply = cmd.pubsubCommand(ctx, ['SHARDNUMSUB', 'news']);
    expect(reply.kind).toBe('array');
    if (reply.kind === 'array') {
      expect(reply.value).toHaveLength(2);
    }
  });

  it('dispatches HELP subcommand', () => {
    const reply = cmd.pubsubCommand(createCtx().ctx, ['HELP']);
    expect(reply.kind).toBe('array');
  });

  it('is case-insensitive for subcommands', () => {
    const { ctx } = createCtx();
    const reply = cmd.pubsubCommand(ctx, ['numpat']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns error for unknown subcommand', () => {
    const { ctx } = createCtx();
    const reply = cmd.pubsubCommand(ctx, ['BOGUS']);
    expect(reply.kind).toBe('error');
    if (reply.kind === 'error') {
      expect(reply.message).toContain("'pubsub|bogus'");
    }
  });

  it('returns error for CHANNELS with too many args', () => {
    const { ctx } = createCtx();
    const reply = cmd.pubsubCommand(ctx, ['CHANNELS', 'a', 'b']);
    expect(reply.kind).toBe('error');
  });

  it('returns error for NUMPAT with extra args', () => {
    const { ctx } = createCtx();
    const reply = cmd.pubsubCommand(ctx, ['NUMPAT', 'extra']);
    expect(reply.kind).toBe('error');
  });
});

// --- Sharded pub/sub ---

describe('SSUBSCRIBE', () => {
  it('subscribes to a single channel with ssubscribe reply type', () => {
    const { ctx } = createCtx();
    const reply = cmd.ssubscribe(ctx, ['news']);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(1);
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'ssubscribe' },
        { kind: 'bulk', value: 'news' },
        { kind: 'integer', value: 1 },
      ],
    });
  });

  it('subscribes to multiple channels with incrementing count', () => {
    const { ctx } = createCtx();
    const reply = cmd.ssubscribe(ctx, ['ch1', 'ch2']);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(2);
    expect(replies[1]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'ssubscribe' },
        { kind: 'bulk', value: 'ch2' },
        { kind: 'integer', value: 2 },
      ],
    });
  });

  it('sets flagSubscribed on the client', () => {
    const { ctx, client } = createCtx();
    expect(client.flagSubscribed).toBe(false);
    cmd.ssubscribe(ctx, ['news']);
    expect(client.flagSubscribed).toBe(true);
  });

  it('tracks shard channels separately from regular channels', () => {
    const { ctx, pubsub, client } = createCtx();
    cmd.subscribe(ctx, ['ch1']);
    cmd.ssubscribe(ctx, ['ch2']);
    expect(pubsub.channelCount(client.id)).toBe(1);
    expect(pubsub.shardChannelCount(client.id)).toBe(1);
    expect(pubsub.subscriptionCount(client.id)).toBe(2);
  });

  it('count includes regular channel and pattern subscriptions', () => {
    const { ctx } = createCtx();
    cmd.subscribe(ctx, ['ch1']);
    cmd.psubscribe(ctx, ['p1']);
    const reply = cmd.ssubscribe(ctx, ['sch1']);
    const replies = multiReplies(reply);
    // 1 channel + 1 pattern + 1 shard channel = 3
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'ssubscribe' },
        { kind: 'bulk', value: 'sch1' },
        { kind: 'integer', value: 3 },
      ],
    });
  });
});

describe('SUNSUBSCRIBE', () => {
  it('unsubscribes from a single shard channel with sunsubscribe reply type', () => {
    const { ctx } = createCtx();
    cmd.ssubscribe(ctx, ['news']);
    const reply = cmd.sunsubscribe(ctx, ['news']);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(1);
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'sunsubscribe' },
        { kind: 'bulk', value: 'news' },
        { kind: 'integer', value: 0 },
      ],
    });
  });

  it('unsubscribes from all shard channels when no args', () => {
    const { ctx, client } = createCtx();
    cmd.ssubscribe(ctx, ['ch1', 'ch2']);
    const reply = cmd.sunsubscribe(ctx, []);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(2);
    expect(client.flagSubscribed).toBe(false);
  });

  it('does not affect regular channel subscriptions', () => {
    const { ctx, client, pubsub } = createCtx();
    cmd.subscribe(ctx, ['regular']);
    cmd.ssubscribe(ctx, ['shard']);
    cmd.sunsubscribe(ctx, []);
    // Regular channel subscription should remain
    expect(pubsub.channelCount(client.id)).toBe(1);
    expect(pubsub.shardChannelCount(client.id)).toBe(0);
    expect(client.flagSubscribed).toBe(true);
  });

  it('sends null channel reply when no shard subscriptions and no args', () => {
    const { ctx } = createCtx();
    const reply = cmd.sunsubscribe(ctx, []);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(1);
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'sunsubscribe' },
        { kind: 'bulk', value: null },
        { kind: 'integer', value: 0 },
      ],
    });
  });

  it('clears flagSubscribed when count reaches 0', () => {
    const { ctx, client } = createCtx();
    cmd.ssubscribe(ctx, ['news']);
    cmd.sunsubscribe(ctx, ['news']);
    expect(client.flagSubscribed).toBe(false);
  });

  it('remaining count includes regular subscriptions', () => {
    const { ctx } = createCtx();
    cmd.subscribe(ctx, ['ch1']);
    cmd.ssubscribe(ctx, ['sch1']);
    const reply = cmd.sunsubscribe(ctx, ['sch1']);
    const replies = multiReplies(reply);
    // 1 regular channel remains
    expect(replies[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'sunsubscribe' },
        { kind: 'bulk', value: 'sch1' },
        { kind: 'integer', value: 1 },
      ],
    });
  });
});

describe('SPUBLISH', () => {
  it('returns 0 when no subscribers', () => {
    const { ctx } = createCtx();
    const reply = cmd.spublish(ctx, ['news', 'hello']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('delivers smessage to shard channel subscribers', () => {
    const { createClient, sent } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: publisherCtx } = createClient(2);

    cmd.ssubscribe(ctx1, ['news']);
    const reply = cmd.spublish(publisherCtx, ['news', 'hello']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
    expect(sent).toHaveLength(1);
    expect(sent[0]?.reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'smessage' },
        { kind: 'bulk', value: 'news' },
        { kind: 'bulk', value: 'hello' },
      ],
    });
  });

  it('does not deliver to regular SUBSCRIBE subscribers', () => {
    const { createClient, sent } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: publisherCtx } = createClient(2);

    cmd.subscribe(ctx1, ['news']);
    const reply = cmd.spublish(publisherCtx, ['news', 'hello']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
    expect(sent).toHaveLength(0);
  });

  it('does not deliver to pattern subscribers', () => {
    const { createClient, sent } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: publisherCtx } = createClient(2);

    cmd.psubscribe(ctx1, ['new*']);
    const reply = cmd.spublish(publisherCtx, ['news', 'hello']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
    expect(sent).toHaveLength(0);
  });
});

describe('PUBSUB SHARDCHANNELS', () => {
  it('returns empty when no shard subscriptions', () => {
    const { ctx } = createCtx();
    const reply = cmd.pubsubShardchannels(ctx, []);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns only shard channels, not regular channels', () => {
    const { ctx } = createCtx();
    cmd.subscribe(ctx, ['regular']);
    cmd.ssubscribe(ctx, ['shard1', 'shard2']);
    const reply = cmd.pubsubShardchannels(ctx, []);
    expect(reply.kind).toBe('array');
    if (reply.kind === 'array') {
      const names = reply.value.map((r) =>
        r.kind === 'bulk' ? r.value : null
      );
      expect(names).toEqual(['shard1', 'shard2']);
    }
  });

  it('filters by glob pattern', () => {
    const { ctx } = createCtx();
    cmd.ssubscribe(ctx, ['news.uk', 'news.us', 'sports.uk']);
    const reply = cmd.pubsubShardchannels(ctx, ['news.*']);
    if (reply.kind === 'array') {
      const names = reply.value.map((r) =>
        r.kind === 'bulk' ? r.value : null
      );
      expect(names).toEqual(['news.uk', 'news.us']);
    }
  });
});

describe('PUBSUB SHARDNUMSUB', () => {
  it('returns empty array when no channels given', () => {
    const { ctx } = createCtx();
    const reply = cmd.pubsubShardnumsub(ctx, []);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns subscriber counts for shard channels only', () => {
    const { createClient } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: ctx2 } = createClient(2);

    cmd.subscribe(ctx1, ['news']); // regular subscribe
    cmd.ssubscribe(ctx2, ['news']); // shard subscribe

    const reply = cmd.pubsubShardnumsub(ctx1, ['news']);
    // Only shard subscriber (ctx2)
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'news' },
        { kind: 'integer', value: 1 },
      ],
    });
  });
});

describe('specs', () => {
  function findSpec(name: string): CommandSpec | undefined {
    return cmd.specs.find((s) => s.name === name);
  }

  it('exports pubsub spec with subcommands', () => {
    const spec = findSpec('pubsub');
    expect(spec).toBeDefined();
    expect(spec?.arity).toBe(-2);
    expect(spec?.subcommands).toBeDefined();
    expect(spec?.subcommands).toHaveLength(6);
  });

  it('exports ssubscribe spec', () => {
    const spec = findSpec('ssubscribe');
    expect(spec).toBeDefined();
    expect(spec?.arity).toBe(-2);
  });

  it('exports sunsubscribe spec', () => {
    const spec = findSpec('sunsubscribe');
    expect(spec).toBeDefined();
    expect(spec?.arity).toBe(-1);
  });

  it('exports spublish spec', () => {
    const spec = findSpec('spublish');
    expect(spec).toBeDefined();
    expect(spec?.arity).toBe(3);
  });
});
