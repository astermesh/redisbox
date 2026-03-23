import { describe, it, expect } from 'vitest';
import { subscribe, unsubscribe, publish } from './pubsub.ts';
import { psubscribe } from './pattern.ts';
import { RedisEngine } from '../../engine.ts';
import { ClientState } from '../../../server/client-state.ts';
import type { CommandContext, Reply } from '../../types.ts';
import { PubSubManager } from '../../pubsub-manager.ts';

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
    const reply = subscribe(ctx, ['news']);
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
    const reply = subscribe(ctx, ['ch1', 'ch2', 'ch3']);
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
    subscribe(ctx, ['news']);
    expect(client.flagSubscribed).toBe(true);
  });

  it('subscribing to same channel twice does not increment count', () => {
    const { ctx } = createCtx();
    subscribe(ctx, ['news']);
    const reply = subscribe(ctx, ['news']);
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
    subscribe(ctx, ['ch1', 'ch2']);
    expect(pubsub.channelCount(client.id)).toBe(2);
    expect(pubsub.subscribers('ch1').has(client.id)).toBe(true);
    expect(pubsub.subscribers('ch2').has(client.id)).toBe(true);
  });
});

describe('UNSUBSCRIBE', () => {
  it('unsubscribes from a single channel', () => {
    const { ctx } = createCtx();
    subscribe(ctx, ['news']);
    const reply = unsubscribe(ctx, ['news']);
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
    subscribe(ctx, ['ch1', 'ch2', 'ch3']);
    const reply = unsubscribe(ctx, ['ch1', 'ch2', 'ch3']);
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
    subscribe(ctx, ['news']);
    expect(client.flagSubscribed).toBe(true);
    unsubscribe(ctx, ['news']);
    expect(client.flagSubscribed).toBe(false);
  });

  it('keeps flagSubscribed when some subscriptions remain', () => {
    const { ctx, client } = createCtx();
    subscribe(ctx, ['ch1', 'ch2']);
    unsubscribe(ctx, ['ch1']);
    expect(client.flagSubscribed).toBe(true);
  });

  it('unsubscribe without args removes all channels', () => {
    const { ctx, client, pubsub } = createCtx();
    subscribe(ctx, ['ch1', 'ch2', 'ch3']);
    const reply = unsubscribe(ctx, []);
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
    const reply = unsubscribe(ctx, []);
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
    const reply = unsubscribe(ctx, ['nonexistent']);
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
    subscribe(ctx, ['news']);
    unsubscribe(ctx, ['news']);
    expect(pubsub.channelCount(client.id)).toBe(0);
    expect(pubsub.subscribers('news').size).toBe(0);
  });
});

describe('SUBSCRIBE + UNSUBSCRIBE integration', () => {
  it('subscribe then partial unsubscribe preserves remaining', () => {
    const { ctx, client, pubsub } = createCtx();
    subscribe(ctx, ['a', 'b', 'c']);
    expect(pubsub.channelCount(client.id)).toBe(3);

    unsubscribe(ctx, ['b']);
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

    subscribe(ctx1, ['news']);
    subscribe(ctx2, ['news']);

    expect(pubsub.subscribers('news').size).toBe(2);

    unsubscribe(ctx1, ['news']);
    expect(pubsub.subscribers('news').size).toBe(1);
    expect(pubsub.subscribers('news').has(2)).toBe(true);
  });

  it('re-subscribe after unsubscribe works', () => {
    const { ctx, client, pubsub } = createCtx();
    subscribe(ctx, ['news']);
    unsubscribe(ctx, ['news']);
    expect(client.flagSubscribed).toBe(false);

    const reply = subscribe(ctx, ['news']);
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

describe('SUBSCRIBE count includes pattern subscriptions', () => {
  it('subscribe count includes existing pattern subscriptions', () => {
    const { ctx } = createCtx();
    psubscribe(ctx, ['p1', 'p2']);
    const reply = subscribe(ctx, ['ch1']);
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
    psubscribe(ctx, ['p1']);
    subscribe(ctx, ['ch1']);
    const reply = unsubscribe(ctx, ['ch1']);
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
    psubscribe(ctx, ['p1']);
    subscribe(ctx, ['ch1']);
    unsubscribe(ctx, ['ch1']);
    expect(client.flagSubscribed).toBe(true);
  });
});

describe('PUBLISH', () => {
  it('returns 0 when no subscribers on channel', () => {
    const { ctx } = createCtx();
    const reply = publish(ctx, ['news', 'hello']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns count of channel subscribers', () => {
    const { createClient } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: ctx2 } = createClient(2);
    const { ctx: publisherCtx } = createClient(3);

    subscribe(ctx1, ['news']);
    subscribe(ctx2, ['news']);

    const reply = publish(publisherCtx, ['news', 'hello']);
    expect(reply).toEqual({ kind: 'integer', value: 2 });
  });

  it('delivers message to all channel subscribers', () => {
    const { createClient, sent } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: ctx2 } = createClient(2);
    const { ctx: publisherCtx } = createClient(3);

    subscribe(ctx1, ['news']);
    subscribe(ctx2, ['news']);

    publish(publisherCtx, ['news', 'hello world']);

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

    subscribe(ctx1, ['news']);
    unsubscribe(ctx1, ['news']);

    publish(publisherCtx, ['news', 'hello']);
    expect(sent).toHaveLength(0);
  });

  it('only delivers to subscribers of the target channel', () => {
    const { createClient, sent } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: ctx2 } = createClient(2);
    const { ctx: publisherCtx } = createClient(3);

    subscribe(ctx1, ['sports']);
    subscribe(ctx2, ['news']);

    publish(publisherCtx, ['news', 'hello']);

    expect(sent).toHaveLength(1);
    expect(sent[0]?.clientId).toBe(2);
  });

  it('works without pubsub in context', () => {
    const engine = new RedisEngine({ clock: () => 1000 });
    const ctx: CommandContext = {
      db: engine.db(0),
      engine,
    };
    const reply = publish(ctx, ['news', 'hello']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('publisher can also be a subscriber and receive its own message', () => {
    const { createClient, sent } = createMultiClientCtx();
    const { ctx } = createClient(1);

    subscribe(ctx, ['news']);

    publish(ctx, ['news', 'self']);
    expect(sent).toHaveLength(1);
    expect(sent[0]?.clientId).toBe(1);
  });
});
