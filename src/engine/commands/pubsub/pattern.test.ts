import { describe, it, expect } from 'vitest';
import { psubscribe, punsubscribe } from './pattern.ts';
import { subscribe } from './pubsub.ts';
import { publish } from './pubsub.ts';
import { RedisEngine } from '../../engine.ts';
import { ClientState } from '../../../server/client-state.ts';
import type { CommandContext, Reply } from '../../types.ts';
import { PubSubManager } from '../../pubsub/pubsub-manager.ts';

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

describe('PSUBSCRIBE', () => {
  it('subscribes to a single pattern', () => {
    const { ctx } = createCtx();
    const reply = psubscribe(ctx, ['news.*']);
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
    const reply = psubscribe(ctx, ['p1', 'p2', 'p3']);
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
    psubscribe(ctx, ['news.*']);
    expect(client.flagSubscribed).toBe(true);
  });

  it('subscribing to same pattern twice does not increment count', () => {
    const { ctx } = createCtx();
    psubscribe(ctx, ['news.*']);
    const reply = psubscribe(ctx, ['news.*']);
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
    subscribe(ctx, ['ch1', 'ch2']);
    const reply = psubscribe(ctx, ['p1']);
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
    psubscribe(ctx, ['p1', 'p2']);
    expect(pubsub.patternCount(client.id)).toBe(2);
    expect(pubsub.patternSubscribers('p1').has(client.id)).toBe(true);
    expect(pubsub.patternSubscribers('p2').has(client.id)).toBe(true);
  });
});

describe('PUNSUBSCRIBE', () => {
  it('unsubscribes from a single pattern', () => {
    const { ctx } = createCtx();
    psubscribe(ctx, ['news.*']);
    const reply = punsubscribe(ctx, ['news.*']);
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
    psubscribe(ctx, ['p1', 'p2', 'p3']);
    const reply = punsubscribe(ctx, ['p1', 'p2', 'p3']);
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
    psubscribe(ctx, ['news.*']);
    expect(client.flagSubscribed).toBe(true);
    punsubscribe(ctx, ['news.*']);
    expect(client.flagSubscribed).toBe(false);
  });

  it('keeps flagSubscribed when channel subscriptions remain', () => {
    const { ctx, client } = createCtx();
    subscribe(ctx, ['ch1']);
    psubscribe(ctx, ['p1']);
    punsubscribe(ctx, ['p1']);
    expect(client.flagSubscribed).toBe(true);
  });

  it('punsubscribe without args removes all patterns', () => {
    const { ctx, client, pubsub } = createCtx();
    psubscribe(ctx, ['p1', 'p2', 'p3']);
    const reply = punsubscribe(ctx, []);
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
    const reply = punsubscribe(ctx, []);
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
    const reply = punsubscribe(ctx, ['nonexistent']);
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
    subscribe(ctx, ['ch1']);
    psubscribe(ctx, ['p1']);
    const reply = punsubscribe(ctx, ['p1']);
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
    psubscribe(ctx, ['news.*']);
    punsubscribe(ctx, ['news.*']);
    expect(client.flagSubscribed).toBe(false);

    const reply = psubscribe(ctx, ['news.*']);
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

    psubscribe(ctx1, ['news.*']);
    publish(publisherCtx, ['news.breaking', 'alert']);

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

    subscribe(ctx1, ['news']);
    psubscribe(ctx2, ['new*']);

    const reply = publish(publisherCtx, ['news', 'hello']);
    expect(reply).toEqual({ kind: 'integer', value: 2 });
  });

  it('client with both channel and pattern sub gets both messages', () => {
    const { createClient, sent } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: publisherCtx } = createClient(2);

    subscribe(ctx1, ['news']);
    psubscribe(ctx1, ['new*']);

    const reply = publish(publisherCtx, ['news', 'hello']);
    expect(reply).toEqual({ kind: 'integer', value: 2 });
    expect(sent).toHaveLength(2);
  });
});
