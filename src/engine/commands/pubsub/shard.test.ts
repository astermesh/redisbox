import { describe, it, expect } from 'vitest';
import { ssubscribe, sunsubscribe, spublish } from './shard.ts';
import { subscribe } from './pubsub.ts';
import { psubscribe } from './pattern.ts';
import { pubsubShardchannels, pubsubShardnumsub } from './introspection.ts';
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

describe('SSUBSCRIBE', () => {
  it('subscribes to a single channel with ssubscribe reply type', () => {
    const { ctx } = createCtx();
    const reply = ssubscribe(ctx, ['news']);
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
    const reply = ssubscribe(ctx, ['ch1', 'ch2']);
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
    ssubscribe(ctx, ['news']);
    expect(client.flagSubscribed).toBe(true);
  });

  it('tracks shard channels separately from regular channels', () => {
    const { ctx, pubsub, client } = createCtx();
    subscribe(ctx, ['ch1']);
    ssubscribe(ctx, ['ch2']);
    expect(pubsub.channelCount(client.id)).toBe(1);
    expect(pubsub.shardChannelCount(client.id)).toBe(1);
    expect(pubsub.subscriptionCount(client.id)).toBe(2);
  });

  it('count includes regular channel and pattern subscriptions', () => {
    const { ctx } = createCtx();
    subscribe(ctx, ['ch1']);
    psubscribe(ctx, ['p1']);
    const reply = ssubscribe(ctx, ['sch1']);
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
    ssubscribe(ctx, ['news']);
    const reply = sunsubscribe(ctx, ['news']);
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
    ssubscribe(ctx, ['ch1', 'ch2']);
    const reply = sunsubscribe(ctx, []);
    const replies = multiReplies(reply);
    expect(replies).toHaveLength(2);
    expect(client.flagSubscribed).toBe(false);
  });

  it('does not affect regular channel subscriptions', () => {
    const { ctx, client, pubsub } = createCtx();
    subscribe(ctx, ['regular']);
    ssubscribe(ctx, ['shard']);
    sunsubscribe(ctx, []);
    // Regular channel subscription should remain
    expect(pubsub.channelCount(client.id)).toBe(1);
    expect(pubsub.shardChannelCount(client.id)).toBe(0);
    expect(client.flagSubscribed).toBe(true);
  });

  it('sends null channel reply when no shard subscriptions and no args', () => {
    const { ctx } = createCtx();
    const reply = sunsubscribe(ctx, []);
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
    ssubscribe(ctx, ['news']);
    sunsubscribe(ctx, ['news']);
    expect(client.flagSubscribed).toBe(false);
  });

  it('remaining count includes regular subscriptions', () => {
    const { ctx } = createCtx();
    subscribe(ctx, ['ch1']);
    ssubscribe(ctx, ['sch1']);
    const reply = sunsubscribe(ctx, ['sch1']);
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
    const reply = spublish(ctx, ['news', 'hello']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('delivers smessage to shard channel subscribers', () => {
    const { createClient, sent } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: publisherCtx } = createClient(2);

    ssubscribe(ctx1, ['news']);
    const reply = spublish(publisherCtx, ['news', 'hello']);
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

    subscribe(ctx1, ['news']);
    const reply = spublish(publisherCtx, ['news', 'hello']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
    expect(sent).toHaveLength(0);
  });

  it('does not deliver to pattern subscribers', () => {
    const { createClient, sent } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: publisherCtx } = createClient(2);

    psubscribe(ctx1, ['new*']);
    const reply = spublish(publisherCtx, ['news', 'hello']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
    expect(sent).toHaveLength(0);
  });
});

describe('PUBSUB SHARDCHANNELS', () => {
  it('returns empty when no shard subscriptions', () => {
    const { ctx } = createCtx();
    const reply = pubsubShardchannels(ctx, []);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns only shard channels, not regular channels', () => {
    const { ctx } = createCtx();
    subscribe(ctx, ['regular']);
    ssubscribe(ctx, ['shard1', 'shard2']);
    const reply = pubsubShardchannels(ctx, []);
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
    ssubscribe(ctx, ['news.uk', 'news.us', 'sports.uk']);
    const reply = pubsubShardchannels(ctx, ['news.*']);
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
    const reply = pubsubShardnumsub(ctx, []);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns subscriber counts for shard channels only', () => {
    const { createClient } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: ctx2 } = createClient(2);

    subscribe(ctx1, ['news']); // regular subscribe
    ssubscribe(ctx2, ['news']); // shard subscribe

    const reply = pubsubShardnumsub(ctx1, ['news']);
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
