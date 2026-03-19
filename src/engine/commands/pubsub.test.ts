import { describe, it, expect } from 'vitest';
import * as cmd from './pubsub.ts';
import { RedisEngine } from '../engine.ts';
import { ClientState } from '../../server/client-state.ts';
import type { CommandContext, Reply } from '../types.ts';
import { PubSubManager } from '../pubsub-manager.ts';

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
