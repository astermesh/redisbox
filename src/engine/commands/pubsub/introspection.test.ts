import { describe, it, expect } from 'vitest';
import {
  pubsubChannels,
  pubsubNumsub,
  pubsubNumpat,
  pubsubHelp,
  pubsubCommand,
} from './introspection.ts';
import { specs } from './index.ts';
import { subscribe } from './pubsub.ts';
import { psubscribe } from './pattern.ts';
import { ssubscribe } from './shard.ts';
import { RedisEngine } from '../../engine.ts';
import { ClientState } from '../../../server/client-state.ts';
import type { CommandContext, Reply } from '../../types.ts';
import type { CommandSpec } from '../../command-table.ts';
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

describe('PUBSUB CHANNELS', () => {
  it('returns empty array when no channels have subscribers', () => {
    const { ctx } = createCtx();
    const reply = pubsubChannels(ctx, []);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns all active channels', () => {
    const { ctx } = createCtx();
    subscribe(ctx, ['news', 'sports', 'weather']);
    const reply = pubsubChannels(ctx, []);
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
    subscribe(ctx, ['news.uk', 'news.us', 'sports.uk']);
    const reply = pubsubChannels(ctx, ['news.*']);
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
    subscribe(ctx, ['news']);
    const reply = pubsubChannels(ctx, ['xyz*']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns sorted channels', () => {
    const { ctx } = createCtx();
    subscribe(ctx, ['z-chan', 'a-chan', 'm-chan']);
    const reply = pubsubChannels(ctx, []);
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
    const reply = pubsubNumsub(ctx, []);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns channel name and subscriber count pairs', () => {
    const { createClient } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: ctx2 } = createClient(2);

    subscribe(ctx1, ['news']);
    subscribe(ctx2, ['news']);
    subscribe(ctx1, ['sports']);

    const reply = pubsubNumsub(ctx1, ['news', 'sports', 'nonexistent']);
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
    const reply = pubsubNumpat(ctx);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns count of unique patterns', () => {
    const { createClient } = createMultiClientCtx();
    const { ctx: ctx1 } = createClient(1);
    const { ctx: ctx2 } = createClient(2);

    psubscribe(ctx1, ['news.*', 'sports.*']);
    psubscribe(ctx2, ['news.*']); // same pattern, different client

    const reply = pubsubNumpat(ctx1);
    expect(reply).toEqual({ kind: 'integer', value: 2 });
  });
});

describe('PUBSUB HELP', () => {
  it('returns array of help lines', () => {
    const reply = pubsubHelp();
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
    const reply = pubsubCommand(ctx, []);
    expect(reply.kind).toBe('error');
    if (reply.kind === 'error') {
      expect(reply.message).toContain("'pubsub'");
    }
  });

  it('dispatches CHANNELS subcommand', () => {
    const { ctx } = createCtx();
    subscribe(ctx, ['news']);
    const reply = pubsubCommand(ctx, ['CHANNELS']);
    expect(reply.kind).toBe('array');
    if (reply.kind === 'array') {
      expect(reply.value).toHaveLength(1);
    }
  });

  it('dispatches NUMSUB subcommand', () => {
    const { ctx } = createCtx();
    subscribe(ctx, ['news']);
    const reply = pubsubCommand(ctx, ['NUMSUB', 'news']);
    expect(reply.kind).toBe('array');
    if (reply.kind === 'array') {
      expect(reply.value).toHaveLength(2);
    }
  });

  it('dispatches NUMPAT subcommand', () => {
    const { ctx } = createCtx();
    const reply = pubsubCommand(ctx, ['NUMPAT']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('dispatches SHARDCHANNELS subcommand', () => {
    const { ctx } = createCtx();
    ssubscribe(ctx, ['news']);
    const reply = pubsubCommand(ctx, ['SHARDCHANNELS']);
    expect(reply.kind).toBe('array');
    if (reply.kind === 'array') {
      expect(reply.value).toHaveLength(1);
    }
  });

  it('dispatches SHARDNUMSUB subcommand', () => {
    const { ctx } = createCtx();
    ssubscribe(ctx, ['news']);
    const reply = pubsubCommand(ctx, ['SHARDNUMSUB', 'news']);
    expect(reply.kind).toBe('array');
    if (reply.kind === 'array') {
      expect(reply.value).toHaveLength(2);
    }
  });

  it('dispatches HELP subcommand', () => {
    const reply = pubsubCommand(createCtx().ctx, ['HELP']);
    expect(reply.kind).toBe('array');
  });

  it('is case-insensitive for subcommands', () => {
    const { ctx } = createCtx();
    const reply = pubsubCommand(ctx, ['numpat']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns error for unknown subcommand', () => {
    const { ctx } = createCtx();
    const reply = pubsubCommand(ctx, ['BOGUS']);
    expect(reply.kind).toBe('error');
    if (reply.kind === 'error') {
      expect(reply.message).toContain("'pubsub|bogus'");
    }
  });

  it('returns error for CHANNELS with too many args', () => {
    const { ctx } = createCtx();
    const reply = pubsubCommand(ctx, ['CHANNELS', 'a', 'b']);
    expect(reply.kind).toBe('error');
  });

  it('returns error for NUMPAT with extra args', () => {
    const { ctx } = createCtx();
    const reply = pubsubCommand(ctx, ['NUMPAT', 'extra']);
    expect(reply.kind).toBe('error');
  });
});

describe('specs', () => {
  function findSpec(name: string): CommandSpec | undefined {
    return specs.find((s) => s.name === name);
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
