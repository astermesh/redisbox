import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import type { CommandContext } from '../../types.ts';
import { ClientState, ClientStateStore } from '../../../server/client-state.ts';
import * as cmd from './tracking.ts';

function createCtx(opts?: {
  time?: number;
  clientId?: number;
  clientStore?: boolean;
}): {
  ctx: CommandContext;
  client: ClientState;
  store: ClientStateStore;
  setTime: (t: number) => void;
} {
  let now = opts?.time ?? 1000;
  const engine = new RedisEngine({
    clock: () => now,
    rng: () => 0.5,
  });
  const store = new ClientStateStore();
  const clientId = opts?.clientId ?? 1;
  const client = store.create(clientId, now);

  const ctx: CommandContext = {
    db: engine.db(0),
    engine,
    client,
    clientStore: opts?.clientStore !== false ? store : undefined,
  };

  return {
    ctx,
    client,
    store,
    setTime: (t: number) => {
      now = t;
    },
  };
}

// --- CLIENT TRACKING ---

describe('CLIENT TRACKING', () => {
  it('enables tracking in normal mode', () => {
    const { ctx, client } = createCtx();
    const reply = cmd.clientTracking(ctx.client, ctx.clientStore, ['ON']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
    expect(client.tracking).toBe(true);
    expect(client.trackingMode).toBe('normal');
  });

  it('enables tracking in BCAST mode', () => {
    const { ctx, client } = createCtx();
    cmd.clientTracking(ctx.client, ctx.clientStore, ['ON', 'BCAST']);
    expect(client.tracking).toBe(true);
    expect(client.trackingMode).toBe('bcast');
  });

  it('enables tracking with OPTIN', () => {
    const { ctx, client } = createCtx();
    cmd.clientTracking(ctx.client, ctx.clientStore, ['ON', 'OPTIN']);
    expect(client.trackingMode).toBe('optin');
  });

  it('enables tracking with OPTOUT', () => {
    const { ctx, client } = createCtx();
    cmd.clientTracking(ctx.client, ctx.clientStore, ['ON', 'OPTOUT']);
    expect(client.trackingMode).toBe('optout');
  });

  it('rejects OPTIN and OPTOUT together', () => {
    const { ctx } = createCtx();
    const reply = cmd.clientTracking(ctx.client, ctx.clientStore, [
      'ON',
      'OPTIN',
      'OPTOUT',
    ]);
    expect(reply.kind).toBe('error');
  });

  it('enables tracking with REDIRECT', () => {
    const { ctx, client, store } = createCtx();
    store.create(2, 1000);
    cmd.clientTracking(ctx.client, ctx.clientStore, ['ON', 'REDIRECT', '2']);
    expect(client.trackingRedirect).toBe(2);
  });

  it('rejects REDIRECT to non-existent client', () => {
    const { ctx } = createCtx();
    const reply = cmd.clientTracking(ctx.client, ctx.clientStore, [
      'ON',
      'REDIRECT',
      '999',
    ]);
    expect(reply.kind).toBe('error');
  });

  it('enables tracking with PREFIX in BCAST mode', () => {
    const { ctx, client } = createCtx();
    cmd.clientTracking(ctx.client, ctx.clientStore, [
      'ON',
      'BCAST',
      'PREFIX',
      'user:',
      'PREFIX',
      'session:',
    ]);
    expect(client.trackingPrefixes).toEqual(['user:', 'session:']);
  });

  it('rejects PREFIX without BCAST', () => {
    const { ctx } = createCtx();
    const reply = cmd.clientTracking(ctx.client, ctx.clientStore, [
      'ON',
      'PREFIX',
      'user:',
    ]);
    expect(reply.kind).toBe('error');
  });

  it('enables NOLOOP', () => {
    const { ctx, client } = createCtx();
    cmd.clientTracking(ctx.client, ctx.clientStore, ['ON', 'NOLOOP']);
    expect(client.trackingNoloop).toBe(true);
  });

  it('disables tracking', () => {
    const { ctx, client } = createCtx();
    client.tracking = true;
    client.trackingMode = 'bcast';
    client.trackingRedirect = 5;
    client.trackingPrefixes = ['foo:'];
    client.trackingNoloop = true;

    const reply = cmd.clientTracking(ctx.client, ctx.clientStore, ['OFF']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
    expect(client.tracking).toBe(false);
    expect(client.trackingMode).toBeNull();
    expect(client.trackingRedirect).toBe(0);
    expect(client.trackingPrefixes).toEqual([]);
    expect(client.trackingNoloop).toBe(false);
  });

  it('rejects invalid toggle', () => {
    const { ctx } = createCtx();
    const reply = cmd.clientTracking(ctx.client, ctx.clientStore, ['MAYBE']);
    expect(reply.kind).toBe('error');
  });

  it('rejects empty args', () => {
    const { ctx } = createCtx();
    const reply = cmd.clientTracking(ctx.client, ctx.clientStore, []);
    expect(reply.kind).toBe('error');
  });

  it('adds t flag when tracking is on', () => {
    const { client } = createCtx();
    client.tracking = true;
    expect(client.flagsString()).toBe('t');
  });
});

// --- CLIENT CACHING ---

describe('CLIENT CACHING', () => {
  it('accepts YES when tracking is OPTIN', () => {
    const { ctx, client } = createCtx();
    client.tracking = true;
    client.trackingMode = 'optin';
    expect(cmd.clientCaching(ctx.client, ['YES'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
  });

  it('accepts NO when tracking is OPTOUT', () => {
    const { ctx, client } = createCtx();
    client.tracking = true;
    client.trackingMode = 'optout';
    expect(cmd.clientCaching(ctx.client, ['NO'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
  });

  it('rejects when tracking is off', () => {
    const { ctx } = createCtx();
    const reply = cmd.clientCaching(ctx.client, ['YES']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled',
    });
  });

  it('rejects YES when tracking is in normal mode', () => {
    const { ctx, client } = createCtx();
    client.tracking = true;
    client.trackingMode = 'normal';
    const reply = cmd.clientCaching(ctx.client, ['YES']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.',
    });
  });

  it('rejects NO when tracking is in normal mode', () => {
    const { ctx, client } = createCtx();
    client.tracking = true;
    client.trackingMode = 'normal';
    const reply = cmd.clientCaching(ctx.client, ['NO']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.',
    });
  });

  it('rejects YES when tracking is OPTOUT (wrong mode)', () => {
    const { ctx, client } = createCtx();
    client.tracking = true;
    client.trackingMode = 'optout';
    const reply = cmd.clientCaching(ctx.client, ['YES']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.',
    });
  });

  it('rejects NO when tracking is OPTIN (wrong mode)', () => {
    const { ctx, client } = createCtx();
    client.tracking = true;
    client.trackingMode = 'optin';
    const reply = cmd.clientCaching(ctx.client, ['NO']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.',
    });
  });

  it('rejects invalid argument with syntax error', () => {
    const { ctx, client } = createCtx();
    client.tracking = true;
    client.trackingMode = 'optin';
    const reply = cmd.clientCaching(ctx.client, ['MAYBE']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });
});

// --- CLIENT TRACKINGINFO ---

describe('CLIENT TRACKINGINFO', () => {
  it('returns off when tracking disabled', () => {
    const { ctx } = createCtx();
    const reply = cmd.clientTrackinginfo(ctx.client);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'flags' },
        { kind: 'array', value: [{ kind: 'bulk', value: 'off' }] },
        { kind: 'bulk', value: 'redirect' },
        { kind: 'integer', value: 0 },
        { kind: 'bulk', value: 'prefixes' },
        { kind: 'array', value: [] },
      ],
    });
  });

  it('returns on with mode flags when tracking enabled', () => {
    const { ctx, client } = createCtx();
    client.tracking = true;
    client.trackingMode = 'bcast';
    client.trackingNoloop = true;
    client.trackingRedirect = 5;
    client.trackingPrefixes = ['user:'];

    const reply = cmd.clientTrackinginfo(ctx.client);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'flags' },
        {
          kind: 'array',
          value: [
            { kind: 'bulk', value: 'on' },
            { kind: 'bulk', value: 'bcast' },
            { kind: 'bulk', value: 'noloop' },
          ],
        },
        { kind: 'bulk', value: 'redirect' },
        { kind: 'integer', value: 5 },
        { kind: 'bulk', value: 'prefixes' },
        { kind: 'array', value: [{ kind: 'bulk', value: 'user:' }] },
      ],
    });
  });

  it('returns defaults when no client', () => {
    const reply = cmd.clientTrackinginfo(undefined);
    expect(reply.kind).toBe('array');
    if (reply.kind === 'array') {
      expect(reply.value[1]).toEqual({
        kind: 'array',
        value: [{ kind: 'bulk', value: 'off' }],
      });
    }
  });
});

// --- CLIENT GETREDIR ---

describe('CLIENT GETREDIR', () => {
  it('returns -1 when tracking off', () => {
    const { ctx } = createCtx();
    expect(cmd.clientGetredir(ctx.client)).toEqual({
      kind: 'integer',
      value: -1,
    });
  });

  it('returns redirect target when tracking on', () => {
    const { ctx, client } = createCtx();
    client.tracking = true;
    client.trackingRedirect = 42;
    expect(cmd.clientGetredir(ctx.client)).toEqual({
      kind: 'integer',
      value: 42,
    });
  });

  it('returns 0 when tracking on with no redirect', () => {
    const { ctx, client } = createCtx();
    client.tracking = true;
    expect(cmd.clientGetredir(ctx.client)).toEqual({
      kind: 'integer',
      value: 0,
    });
  });

  it('returns -1 when no client', () => {
    expect(cmd.clientGetredir(undefined)).toEqual({
      kind: 'integer',
      value: -1,
    });
  });
});
