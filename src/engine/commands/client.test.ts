import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { CommandContext } from '../types.ts';
import { ClientState, ClientStateStore } from '../../server/client-state.ts';
import * as cmd from './client.ts';

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

// --- CLIENT ID ---

describe('CLIENT ID', () => {
  it('returns current client id', () => {
    const { ctx } = createCtx({ clientId: 42 });
    expect(cmd.clientId(ctx.client)).toEqual({ kind: 'integer', value: 42 });
  });

  it('returns 0 when no client', () => {
    expect(cmd.clientId(undefined)).toEqual({ kind: 'integer', value: 0 });
  });
});

// --- CLIENT GETNAME ---

describe('CLIENT GETNAME', () => {
  it('returns nil when no name set', () => {
    const { ctx } = createCtx();
    expect(cmd.clientGetname(ctx.client)).toEqual({
      kind: 'bulk',
      value: null,
    });
  });

  it('returns name when set', () => {
    const { ctx, client } = createCtx();
    client.name = 'my-connection';
    expect(cmd.clientGetname(ctx.client)).toEqual({
      kind: 'bulk',
      value: 'my-connection',
    });
  });

  it('returns nil when no client', () => {
    expect(cmd.clientGetname(undefined)).toEqual({
      kind: 'bulk',
      value: null,
    });
  });
});

// --- CLIENT SETNAME ---

describe('CLIENT SETNAME', () => {
  it('sets client name and returns OK', () => {
    const { ctx, client } = createCtx();
    expect(cmd.clientSetname(ctx.client, ['test-client'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
    expect(client.name).toBe('test-client');
  });

  it('clears name with empty string', () => {
    const { ctx, client } = createCtx();
    client.name = 'old-name';
    expect(cmd.clientSetname(ctx.client, [''])).toEqual({
      kind: 'status',
      value: 'OK',
    });
    expect(client.name).toBe('');
  });

  it('rejects names with spaces', () => {
    const { ctx } = createCtx();
    const reply = cmd.clientSetname(ctx.client, ['bad name']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'Client names cannot contain spaces, newlines or special characters.',
    });
  });

  it('rejects names with newlines', () => {
    const { ctx } = createCtx();
    const reply = cmd.clientSetname(ctx.client, ['bad\nname']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'Client names cannot contain spaces, newlines or special characters.',
    });
  });

  it('rejects names with control characters', () => {
    const { ctx } = createCtx();
    const reply = cmd.clientSetname(ctx.client, ['bad\x01name']);
    expect(reply.kind).toBe('error');
  });

  it('rejects names with characters above ASCII 126', () => {
    const { ctx } = createCtx();
    const reply = cmd.clientSetname(ctx.client, ['café']);
    expect(reply.kind).toBe('error');
  });

  it('accepts names with hyphens and underscores', () => {
    const { ctx, client } = createCtx();
    cmd.clientSetname(ctx.client, ['my_client-name']);
    expect(client.name).toBe('my_client-name');
  });

  it('works without client context', () => {
    expect(cmd.clientSetname(undefined, ['test'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
  });
});

// --- CLIENT INFO ---

describe('CLIENT INFO', () => {
  it('returns formatted client info', () => {
    const { ctx, client } = createCtx({ time: 10000 });
    client.lastCommand = 'get';
    client.lastCommandTime = 10000;
    const reply = cmd.clientInfo(ctx.client, ctx.engine.clock);
    expect(reply.kind).toBe('bulk');
    if (reply.kind === 'bulk' && reply.value) {
      expect(reply.value).toContain('id=1');
      expect(reply.value).toContain('cmd=get');
      expect(reply.value).toContain('flags=N');
      expect(reply.value).toContain('db=0');
      expect(reply.value).toContain('age=0');
      expect(reply.value).toContain('idle=0');
      expect(reply.value?.endsWith('\n')).toBe(true);
    }
  });

  it('shows correct age and idle time', () => {
    const { ctx, client, setTime } = createCtx({ time: 1000 });
    client.lastCommandTime = 1000;
    setTime(6000); // 5 seconds later
    const reply = cmd.clientInfo(ctx.client, ctx.engine.clock);
    expect(reply.kind).toBe('bulk');
    if (reply.kind === 'bulk' && reply.value) {
      expect(reply.value).toContain('age=5');
      expect(reply.value).toContain('idle=5');
    }
  });

  it('shows idle=age when no command executed', () => {
    const { ctx, setTime } = createCtx({ time: 1000 });
    setTime(4000);
    const reply = cmd.clientInfo(ctx.client, ctx.engine.clock);
    if (reply.kind === 'bulk' && reply.value) {
      expect(reply.value).toContain('age=3');
      expect(reply.value).toContain('idle=3');
    }
  });

  it('returns empty for no client', () => {
    const engine = new RedisEngine({ clock: () => 1000 });
    expect(cmd.clientInfo(undefined, engine.clock)).toEqual({
      kind: 'bulk',
      value: '',
    });
  });

  it('shows name when set', () => {
    const { ctx, client } = createCtx();
    client.name = 'worker-1';
    const reply = cmd.clientInfo(ctx.client, ctx.engine.clock);
    if (reply.kind === 'bulk' && reply.value) {
      expect(reply.value).toContain('name=worker-1');
    }
  });

  it('shows multi=-1 when not in MULTI', () => {
    const { ctx } = createCtx();
    const reply = cmd.clientInfo(ctx.client, ctx.engine.clock);
    if (reply.kind === 'bulk' && reply.value) {
      expect(reply.value).toContain('multi=-1');
    }
  });

  it('shows multi=0 when in MULTI', () => {
    const { ctx, client } = createCtx();
    client.flagMulti = true;
    const reply = cmd.clientInfo(ctx.client, ctx.engine.clock);
    if (reply.kind === 'bulk' && reply.value) {
      expect(reply.value).toContain('multi=0');
    }
  });

  it('includes tracking redirect', () => {
    const { ctx, client } = createCtx();
    client.trackingRedirect = 42;
    const reply = cmd.clientInfo(ctx.client, ctx.engine.clock);
    if (reply.kind === 'bulk' && reply.value) {
      expect(reply.value).toContain('redir=42');
    }
  });

  it('includes resp version', () => {
    const { ctx } = createCtx();
    const reply = cmd.clientInfo(ctx.client, ctx.engine.clock);
    if (reply.kind === 'bulk' && reply.value) {
      expect(reply.value).toContain('resp=2');
    }
  });

  it('has correct field order matching Redis', () => {
    const { ctx } = createCtx({ time: 1000 });
    const reply = cmd.clientInfo(ctx.client, ctx.engine.clock);
    if (reply.kind === 'bulk' && reply.value) {
      const line = reply.value.trim();
      // Verify key fields appear in correct Redis order
      const idPos = line.indexOf('id=');
      const namePos = line.indexOf('name=');
      const agePos = line.indexOf('age=');
      const idlePos = line.indexOf('idle=');
      const flagsPos = line.indexOf('flags=');
      const dbPos = line.indexOf('db=');
      const cmdPos = line.indexOf('cmd=');
      expect(idPos).toBeLessThan(namePos);
      expect(namePos).toBeLessThan(agePos);
      expect(agePos).toBeLessThan(idlePos);
      expect(idlePos).toBeLessThan(flagsPos);
      expect(flagsPos).toBeLessThan(dbPos);
      expect(dbPos).toBeLessThan(cmdPos);
    }
  });
});

// --- CLIENT LIST ---

describe('CLIENT LIST', () => {
  it('lists all clients', () => {
    const { ctx, store } = createCtx({ time: 1000 });
    store.create(2, 1000);
    store.create(3, 1000);
    const reply = cmd.clientList(
      ctx.clientStore,
      ctx.client,
      ctx.engine.clock,
      []
    );
    expect(reply.kind).toBe('bulk');
    if (reply.kind === 'bulk' && reply.value) {
      const lines = reply.value.trim().split('\n');
      expect(lines.length).toBe(3);
      expect(lines[0]).toContain('id=1');
      expect(lines[1]).toContain('id=2');
      expect(lines[2]).toContain('id=3');
    }
  });

  it('filters by TYPE normal', () => {
    const { ctx, store } = createCtx({ time: 1000 });
    const c2 = store.create(2, 1000);
    c2.flagSubscribed = true;
    const reply = cmd.clientList(
      ctx.clientStore,
      ctx.client,
      ctx.engine.clock,
      ['TYPE', 'normal']
    );
    if (reply.kind === 'bulk' && reply.value) {
      const lines = reply.value.trim().split('\n');
      expect(lines.length).toBe(1);
      expect(lines[0]).toContain('id=1');
    }
  });

  it('filters by TYPE pubsub', () => {
    const { ctx, store } = createCtx({ time: 1000 });
    const c2 = store.create(2, 1000);
    c2.flagSubscribed = true;
    const reply = cmd.clientList(
      ctx.clientStore,
      ctx.client,
      ctx.engine.clock,
      ['TYPE', 'pubsub']
    );
    if (reply.kind === 'bulk' && reply.value) {
      const lines = reply.value.trim().split('\n');
      expect(lines.length).toBe(1);
      expect(lines[0]).toContain('id=2');
    }
  });

  it('filters by ID', () => {
    const { ctx, store } = createCtx({ time: 1000 });
    store.create(2, 1000);
    store.create(3, 1000);
    const reply = cmd.clientList(
      ctx.clientStore,
      ctx.client,
      ctx.engine.clock,
      ['ID', '1', '3']
    );
    if (reply.kind === 'bulk' && reply.value) {
      const lines = reply.value.trim().split('\n');
      expect(lines.length).toBe(2);
      expect(lines[0]).toContain('id=1');
      expect(lines[1]).toContain('id=3');
    }
  });

  it('returns empty bulk for no matching clients', () => {
    const { ctx } = createCtx({ time: 1000 });
    const reply = cmd.clientList(
      ctx.clientStore,
      ctx.client,
      ctx.engine.clock,
      ['ID', '999']
    );
    expect(reply).toEqual({ kind: 'bulk', value: '' });
  });

  it('works without clientStore (single client fallback)', () => {
    const { ctx } = createCtx({ clientStore: false });
    const reply = cmd.clientList(undefined, ctx.client, ctx.engine.clock, []);
    if (reply.kind === 'bulk' && reply.value) {
      expect(reply.value).toContain('id=1');
    }
  });
});

// --- CLIENT KILL ---

describe('CLIENT KILL', () => {
  it('kills by ID', () => {
    const { client, store } = createCtx({ time: 1000 });
    store.create(2, 1000);
    expect(store.has(2)).toBe(true);
    const reply = cmd.clientKill(store, client, ['ID', '2']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
    expect(store.has(2)).toBe(false);
  });

  it('returns 0 for non-existent ID', () => {
    const { client, store } = createCtx({ time: 1000 });
    const reply = cmd.clientKill(store, client, ['ID', '999']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('skips self by default (SKIPME yes)', () => {
    const { client, store } = createCtx({ time: 1000 });
    const reply = cmd.clientKill(store, client, ['ID', '1']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
    expect(store.has(1)).toBe(true);
  });

  it('kills self with SKIPME no', () => {
    const { client, store } = createCtx({ time: 1000 });
    const reply = cmd.clientKill(store, client, ['ID', '1', 'SKIPME', 'no']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
    expect(store.has(1)).toBe(false);
  });

  it('old-style single arg returns error', () => {
    const { client, store } = createCtx({ time: 1000 });
    const reply = cmd.clientKill(store, client, ['127.0.0.1:1234']);
    expect(reply.kind).toBe('error');
  });

  it('returns error for missing args', () => {
    const reply = cmd.clientKill(undefined, undefined, []);
    expect(reply.kind).toBe('error');
  });
});

// --- CLIENT PAUSE/UNPAUSE ---

describe('CLIENT PAUSE', () => {
  it('returns OK', () => {
    expect(cmd.clientPause()).toEqual({ kind: 'status', value: 'OK' });
  });
});

describe('CLIENT UNPAUSE', () => {
  it('returns OK', () => {
    expect(cmd.clientUnpause()).toEqual({ kind: 'status', value: 'OK' });
  });
});

// --- CLIENT REPLY ---

describe('CLIENT REPLY', () => {
  it('accepts ON', () => {
    expect(cmd.clientReply(['ON'])).toEqual({ kind: 'status', value: 'OK' });
  });

  it('accepts OFF', () => {
    expect(cmd.clientReply(['OFF'])).toEqual({ kind: 'status', value: 'OK' });
  });

  it('accepts SKIP', () => {
    expect(cmd.clientReply(['SKIP'])).toEqual({ kind: 'status', value: 'OK' });
  });

  it('rejects invalid mode with syntax error', () => {
    const reply = cmd.clientReply(['INVALID']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });
});

// --- CLIENT NO-EVICT ---

describe('CLIENT NO-EVICT', () => {
  it('sets noEvict on', () => {
    const { ctx, client } = createCtx();
    expect(cmd.clientNoEvict(ctx.client, ['ON'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
    expect(client.noEvict).toBe(true);
  });

  it('sets noEvict off', () => {
    const { ctx, client } = createCtx();
    client.noEvict = true;
    expect(cmd.clientNoEvict(ctx.client, ['OFF'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
    expect(client.noEvict).toBe(false);
  });

  it('is case-insensitive', () => {
    const { ctx, client } = createCtx();
    cmd.clientNoEvict(ctx.client, ['on']);
    expect(client.noEvict).toBe(true);
  });

  it('rejects invalid argument with syntax error', () => {
    const { ctx } = createCtx();
    const reply = cmd.clientNoEvict(ctx.client, ['MAYBE']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('works without client', () => {
    expect(cmd.clientNoEvict(undefined, ['ON'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
  });
});

// --- CLIENT NO-TOUCH ---

describe('CLIENT NO-TOUCH', () => {
  it('sets noTouch on', () => {
    const { ctx, client } = createCtx();
    expect(cmd.clientNoTouch(ctx.client, ['ON'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
    expect(client.noTouch).toBe(true);
  });

  it('sets noTouch off', () => {
    const { ctx, client } = createCtx();
    client.noTouch = true;
    cmd.clientNoTouch(ctx.client, ['OFF']);
    expect(client.noTouch).toBe(false);
  });

  it('rejects invalid argument with syntax error', () => {
    const { ctx } = createCtx();
    const reply = cmd.clientNoTouch(ctx.client, ['MAYBE']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });
});

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

// --- CLIENT HELP ---

describe('CLIENT HELP', () => {
  it('returns array of help lines', () => {
    const reply = cmd.clientHelp();
    expect(reply.kind).toBe('array');
    if (reply.kind === 'array') {
      expect(reply.value.length).toBeGreaterThan(0);
      expect(reply.value[0]).toEqual({
        kind: 'bulk',
        value: expect.stringContaining('CLIENT'),
      });
    }
  });
});

// --- CLIENT dispatch ---

describe('CLIENT (dispatch)', () => {
  it('dispatches ID subcommand', () => {
    const { ctx } = createCtx({ clientId: 7 });
    const reply = cmd.client(ctx, ['ID']);
    expect(reply).toEqual({ kind: 'integer', value: 7 });
  });

  it('dispatches GETNAME subcommand', () => {
    const { ctx, client } = createCtx();
    client.name = 'test';
    const reply = cmd.client(ctx, ['GETNAME']);
    expect(reply).toEqual({ kind: 'bulk', value: 'test' });
  });

  it('dispatches SETNAME subcommand', () => {
    const { ctx, client } = createCtx();
    const reply = cmd.client(ctx, ['SETNAME', 'hello']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
    expect(client.name).toBe('hello');
  });

  it('SETNAME requires exactly one argument', () => {
    const { ctx } = createCtx();
    const reply = cmd.client(ctx, ['SETNAME']);
    expect(reply.kind).toBe('error');
  });

  it('dispatches LIST subcommand', () => {
    const { ctx } = createCtx();
    const reply = cmd.client(ctx, ['LIST']);
    expect(reply.kind).toBe('bulk');
  });

  it('dispatches INFO subcommand', () => {
    const { ctx } = createCtx();
    const reply = cmd.client(ctx, ['INFO']);
    expect(reply.kind).toBe('bulk');
  });

  it('dispatches HELP subcommand', () => {
    const reply = cmd.client({ db: null as never, engine: null as never }, [
      'HELP',
    ]);
    expect(reply.kind).toBe('array');
  });

  it('dispatches NO-EVICT subcommand', () => {
    const { ctx, client } = createCtx();
    cmd.client(ctx, ['NO-EVICT', 'ON']);
    expect(client.noEvict).toBe(true);
  });

  it('dispatches NO-TOUCH subcommand', () => {
    const { ctx, client } = createCtx();
    cmd.client(ctx, ['NO-TOUCH', 'ON']);
    expect(client.noTouch).toBe(true);
  });

  it('dispatches TRACKING subcommand', () => {
    const { ctx, client } = createCtx();
    cmd.client(ctx, ['TRACKING', 'ON', 'BCAST']);
    expect(client.tracking).toBe(true);
    expect(client.trackingMode).toBe('bcast');
  });

  it('dispatches CACHING subcommand', () => {
    const { ctx, client } = createCtx();
    client.tracking = true;
    client.trackingMode = 'optin';
    const reply = cmd.client(ctx, ['CACHING', 'YES']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('dispatches TRACKINGINFO subcommand', () => {
    const { ctx } = createCtx();
    const reply = cmd.client(ctx, ['TRACKINGINFO']);
    expect(reply.kind).toBe('array');
  });

  it('dispatches GETREDIR subcommand', () => {
    const { ctx } = createCtx();
    const reply = cmd.client(ctx, ['GETREDIR']);
    expect(reply).toEqual({ kind: 'integer', value: -1 });
  });

  it('dispatches REPLY subcommand', () => {
    const { ctx } = createCtx();
    const reply = cmd.client(ctx, ['REPLY', 'ON']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('dispatches PAUSE subcommand', () => {
    const { ctx } = createCtx();
    const reply = cmd.client(ctx, ['PAUSE']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('dispatches UNPAUSE subcommand', () => {
    const { ctx } = createCtx();
    const reply = cmd.client(ctx, ['UNPAUSE']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('dispatches KILL subcommand', () => {
    const { ctx, store } = createCtx();
    store.create(2, 1000);
    const reply = cmd.client(ctx, ['KILL', 'ID', '2']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
  });

  it('returns error for unknown subcommand', () => {
    const { ctx } = createCtx();
    const reply = cmd.client(ctx, ['BADCOMMAND']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        "unknown subcommand or wrong number of arguments for 'client|badcommand' command",
    });
  });

  it('returns error for empty subcommand', () => {
    const { ctx } = createCtx();
    const reply = cmd.client(ctx, []);
    expect(reply.kind).toBe('error');
  });

  it('is case-insensitive for subcommands', () => {
    const { ctx } = createCtx({ clientId: 10 });
    expect(cmd.client(ctx, ['id'])).toEqual({ kind: 'integer', value: 10 });
    expect(cmd.client(ctx, ['Id'])).toEqual({ kind: 'integer', value: 10 });
    expect(cmd.client(ctx, ['iD'])).toEqual({ kind: 'integer', value: 10 });
  });
});
