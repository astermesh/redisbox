import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import type { CommandContext } from '../../types.ts';
import { ClientState, ClientStateStore } from '../../../server/client-state.ts';
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
