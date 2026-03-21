import { describe, it, expect, beforeEach } from 'vitest';
import {
  CommandDispatcher,
  createTransactionState,
} from './command-dispatcher.ts';
import type { TransactionState } from './command-dispatcher.ts';
import { createCommandTable } from './command-registry.ts';
import { CommandTable } from './command-table.ts';
import type { CommandDefinition, CommandHandler } from './command-table.ts';
import { RedisEngine } from './engine.ts';
import type { CommandContext, Reply } from './types.ts';
import { statusReply, errorReply } from './types.ts';
import { ClientState as ClientStateObj } from '../server/client-state.ts';
import { ConfigStore } from '../config-store.ts';

function createCtx(clock = 1000): {
  ctx: CommandContext;
  engine: RedisEngine;
  setTime: (t: number) => void;
} {
  let now = clock;
  const engine = new RedisEngine({ clock: () => now, rng: () => 0.5 });
  return {
    ctx: { db: engine.db(0), engine },
    engine,
    setTime: (t: number) => {
      now = t;
    },
  };
}

function stubHandler(reply: Reply = statusReply('OK')): CommandHandler {
  return () => reply;
}

function makeDef(
  overrides: Partial<CommandDefinition> = {}
): CommandDefinition {
  return {
    name: 'test',
    handler: stubHandler(),
    arity: 1,
    flags: new Set(),
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: new Set(),
    ...overrides,
  };
}

describe('CommandDispatcher', () => {
  let table: CommandTable;
  let dispatcher: CommandDispatcher;
  let state: TransactionState;
  let ctx: CommandContext;

  beforeEach(() => {
    table = createCommandTable();
    dispatcher = new CommandDispatcher(table);
    state = createTransactionState();
    const setup = createCtx();
    ctx = setup.ctx;
  });

  describe('basic routing', () => {
    it('routes GET command to handler', () => {
      ctx.db.set('k', 'string', 'raw', 'hello');
      const result = dispatcher.dispatch(state, ctx, ['GET', 'k']);
      expect(result).toEqual({ kind: 'bulk', value: 'hello' });
    });

    it('routes SET command to handler', () => {
      const result = dispatcher.dispatch(state, ctx, ['SET', 'k', 'val']);
      expect(result).toEqual({ kind: 'status', value: 'OK' });
      expect(ctx.db.get('k')?.value).toBe('val');
    });

    it('routes DEL command to handler', () => {
      ctx.db.set('a', 'string', 'raw', '1');
      ctx.db.set('b', 'string', 'raw', '2');
      const result = dispatcher.dispatch(state, ctx, ['DEL', 'a', 'b']);
      expect(result).toEqual({ kind: 'integer', value: 2 });
    });

    it('routes EXISTS command to handler', () => {
      ctx.db.set('x', 'string', 'raw', 'v');
      const result = dispatcher.dispatch(state, ctx, ['EXISTS', 'x']);
      expect(result).toEqual({ kind: 'integer', value: 1 });
    });

    it('routes TYPE command to handler', () => {
      ctx.db.set('k', 'string', 'raw', 'v');
      const result = dispatcher.dispatch(state, ctx, ['TYPE', 'k']);
      expect(result).toEqual({ kind: 'status', value: 'string' });
    });
  });

  describe('command name normalization', () => {
    it('handles lowercase command names', () => {
      ctx.db.set('k', 'string', 'raw', 'hello');
      const result = dispatcher.dispatch(state, ctx, ['get', 'k']);
      expect(result).toEqual({ kind: 'bulk', value: 'hello' });
    });

    it('handles mixed-case command names', () => {
      ctx.db.set('k', 'string', 'raw', 'hello');
      const result = dispatcher.dispatch(state, ctx, ['Get', 'k']);
      expect(result).toEqual({ kind: 'bulk', value: 'hello' });
    });

    it('handles uppercase command names', () => {
      ctx.db.set('k', 'string', 'raw', 'hello');
      const result = dispatcher.dispatch(state, ctx, ['GET', 'k']);
      expect(result).toEqual({ kind: 'bulk', value: 'hello' });
    });
  });

  describe('unknown command', () => {
    it('returns error for unknown command', () => {
      const result = dispatcher.dispatch(state, ctx, ['BADCMD']);
      expect(result.kind).toBe('error');
      expect(result).toEqual(
        errorReply(
          'ERR',
          "unknown command 'BADCMD', with args beginning with: "
        )
      );
    });

    it('includes args in error message', () => {
      const result = dispatcher.dispatch(state, ctx, [
        'BADCMD',
        'arg1',
        'arg2',
      ]);
      expect(result.kind).toBe('error');
      const err = result as { kind: 'error'; prefix: string; message: string };
      expect(err.message).toContain("unknown command 'BADCMD'");
      expect(err.message).toContain("'arg1'");
      expect(err.message).toContain("'arg2'");
    });

    it('preserves original command case in error', () => {
      const result = dispatcher.dispatch(state, ctx, ['BadCmd', 'x']);
      expect(result.kind).toBe('error');
      const err = result as { kind: 'error'; prefix: string; message: string };
      expect(err.message).toContain("'BadCmd'");
    });

    it('returns correct error format for empty command array', () => {
      const result = dispatcher.dispatch(state, ctx, []);
      expect(result).toEqual(
        errorReply('ERR', "unknown command '', with args beginning with: ")
      );
    });
  });

  describe('arity validation', () => {
    it('rejects GET with no args (needs exactly 2 including cmd name)', () => {
      const result = dispatcher.dispatch(state, ctx, ['GET']);
      expect(result.kind).toBe('error');
      const err = result as { kind: 'error'; message: string };
      expect(err.message).toContain("wrong number of arguments for 'get'");
    });

    it('rejects GET with too many args', () => {
      const result = dispatcher.dispatch(state, ctx, ['GET', 'k1', 'k2']);
      expect(result.kind).toBe('error');
      const err = result as { kind: 'error'; message: string };
      expect(err.message).toContain("wrong number of arguments for 'get'");
    });

    it('rejects SET with too few args', () => {
      const result = dispatcher.dispatch(state, ctx, ['SET', 'k']);
      expect(result.kind).toBe('error');
      const err = result as { kind: 'error'; message: string };
      expect(err.message).toContain("wrong number of arguments for 'set'");
    });

    it('accepts SET with variable args (EX, NX, etc)', () => {
      const result = dispatcher.dispatch(state, ctx, [
        'SET',
        'k',
        'v',
        'EX',
        '10',
      ]);
      expect(result.kind).toBe('status');
    });

    it('accepts DEL with multiple keys', () => {
      const result = dispatcher.dispatch(state, ctx, ['DEL', 'a', 'b', 'c']);
      expect(result).toEqual({ kind: 'integer', value: 0 });
    });

    it('rejects RENAME with too few args', () => {
      const result = dispatcher.dispatch(state, ctx, ['RENAME', 'src']);
      expect(result.kind).toBe('error');
      const err = result as { kind: 'error'; message: string };
      expect(err.message).toContain("wrong number of arguments for 'rename'");
    });
  });

  describe('subcommand dispatch', () => {
    it('dispatches OBJECT ENCODING to subcommand handler', () => {
      ctx.db.set('k', 'string', 'embstr', 'hi');
      const result = dispatcher.dispatch(state, ctx, [
        'OBJECT',
        'ENCODING',
        'k',
      ]);
      expect(result).toEqual({ kind: 'bulk', value: 'embstr' });
    });

    it('dispatches OBJECT REFCOUNT to subcommand handler', () => {
      ctx.db.set('k', 'string', 'raw', 'hello');
      const result = dispatcher.dispatch(state, ctx, [
        'OBJECT',
        'REFCOUNT',
        'k',
      ]);
      expect(result).toEqual({ kind: 'integer', value: 1 });
    });

    it('dispatches OBJECT HELP', () => {
      const result = dispatcher.dispatch(state, ctx, ['OBJECT', 'HELP']);
      expect(result.kind).toBe('array');
    });

    it('handles unknown subcommand via parent handler', () => {
      const result = dispatcher.dispatch(state, ctx, [
        'OBJECT',
        'BADCMD',
        'key',
      ]);
      expect(result.kind).toBe('error');
      const err = result as { kind: 'error'; message: string };
      expect(err.message).toContain("'object|badcmd'");
    });

    it('validates arity for subcommands', () => {
      // OBJECT ENCODING needs exactly 3 args (OBJECT ENCODING key)
      const result = dispatcher.dispatch(state, ctx, ['OBJECT', 'ENCODING']);
      expect(result.kind).toBe('error');
      const err = result as { kind: 'error'; message: string };
      expect(err.message).toContain("'object|encoding'");
    });

    it('subcommand name is case-insensitive', () => {
      ctx.db.set('k', 'string', 'embstr', 'hi');
      const lower = dispatcher.dispatch(state, ctx, [
        'object',
        'encoding',
        'k',
      ]);
      const upper = dispatcher.dispatch(state, ctx, [
        'OBJECT',
        'ENCODING',
        'k',
      ]);
      const mixed = dispatcher.dispatch(state, ctx, [
        'Object',
        'Encoding',
        'k',
      ]);
      expect(lower).toEqual(upper);
      expect(upper).toEqual(mixed);
    });
  });

  describe('MULTI mode queuing', () => {
    beforeEach(() => {
      state.inMulti = true;
    });

    it('queues regular commands and returns QUEUED', () => {
      const result = dispatcher.dispatch(state, ctx, ['SET', 'k', 'v']);
      expect(result).toEqual(statusReply('QUEUED'));
      expect(state.multiQueue).toHaveLength(1);
    });

    it('queues multiple commands in order', () => {
      dispatcher.dispatch(state, ctx, ['SET', 'k', 'v']);
      dispatcher.dispatch(state, ctx, ['GET', 'k']);
      dispatcher.dispatch(state, ctx, ['DEL', 'k']);
      expect(state.multiQueue).toHaveLength(3);
      expect(state.multiQueue[0]?.def.name).toBe('set');
      expect(state.multiQueue[1]?.def.name).toBe('get');
      expect(state.multiQueue[2]?.def.name).toBe('del');
    });

    it('stores correct args in queue (without command name)', () => {
      dispatcher.dispatch(state, ctx, ['SET', 'mykey', 'myval']);
      expect(state.multiQueue[0]?.args).toEqual(['mykey', 'myval']);
    });

    it('does not queue EXEC', () => {
      // EXEC is not registered yet, so it returns unknown command error
      // but the key point is it should NOT be queued
      dispatcher.dispatch(state, ctx, ['EXEC']);
      expect(state.multiQueue).toHaveLength(0);
    });

    it('does not queue DISCARD', () => {
      dispatcher.dispatch(state, ctx, ['DISCARD']);
      expect(state.multiQueue).toHaveLength(0);
    });

    it('returns error for nested MULTI', () => {
      const result = dispatcher.dispatch(state, ctx, ['MULTI']);
      expect(result).toEqual(
        errorReply('ERR', 'MULTI calls can not be nested')
      );
      expect(state.multiQueue).toHaveLength(0);
    });

    it('returns error for WATCH inside MULTI', () => {
      const result = dispatcher.dispatch(state, ctx, ['WATCH', 'k']);
      expect(result).toEqual(
        errorReply('ERR', 'WATCH inside MULTI is not allowed')
      );
      expect(state.multiQueue).toHaveLength(0);
    });

    it('does not queue unknown commands and flags transaction dirty', () => {
      const result = dispatcher.dispatch(state, ctx, ['BADCMD', 'arg']);
      expect(result.kind).toBe('error');
      expect(state.multiQueue).toHaveLength(0);
      expect(state.multiDirty).toBe(true);
    });

    it('does not queue commands with arity errors and flags dirty', () => {
      const result = dispatcher.dispatch(state, ctx, ['GET']);
      expect(result.kind).toBe('error');
      expect(state.multiQueue).toHaveLength(0);
      expect(state.multiDirty).toBe(true);
    });

    it('queues commands with subcommands', () => {
      ctx.db.set('k', 'string', 'embstr', 'v');
      const result = dispatcher.dispatch(state, ctx, [
        'OBJECT',
        'ENCODING',
        'k',
      ]);
      expect(result).toEqual(statusReply('QUEUED'));
      expect(state.multiQueue).toHaveLength(1);
      expect(state.multiQueue[0]?.def.name).toBe('object');
    });

    it('rejects subcommand arity error in MULTI and flags dirty', () => {
      const result = dispatcher.dispatch(state, ctx, ['OBJECT', 'ENCODING']);
      expect(result.kind).toBe('error');
      expect(state.multiQueue).toHaveLength(0);
      expect(state.multiDirty).toBe(true);
    });
  });

  describe('subscribe mode restrictions', () => {
    beforeEach(() => {
      state.subscribed = true;
    });

    it('rejects GET in subscribe mode', () => {
      const result = dispatcher.dispatch(state, ctx, ['GET', 'k']);
      expect(result.kind).toBe('error');
      const err = result as { kind: 'error'; message: string };
      expect(err.message).toContain("Can't execute 'get'");
      expect(err.message).toContain(
        'only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context'
      );
    });

    it('rejects SET in subscribe mode', () => {
      const result = dispatcher.dispatch(state, ctx, ['SET', 'k', 'v']);
      expect(result.kind).toBe('error');
      const err = result as { kind: 'error'; message: string };
      expect(err.message).toContain("Can't execute 'set'");
    });

    it('rejects DEL in subscribe mode', () => {
      const result = dispatcher.dispatch(state, ctx, ['DEL', 'k']);
      expect(result.kind).toBe('error');
    });

    it('allows SUBSCRIBE in subscribe mode', () => {
      const result = dispatcher.dispatch(state, ctx, ['SUBSCRIBE', 'ch']);
      // Should not be blocked — returns a multi reply, not subscribe mode error
      expect(result.kind).not.toBe('error');
    });

    it('allows UNSUBSCRIBE in subscribe mode', () => {
      const result = dispatcher.dispatch(state, ctx, ['UNSUBSCRIBE', 'ch']);
      // Should not be blocked — returns a multi reply, not subscribe mode error
      expect(result.kind).not.toBe('error');
    });

    it('allows PSUBSCRIBE in subscribe mode', () => {
      const result = dispatcher.dispatch(state, ctx, ['PSUBSCRIBE', 'p*']);
      // Should not be blocked — returns a multi reply, not subscribe mode error
      expect(result.kind).not.toBe('error');
    });

    it('allows PUNSUBSCRIBE in subscribe mode', () => {
      const result = dispatcher.dispatch(state, ctx, ['PUNSUBSCRIBE', 'p*']);
      // Should not be blocked — returns a multi reply, not subscribe mode error
      expect(result.kind).not.toBe('error');
    });

    it('allows PING in subscribe mode and returns push-style array', () => {
      const client = new ClientStateObj(1, 100);
      client.flagSubscribed = true;
      const subCtx: CommandContext = { ...ctx, client };
      const result = dispatcher.dispatch(state, subCtx, ['PING']);
      expect(result).toEqual({
        kind: 'array',
        value: [
          { kind: 'bulk', value: 'pong' },
          { kind: 'bulk', value: '' },
        ],
      });
    });

    it('PING with message in subscribe mode returns push-style array with message', () => {
      const client = new ClientStateObj(1, 100);
      client.flagSubscribed = true;
      const subCtx: CommandContext = { ...ctx, client };
      const result = dispatcher.dispatch(state, subCtx, ['PING', 'hello']);
      expect(result).toEqual({
        kind: 'array',
        value: [
          { kind: 'bulk', value: 'pong' },
          { kind: 'bulk', value: 'hello' },
        ],
      });
    });

    it('allows RESET in subscribe mode and clears subscribed flag', () => {
      const result = dispatcher.dispatch(state, ctx, ['RESET']);
      expect(result).toEqual({ kind: 'status', value: 'RESET' });
      expect(state.subscribed).toBe(false);
    });

    it('allows QUIT in subscribe mode', () => {
      const result = dispatcher.dispatch(state, ctx, ['QUIT']);
      expect(result).toEqual({ kind: 'status', value: 'OK' });
    });

    it('lowercases command name in subscribe error message', () => {
      const result = dispatcher.dispatch(state, ctx, ['GET', 'k']);
      const err = result as { kind: 'error'; message: string };
      expect(err.message).toContain("'get'");
    });

    it('lowercases mixed-case command in subscribe error', () => {
      const result = dispatcher.dispatch(state, ctx, ['Set', 'k', 'v']);
      const err = result as { kind: 'error'; message: string };
      expect(err.message).toContain("'set'");
    });
  });

  describe('subscribe mode + MULTI interaction', () => {
    it('subscribe mode check takes priority over MULTI mode', () => {
      state.subscribed = true;
      state.inMulti = true;
      const result = dispatcher.dispatch(state, ctx, ['SET', 'k', 'v']);
      const err = result as { kind: 'error'; message: string };
      expect(err.message).toContain("Can't execute 'set'");
      expect(state.multiQueue).toHaveLength(0);
    });

    it('allowed subscribe commands are queued in MULTI', () => {
      state.subscribed = true;
      state.inMulti = true;
      const result = dispatcher.dispatch(state, ctx, ['PING']);
      expect(result).toEqual({ kind: 'status', value: 'QUEUED' });
    });
  });

  describe('createTransactionState', () => {
    it('creates default client state', () => {
      const s = createTransactionState();
      expect(s.inMulti).toBe(false);
      expect(s.multiDirty).toBe(false);
      expect(s.multiQueue).toEqual([]);
      expect(s.subscribed).toBe(false);
    });
  });

  describe('PING command', () => {
    it('returns PONG with no arguments', () => {
      const result = dispatcher.dispatch(state, ctx, ['PING']);
      expect(result).toEqual({ kind: 'status', value: 'PONG' });
    });

    it('returns bulk string with one argument', () => {
      const result = dispatcher.dispatch(state, ctx, ['PING', 'hello']);
      expect(result).toEqual({ kind: 'bulk', value: 'hello' });
    });

    it('is case-insensitive', () => {
      const result = dispatcher.dispatch(state, ctx, ['ping']);
      expect(result).toEqual({ kind: 'status', value: 'PONG' });
    });

    it('rejects more than one argument', () => {
      const result = dispatcher.dispatch(state, ctx, ['PING', 'a', 'b']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: "wrong number of arguments for 'ping' command",
      });
    });
  });

  describe('ECHO command', () => {
    it('returns the argument as bulk string', () => {
      const result = dispatcher.dispatch(state, ctx, ['ECHO', 'hello']);
      expect(result).toEqual({ kind: 'bulk', value: 'hello' });
    });

    it('rejects wrong number of arguments', () => {
      const result = dispatcher.dispatch(state, ctx, ['ECHO']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: "wrong number of arguments for 'echo' command",
      });
    });
  });

  describe('QUIT command', () => {
    it('returns OK', () => {
      const result = dispatcher.dispatch(state, ctx, ['QUIT']);
      expect(result).toEqual({ kind: 'status', value: 'OK' });
    });

    it('accepts extra arguments (arity -1)', () => {
      const result = dispatcher.dispatch(state, ctx, ['QUIT', 'extra', 'args']);
      expect(result).toEqual({ kind: 'status', value: 'OK' });
    });
  });

  describe('RESET command', () => {
    it('returns RESET status', () => {
      const result = dispatcher.dispatch(state, ctx, ['RESET']);
      expect(result).toEqual({ kind: 'status', value: 'RESET' });
    });

    it('clears MULTI state', () => {
      state.inMulti = true;
      state.multiDirty = true;
      state.multiQueue = [
        { def: makeDef(), args: ['a'] },
        { def: makeDef(), args: ['b'] },
      ];
      const result = dispatcher.dispatch(state, ctx, ['RESET']);
      expect(result).toEqual({ kind: 'status', value: 'RESET' });
      expect(state.inMulti).toBe(false);
      expect(state.multiDirty).toBe(false);
      expect(state.multiQueue).toEqual([]);
    });

    it('clears subscribed state', () => {
      state.subscribed = true;
      const result = dispatcher.dispatch(state, ctx, ['RESET']);
      expect(result).toEqual({ kind: 'status', value: 'RESET' });
      expect(state.subscribed).toBe(false);
    });

    it('rejects extra arguments', () => {
      const result = dispatcher.dispatch(state, ctx, ['RESET', 'extra']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: "wrong number of arguments for 'reset' command",
      });
    });
  });

  describe('edge cases', () => {
    it('handles RANDOMKEY (arity 1, no args)', () => {
      const result = dispatcher.dispatch(state, ctx, ['RANDOMKEY']);
      expect(result).toEqual({ kind: 'bulk', value: null });
    });

    it('handles command with custom table', () => {
      const custom = new CommandTable();
      let called = false;
      custom.register(
        makeDef({
          name: 'mycmd',
          arity: 2,
          handler: (_ctx, args) => {
            called = true;
            return statusReply(args[0] ?? '');
          },
        })
      );
      const d = new CommandDispatcher(custom);
      const result = d.dispatch(state, ctx, ['MYCMD', 'hello']);
      expect(called).toBe(true);
      expect(result).toEqual(statusReply('hello'));
    });

    it('handler receives args without command name', () => {
      const custom = new CommandTable();
      let receivedArgs: string[] = [];
      custom.register(
        makeDef({
          name: 'echo',
          arity: -1,
          handler: (_ctx, args) => {
            receivedArgs = args;
            return statusReply('OK');
          },
        })
      );
      const d = new CommandDispatcher(custom);
      d.dispatch(state, ctx, ['ECHO', 'a', 'b', 'c']);
      expect(receivedArgs).toEqual(['a', 'b', 'c']);
    });

    it('allows noauth commands when unauthenticated', () => {
      // HELLO and AUTH have the noauth flag — they should work even when
      // the client is not yet authenticated.
      const config = new ConfigStore();
      config.set('requirepass', 'secret');
      const client = new ClientStateObj(1, 100);
      client.authenticated = false;
      const authCtx: CommandContext = {
        db: ctx.db,
        engine: ctx.engine,
        client,
        config,
      };

      // HELLO (noauth) should succeed
      const helloResult = dispatcher.dispatch(state, authCtx, ['HELLO']);
      expect(helloResult.kind).toBe('array');

      // AUTH (noauth) should succeed
      const authResult = dispatcher.dispatch(state, authCtx, [
        'AUTH',
        'secret',
      ]);
      expect(authResult).toEqual({ kind: 'status', value: 'OK' });
      expect(client.authenticated).toBe(true);
    });

    it('handler receives correct CommandContext', () => {
      const custom = new CommandTable();
      let receivedCtx: CommandContext | null = null;
      custom.register(
        makeDef({
          name: 'check',
          arity: 1,
          handler: (c) => {
            receivedCtx = c;
            return statusReply('OK');
          },
        })
      );
      const d = new CommandDispatcher(custom);
      d.dispatch(state, ctx, ['CHECK']);
      expect(receivedCtx).toBe(ctx);
    });
  });

  describe('auth enforcement', () => {
    let config: ConfigStore;
    let client: ClientStateObj;
    let authCtx: CommandContext;

    beforeEach(() => {
      config = new ConfigStore();
      config.set('requirepass', 'secret');
      client = new ClientStateObj(1, 100);
      client.authenticated = false;
      authCtx = {
        db: ctx.db,
        engine: ctx.engine,
        client,
        config,
      };
    });

    it('rejects non-noauth commands when unauthenticated', () => {
      const result = dispatcher.dispatch(state, authCtx, ['GET', 'k']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'NOAUTH',
        message: 'Authentication required.',
      });
    });

    it('allows commands after authentication', () => {
      client.authenticated = true;
      authCtx.db.set('k', 'string', 'raw', 'val');
      const result = dispatcher.dispatch(state, authCtx, ['GET', 'k']);
      expect(result).toEqual({ kind: 'bulk', value: 'val' });
    });

    it('allows PING (noauth flag) when unauthenticated', () => {
      // PING has noauth via loading+stale but not noauth flag
      // Actually check: PING has flags ['fast', 'stale', 'loading']
      // It does NOT have 'noauth'. So PING should be rejected.
      const result = dispatcher.dispatch(state, authCtx, ['PING']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'NOAUTH',
        message: 'Authentication required.',
      });
    });

    it('allows HELLO (has noauth flag) when unauthenticated', () => {
      const result = dispatcher.dispatch(state, authCtx, ['HELLO']);
      expect(result.kind).toBe('array');
    });

    it('allows AUTH (has noauth flag) when unauthenticated', () => {
      const result = dispatcher.dispatch(state, authCtx, ['AUTH', 'secret']);
      expect(result).toEqual({ kind: 'status', value: 'OK' });
      expect(client.authenticated).toBe(true);
    });

    it('allows QUIT (has noauth flag) when unauthenticated', () => {
      const result = dispatcher.dispatch(state, authCtx, ['QUIT']);
      expect(result).toEqual({ kind: 'status', value: 'OK' });
    });

    it('allows RESET (has noauth flag) when unauthenticated', () => {
      const result = dispatcher.dispatch(state, authCtx, ['RESET']);
      expect(result).toEqual({ kind: 'status', value: 'RESET' });
    });

    it('skips auth check when no requirepass is set', () => {
      const noPassConfig = new ConfigStore();
      const noPassCtx: CommandContext = {
        db: ctx.db,
        engine: ctx.engine,
        client,
        config: noPassConfig,
      };
      authCtx.db.set('k', 'string', 'raw', 'val');
      const result = dispatcher.dispatch(state, noPassCtx, ['GET', 'k']);
      expect(result).toEqual({ kind: 'bulk', value: 'val' });
    });

    it('skips auth check when no config is provided', () => {
      const noConfigCtx: CommandContext = {
        db: ctx.db,
        engine: ctx.engine,
        client,
      };
      const result = dispatcher.dispatch(state, noConfigCtx, ['PING']);
      expect(result).toEqual({ kind: 'status', value: 'PONG' });
    });

    it('skips auth check when no client is provided', () => {
      const noClientCtx: CommandContext = {
        db: ctx.db,
        engine: ctx.engine,
        config,
      };
      const result = dispatcher.dispatch(state, noClientCtx, ['PING']);
      expect(result).toEqual({ kind: 'status', value: 'PONG' });
    });
  });

  describe('subscriber mode lifecycle', () => {
    let engine: import('./engine.ts').RedisEngine;
    let client: ClientStateObj;
    let subCtx: CommandContext;

    beforeEach(() => {
      const setup = createCtx();
      engine = setup.engine;
      client = new ClientStateObj(1, 100);
      subCtx = {
        db: engine.db(0),
        engine,
        client,
        pubsub: engine.pubsub,
      };
    });

    it('enters subscriber mode on first SUBSCRIBE', () => {
      expect(state.subscribed).toBe(false);
      expect(client.flagSubscribed).toBe(false);

      dispatcher.dispatch(state, subCtx, ['SUBSCRIBE', 'ch1']);
      expect(client.flagSubscribed).toBe(true);
      expect(state.subscribed).toBe(true);
    });

    it('enters subscriber mode on first PSUBSCRIBE', () => {
      dispatcher.dispatch(state, subCtx, ['PSUBSCRIBE', 'p*']);
      expect(client.flagSubscribed).toBe(true);
      expect(state.subscribed).toBe(true);
    });

    it('exits subscriber mode when all channel subscriptions removed', () => {
      dispatcher.dispatch(state, subCtx, ['SUBSCRIBE', 'ch1']);
      expect(state.subscribed).toBe(true);

      dispatcher.dispatch(state, subCtx, ['UNSUBSCRIBE', 'ch1']);
      expect(client.flagSubscribed).toBe(false);
      expect(state.subscribed).toBe(false);
    });

    it('exits subscriber mode when all pattern subscriptions removed', () => {
      dispatcher.dispatch(state, subCtx, ['PSUBSCRIBE', 'p*']);
      expect(state.subscribed).toBe(true);

      dispatcher.dispatch(state, subCtx, ['PUNSUBSCRIBE', 'p*']);
      expect(client.flagSubscribed).toBe(false);
      expect(state.subscribed).toBe(false);
    });

    it('stays in subscriber mode when channel subs removed but patterns remain', () => {
      dispatcher.dispatch(state, subCtx, ['SUBSCRIBE', 'ch1']);
      dispatcher.dispatch(state, subCtx, ['PSUBSCRIBE', 'p*']);
      expect(state.subscribed).toBe(true);

      dispatcher.dispatch(state, subCtx, ['UNSUBSCRIBE', 'ch1']);
      expect(client.flagSubscribed).toBe(true);
      expect(state.subscribed).toBe(true);
    });

    it('stays in subscriber mode when pattern subs removed but channels remain', () => {
      dispatcher.dispatch(state, subCtx, ['SUBSCRIBE', 'ch1']);
      dispatcher.dispatch(state, subCtx, ['PSUBSCRIBE', 'p*']);

      dispatcher.dispatch(state, subCtx, ['PUNSUBSCRIBE', 'p*']);
      expect(client.flagSubscribed).toBe(true);
      expect(state.subscribed).toBe(true);
    });

    it('exits subscriber mode only when both channels and patterns reach 0', () => {
      dispatcher.dispatch(state, subCtx, ['SUBSCRIBE', 'ch1', 'ch2']);
      dispatcher.dispatch(state, subCtx, ['PSUBSCRIBE', 'p1', 'p2']);

      dispatcher.dispatch(state, subCtx, ['UNSUBSCRIBE']);
      expect(state.subscribed).toBe(true);

      dispatcher.dispatch(state, subCtx, ['PUNSUBSCRIBE']);
      expect(client.flagSubscribed).toBe(false);
      expect(state.subscribed).toBe(false);
    });

    it('rejects commands after entering subscriber mode', () => {
      dispatcher.dispatch(state, subCtx, ['SUBSCRIBE', 'ch1']);

      const result = dispatcher.dispatch(state, subCtx, ['GET', 'k']);
      expect(result.kind).toBe('error');
      const err = result as { kind: 'error'; message: string };
      expect(err.message).toContain("Can't execute 'get'");
    });

    it('allows commands again after exiting subscriber mode', () => {
      dispatcher.dispatch(state, subCtx, ['SUBSCRIBE', 'ch1']);
      dispatcher.dispatch(state, subCtx, ['UNSUBSCRIBE', 'ch1']);

      subCtx.db.set('k', 'string', 'raw', 'val');
      const result = dispatcher.dispatch(state, subCtx, ['GET', 'k']);
      expect(result).toEqual({ kind: 'bulk', value: 'val' });
    });

    it('RESET clears pubsub subscriptions from manager', () => {
      dispatcher.dispatch(state, subCtx, ['SUBSCRIBE', 'ch1', 'ch2']);
      dispatcher.dispatch(state, subCtx, ['PSUBSCRIBE', 'p*']);
      expect(engine.pubsub.subscriptionCount(client.id)).toBe(3);

      dispatcher.dispatch(state, subCtx, ['RESET']);
      expect(state.subscribed).toBe(false);
      expect(client.flagSubscribed).toBe(false);
      expect(engine.pubsub.subscriptionCount(client.id)).toBe(0);
    });

    it('PING returns push-style array during subscriber mode lifecycle', () => {
      dispatcher.dispatch(state, subCtx, ['SUBSCRIBE', 'ch1']);

      const pingResult = dispatcher.dispatch(state, subCtx, ['PING']);
      expect(pingResult).toEqual({
        kind: 'array',
        value: [
          { kind: 'bulk', value: 'pong' },
          { kind: 'bulk', value: '' },
        ],
      });
    });

    it('PING returns normal PONG after exiting subscriber mode', () => {
      dispatcher.dispatch(state, subCtx, ['SUBSCRIBE', 'ch1']);
      dispatcher.dispatch(state, subCtx, ['UNSUBSCRIBE', 'ch1']);

      const pingResult = dispatcher.dispatch(state, subCtx, ['PING']);
      expect(pingResult).toEqual({ kind: 'status', value: 'PONG' });
    });
  });

  describe('ACL permission enforcement', () => {
    let engine: import('./engine.ts').RedisEngine;
    let client: ClientStateObj;
    let aclCtx: CommandContext;

    beforeEach(() => {
      const setup = createCtx();
      engine = setup.engine;
      client = new ClientStateObj(1, 100);
      client.authenticated = true;
      client.username = 'testuser';
      aclCtx = {
        db: engine.db(0),
        engine,
        client,
        acl: engine.acl,
      };
    });

    it('allows commands when user has allCommands', () => {
      const user = engine.acl.createOrGetUser('testuser');
      user.enabled = true;
      user.allCommands = true;
      user.allKeys = true;

      aclCtx.db.set('k', 'string', 'raw', 'val');
      const result = dispatcher.dispatch(state, aclCtx, ['GET', 'k']);
      expect(result).toEqual({ kind: 'bulk', value: 'val' });
    });

    it('rejects commands when user lacks allCommands', () => {
      const user = engine.acl.createOrGetUser('testuser');
      user.enabled = true;
      user.allCommands = false;
      user.allKeys = true;

      const result = dispatcher.dispatch(state, aclCtx, ['GET', 'k']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'NOPERM',
        message: "this user has no permissions to run the 'get' command",
      });
    });

    it('rejects key access when user lacks allKeys', () => {
      const user = engine.acl.createOrGetUser('testuser');
      user.enabled = true;
      user.allCommands = true;
      user.allKeys = false;

      const result = dispatcher.dispatch(state, aclCtx, ['GET', 'mykey']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'NOPERM',
        message:
          'this user has no permissions to access one of the keys used as arguments',
      });
    });

    it('allows keyless commands when user lacks allKeys', () => {
      const user = engine.acl.createOrGetUser('testuser');
      user.enabled = true;
      user.allCommands = true;
      user.allKeys = false;

      const result = dispatcher.dispatch(state, aclCtx, ['PING']);
      expect(result).toEqual({ kind: 'status', value: 'PONG' });
    });

    it('rejects pubsub commands when user lacks allChannels', () => {
      const user = engine.acl.createOrGetUser('testuser');
      user.enabled = true;
      user.allCommands = true;
      user.allKeys = true;
      user.allChannels = false;

      aclCtx.pubsub = engine.pubsub;
      const result = dispatcher.dispatch(state, aclCtx, ['SUBSCRIBE', 'ch1']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'NOPERM',
        message:
          'this user has no permissions to access one of the channels used as arguments',
      });
    });

    it('allows pubsub when user has allChannels', () => {
      const user = engine.acl.createOrGetUser('testuser');
      user.enabled = true;
      user.allCommands = true;
      user.allKeys = true;
      user.allChannels = true;

      aclCtx.pubsub = engine.pubsub;
      const result = dispatcher.dispatch(state, aclCtx, ['SUBSCRIBE', 'ch1']);
      expect(result.kind).not.toBe('error');
    });

    it('skips ACL check for noauth commands (AUTH)', () => {
      const user = engine.acl.createOrGetUser('testuser');
      user.enabled = true;
      user.allCommands = false;
      user.allKeys = false;

      const result = dispatcher.dispatch(state, aclCtx, ['AUTH', 'pass']);
      // AUTH has noauth flag — should not be blocked by ACL
      if (result.kind === 'error') {
        const err = result as {
          kind: 'error';
          prefix: string;
          message: string;
        };
        expect(err.prefix).not.toBe('NOPERM');
      }
    });

    it('skips ACL check when no ACL store is present', () => {
      const noAclCtx: CommandContext = {
        db: aclCtx.db,
        engine,
        client,
      };
      noAclCtx.db.set('k', 'string', 'raw', 'val');
      const result = dispatcher.dispatch(state, noAclCtx, ['GET', 'k']);
      expect(result).toEqual({ kind: 'bulk', value: 'val' });
    });

    it('skips ACL check when no client is present', () => {
      const noClientCtx: CommandContext = {
        db: aclCtx.db,
        engine,
        acl: engine.acl,
      };
      noClientCtx.db.set('k', 'string', 'raw', 'val');
      const result = dispatcher.dispatch(state, noClientCtx, ['GET', 'k']);
      expect(result).toEqual({ kind: 'bulk', value: 'val' });
    });

    it('allows default user with full permissions', () => {
      client.username = 'default';
      aclCtx.db.set('k', 'string', 'raw', 'val');
      const result = dispatcher.dispatch(state, aclCtx, ['GET', 'k']);
      expect(result).toEqual({ kind: 'bulk', value: 'val' });
    });

    it('rejects commands for nonexistent user', () => {
      client.username = 'ghost';
      const result = dispatcher.dispatch(state, aclCtx, ['GET', 'k']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'NOPERM',
        message: "this user has no permissions to run the 'get' command",
      });
    });

    it('logs command denial to ACL log', () => {
      const user = engine.acl.createOrGetUser('testuser');
      user.enabled = true;
      user.allCommands = false;

      dispatcher.dispatch(state, aclCtx, ['GET', 'k']);
      const log = engine.acl.getLog();
      expect(log.length).toBeGreaterThan(0);
      expect(log[0]?.reason).toBe('command');
      expect(log[0]?.object).toBe('get');
      expect(log[0]?.username).toBe('testuser');
    });

    it('logs key denial to ACL log', () => {
      const user = engine.acl.createOrGetUser('testuser');
      user.enabled = true;
      user.allCommands = true;
      user.allKeys = false;

      dispatcher.dispatch(state, aclCtx, ['GET', 'mykey']);
      const log = engine.acl.getLog();
      expect(log.length).toBeGreaterThan(0);
      expect(log[0]?.reason).toBe('key');
      expect(log[0]?.object).toBe('mykey');
      expect(log[0]?.username).toBe('testuser');
    });

    it('logs channel denial to ACL log', () => {
      const user = engine.acl.createOrGetUser('testuser');
      user.enabled = true;
      user.allCommands = true;
      user.allKeys = true;
      user.allChannels = false;

      aclCtx.pubsub = engine.pubsub;
      dispatcher.dispatch(state, aclCtx, ['SUBSCRIBE', 'secret-ch']);
      const log = engine.acl.getLog();
      expect(log.length).toBeGreaterThan(0);
      expect(log[0]?.reason).toBe('channel');
      expect(log[0]?.object).toBe('secret-ch');
      expect(log[0]?.username).toBe('testuser');
    });

    it('rejects commands in MULTI mode and returns NOPERM instead of QUEUED', () => {
      const user = engine.acl.createOrGetUser('testuser');
      user.enabled = true;
      user.allCommands = false;

      state.inMulti = true;
      const result = dispatcher.dispatch(state, aclCtx, ['GET', 'k']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'NOPERM',
        message: "this user has no permissions to run the 'get' command",
      });
      // Should NOT be queued
      expect(state.multiQueue).toHaveLength(0);
    });

    it('checks key permission for multi-key commands', () => {
      const user = engine.acl.createOrGetUser('testuser');
      user.enabled = true;
      user.allCommands = true;
      user.allKeys = false;

      const result = dispatcher.dispatch(state, aclCtx, ['MGET', 'k1', 'k2']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'NOPERM',
        message:
          'this user has no permissions to access one of the keys used as arguments',
      });
    });

    it('uses lowercase command name in NOPERM message', () => {
      const user = engine.acl.createOrGetUser('testuser');
      user.enabled = true;
      user.allCommands = false;

      const result = dispatcher.dispatch(state, aclCtx, ['SET', 'k', 'v']);
      const err = result as { kind: 'error'; prefix: string; message: string };
      expect(err.message).toContain("'set'");
    });
  });
});
