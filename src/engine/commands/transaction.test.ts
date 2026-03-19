import { describe, it, expect, beforeEach } from 'vitest';
import { CommandDispatcher, createClientState } from '../command-dispatcher.ts';
import type { ClientState } from '../command-dispatcher.ts';
import { createCommandTable } from '../command-registry.ts';
import { RedisEngine } from '../engine.ts';
import type { CommandContext, Reply } from '../types.ts';
import { statusReply, errorReply } from '../types.ts';
import { ClientState as ServerClientState } from '../../server/client-state.ts';

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

describe('MULTI command', () => {
  let dispatcher: CommandDispatcher;
  let state: ClientState;
  let ctx: CommandContext;

  beforeEach(() => {
    const table = createCommandTable();
    dispatcher = new CommandDispatcher(table);
    state = createClientState();
    const setup = createCtx();
    ctx = setup.ctx;
  });

  describe('entering transaction mode', () => {
    it('returns OK', () => {
      const result = dispatcher.dispatch(state, ctx, ['MULTI']);
      expect(result).toEqual(statusReply('OK'));
    });

    it('sets inMulti flag', () => {
      dispatcher.dispatch(state, ctx, ['MULTI']);
      expect(state.inMulti).toBe(true);
    });

    it('initializes empty queue', () => {
      dispatcher.dispatch(state, ctx, ['MULTI']);
      expect(state.multiQueue).toEqual([]);
    });

    it('clears multiDirty flag', () => {
      state.multiDirty = true;
      dispatcher.dispatch(state, ctx, ['MULTI']);
      expect(state.multiDirty).toBe(false);
    });

    it('is case-insensitive', () => {
      const result = dispatcher.dispatch(state, ctx, ['multi']);
      expect(result).toEqual(statusReply('OK'));
      expect(state.inMulti).toBe(true);
    });

    it('handles mixed case', () => {
      const result = dispatcher.dispatch(state, ctx, ['Multi']);
      expect(result).toEqual(statusReply('OK'));
      expect(state.inMulti).toBe(true);
    });

    it('rejects extra arguments', () => {
      const result = dispatcher.dispatch(state, ctx, ['MULTI', 'extra']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: "wrong number of arguments for 'multi' command",
      });
      expect(state.inMulti).toBe(false);
    });

    it('sets flagMulti on server client state', () => {
      const client = new ServerClientState(1, 100);
      ctx.client = client;
      dispatcher.dispatch(state, ctx, ['MULTI']);
      expect(client.flagMulti).toBe(true);
    });
  });

  describe('nested MULTI', () => {
    beforeEach(() => {
      dispatcher.dispatch(state, ctx, ['MULTI']);
    });

    it('returns error for nested MULTI', () => {
      const result = dispatcher.dispatch(state, ctx, ['MULTI']);
      expect(result).toEqual(
        errorReply('ERR', 'MULTI calls can not be nested')
      );
    });

    it('does not abort transaction on nested MULTI', () => {
      dispatcher.dispatch(state, ctx, ['MULTI']);
      expect(state.multiDirty).toBe(false);
    });

    it('does not add nested MULTI to queue', () => {
      dispatcher.dispatch(state, ctx, ['MULTI']);
      expect(state.multiQueue).toHaveLength(0);
    });

    it('transaction continues normally after nested MULTI error', () => {
      dispatcher.dispatch(state, ctx, ['MULTI']);
      const result = dispatcher.dispatch(state, ctx, ['SET', 'k', 'v']);
      expect(result).toEqual(statusReply('QUEUED'));
      expect(state.multiQueue).toHaveLength(1);
    });
  });

  describe('command queuing after MULTI', () => {
    beforeEach(() => {
      dispatcher.dispatch(state, ctx, ['MULTI']);
    });

    it('queues SET and returns QUEUED', () => {
      const result = dispatcher.dispatch(state, ctx, ['SET', 'k', 'v']);
      expect(result).toEqual(statusReply('QUEUED'));
      expect(state.multiQueue).toHaveLength(1);
      expect(state.multiQueue[0]?.def.name).toBe('set');
      expect(state.multiQueue[0]?.args).toEqual(['k', 'v']);
    });

    it('queues GET and returns QUEUED', () => {
      const result = dispatcher.dispatch(state, ctx, ['GET', 'k']);
      expect(result).toEqual(statusReply('QUEUED'));
      expect(state.multiQueue).toHaveLength(1);
      expect(state.multiQueue[0]?.def.name).toBe('get');
      expect(state.multiQueue[0]?.args).toEqual(['k']);
    });

    it('queues multiple commands in order', () => {
      dispatcher.dispatch(state, ctx, ['SET', 'k', 'v']);
      dispatcher.dispatch(state, ctx, ['GET', 'k']);
      dispatcher.dispatch(state, ctx, ['DEL', 'k']);
      dispatcher.dispatch(state, ctx, ['INCR', 'counter']);
      expect(state.multiQueue).toHaveLength(4);
      expect(state.multiQueue[0]?.def.name).toBe('set');
      expect(state.multiQueue[1]?.def.name).toBe('get');
      expect(state.multiQueue[2]?.def.name).toBe('del');
      expect(state.multiQueue[3]?.def.name).toBe('incr');
    });

    it('does not execute commands during queuing', () => {
      dispatcher.dispatch(state, ctx, ['SET', 'k', 'v']);
      expect(ctx.db.get('k')).toBeNull();
    });

    it('queues commands with variable arguments', () => {
      dispatcher.dispatch(state, ctx, ['SET', 'k', 'v', 'EX', '10']);
      expect(state.multiQueue).toHaveLength(1);
      expect(state.multiQueue[0]?.args).toEqual(['k', 'v', 'EX', '10']);
    });

    it('queues DEL with multiple keys', () => {
      dispatcher.dispatch(state, ctx, ['DEL', 'a', 'b', 'c']);
      expect(state.multiQueue).toHaveLength(1);
      expect(state.multiQueue[0]?.args).toEqual(['a', 'b', 'c']);
    });
  });

  describe('syntax error detection at queue time', () => {
    beforeEach(() => {
      dispatcher.dispatch(state, ctx, ['MULTI']);
    });

    it('marks dirty on unknown command', () => {
      const result = dispatcher.dispatch(state, ctx, ['NOSUCHCMD', 'arg']);
      expect(result.kind).toBe('error');
      expect(state.multiDirty).toBe(true);
      expect(state.multiQueue).toHaveLength(0);
    });

    it('marks dirty on wrong arity', () => {
      const result = dispatcher.dispatch(state, ctx, ['GET']);
      expect(result.kind).toBe('error');
      expect(state.multiDirty).toBe(true);
      expect(state.multiQueue).toHaveLength(0);
    });

    it('marks dirty on GET with too many args', () => {
      const result = dispatcher.dispatch(state, ctx, ['GET', 'a', 'b']);
      expect(result.kind).toBe('error');
      expect(state.multiDirty).toBe(true);
      expect(state.multiQueue).toHaveLength(0);
    });

    it('preserves dirty flag across multiple errors', () => {
      dispatcher.dispatch(state, ctx, ['NOSUCHCMD']);
      expect(state.multiDirty).toBe(true);
      // Valid command still queues
      dispatcher.dispatch(state, ctx, ['SET', 'k', 'v']);
      expect(state.multiQueue).toHaveLength(1);
      // Dirty flag remains
      expect(state.multiDirty).toBe(true);
    });

    it('queues valid commands even after errors', () => {
      dispatcher.dispatch(state, ctx, ['NOSUCHCMD']);
      const result = dispatcher.dispatch(state, ctx, ['SET', 'k', 'v']);
      expect(result).toEqual(statusReply('QUEUED'));
      expect(state.multiQueue).toHaveLength(1);
    });
  });

  describe('passthrough commands in MULTI', () => {
    beforeEach(() => {
      dispatcher.dispatch(state, ctx, ['MULTI']);
    });

    it('does not queue EXEC', () => {
      // EXEC is not registered yet — returns unknown command error
      // but it must NOT be queued
      dispatcher.dispatch(state, ctx, ['EXEC']);
      expect(state.multiQueue).toHaveLength(0);
    });

    it('does not queue DISCARD', () => {
      // DISCARD is not registered yet
      dispatcher.dispatch(state, ctx, ['DISCARD']);
      expect(state.multiQueue).toHaveLength(0);
    });

    it('returns error for WATCH inside MULTI', () => {
      const result = dispatcher.dispatch(state, ctx, ['WATCH', 'k']);
      expect(result).toEqual(
        errorReply('ERR', 'WATCH inside MULTI is not allowed')
      );
      expect(state.multiQueue).toHaveLength(0);
    });

    it('WATCH error does not mark dirty', () => {
      dispatcher.dispatch(state, ctx, ['WATCH', 'k']);
      expect(state.multiDirty).toBe(false);
    });

    it('nested MULTI error does not mark dirty', () => {
      dispatcher.dispatch(state, ctx, ['MULTI']);
      expect(state.multiDirty).toBe(false);
    });
  });

  describe('MULTI with subcommands', () => {
    beforeEach(() => {
      dispatcher.dispatch(state, ctx, ['MULTI']);
    });

    it('queues OBJECT ENCODING', () => {
      ctx.db.set('k', 'string', 'embstr', 'hi');
      const result = dispatcher.dispatch(state, ctx, [
        'OBJECT',
        'ENCODING',
        'k',
      ]);
      expect(result).toEqual(statusReply('QUEUED'));
      expect(state.multiQueue).toHaveLength(1);
    });

    it('marks dirty on subcommand arity error', () => {
      const result = dispatcher.dispatch(state, ctx, ['OBJECT', 'ENCODING']);
      expect(result.kind).toBe('error');
      expect(state.multiDirty).toBe(true);
      expect(state.multiQueue).toHaveLength(0);
    });
  });

  describe('RESET clears transaction state', () => {
    it('clears inMulti and queue on RESET', () => {
      dispatcher.dispatch(state, ctx, ['MULTI']);
      dispatcher.dispatch(state, ctx, ['SET', 'k', 'v']);
      expect(state.inMulti).toBe(true);
      expect(state.multiQueue).toHaveLength(1);

      dispatcher.dispatch(state, ctx, ['RESET']);
      expect(state.inMulti).toBe(false);
      expect(state.multiQueue).toHaveLength(0);
      expect(state.multiDirty).toBe(false);
    });

    it('clears dirty flag on RESET', () => {
      dispatcher.dispatch(state, ctx, ['MULTI']);
      dispatcher.dispatch(state, ctx, ['NOSUCHCMD']);
      expect(state.multiDirty).toBe(true);

      dispatcher.dispatch(state, ctx, ['RESET']);
      expect(state.multiDirty).toBe(false);
      expect(state.inMulti).toBe(false);
    });
  });

  describe('subscribe mode blocks MULTI', () => {
    it('rejects MULTI in subscribe mode', () => {
      state.subscribed = true;
      const result = dispatcher.dispatch(state, ctx, ['MULTI']);
      expect(result.kind).toBe('error');
      expect(state.inMulti).toBe(false);
    });
  });

  describe('MULTI visibility in COMMAND introspection', () => {
    it('MULTI is listed in COMMAND COUNT', () => {
      const before = dispatcher.dispatch(state, ctx, ['COMMAND', 'COUNT']);
      expect(before.kind).toBe('integer');
      // MULTI should be included in the count
      const count = (before as { kind: 'integer'; value: number }).value;
      expect(count).toBeGreaterThan(0);
    });

    it('MULTI is found via COMMAND INFO', () => {
      const result = dispatcher.dispatch(state, ctx, [
        'COMMAND',
        'INFO',
        'MULTI',
      ]);
      expect(result.kind).toBe('array');
      const arr = result as { kind: 'array'; value: Reply[] };
      // Should not be nil (command exists)
      expect(arr.value[0]).not.toEqual({ kind: 'bulk', value: null });
    });
  });
});
