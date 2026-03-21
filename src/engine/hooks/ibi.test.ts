import { describe, it, expect, vi } from 'vitest';
import {
  IbiHookManager,
  resolveIbiHooks,
  type CommandHookCtx,
  type IbiHookName,
} from './ibi.ts';
import type { Reply } from '../types.ts';
import { statusReply, bulkReply, errorReply } from '../types.ts';

function makeCtx(overrides?: Partial<CommandHookCtx>): CommandHookCtx {
  return {
    command: 'GET',
    args: ['key1'],
    clientId: 1,
    db: 0,
    meta: {
      categories: new Set(['@read', '@string', '@fast']),
      flags: new Set(['readonly', 'fast']),
    },
    ...overrides,
  };
}

describe('resolveIbiHooks', () => {
  it('resolves @string + @read to redis:string:read', () => {
    const hooks = resolveIbiHooks(new Set(['@read', '@string', '@fast']));
    expect(hooks).toEqual(['redis:string:read']);
  });

  it('resolves @string + @write to redis:string:write', () => {
    const hooks = resolveIbiHooks(new Set(['@write', '@string']));
    expect(hooks).toEqual(['redis:string:write']);
  });

  it('resolves @hash + @read to redis:hash:read', () => {
    const hooks = resolveIbiHooks(new Set(['@read', '@hash', '@fast']));
    expect(hooks).toEqual(['redis:hash:read']);
  });

  it('resolves @hash + @write to redis:hash:write', () => {
    const hooks = resolveIbiHooks(new Set(['@write', '@hash', '@fast']));
    expect(hooks).toEqual(['redis:hash:write']);
  });

  it('resolves @list + @read to redis:list:read', () => {
    const hooks = resolveIbiHooks(new Set(['@read', '@list', '@slow']));
    expect(hooks).toEqual(['redis:list:read']);
  });

  it('resolves @list + @write to redis:list:write', () => {
    const hooks = resolveIbiHooks(new Set(['@write', '@list', '@fast']));
    expect(hooks).toEqual(['redis:list:write']);
  });

  it('resolves @set + @read to redis:set:read', () => {
    const hooks = resolveIbiHooks(new Set(['@read', '@set', '@fast']));
    expect(hooks).toEqual(['redis:set:read']);
  });

  it('resolves @set + @write to redis:set:write', () => {
    const hooks = resolveIbiHooks(new Set(['@write', '@set', '@fast']));
    expect(hooks).toEqual(['redis:set:write']);
  });

  it('resolves @sortedset + @read to redis:zset:read', () => {
    const hooks = resolveIbiHooks(new Set(['@read', '@sortedset', '@fast']));
    expect(hooks).toEqual(['redis:zset:read']);
  });

  it('resolves @sortedset + @write to redis:zset:write', () => {
    const hooks = resolveIbiHooks(new Set(['@write', '@sortedset', '@fast']));
    expect(hooks).toEqual(['redis:zset:write']);
  });

  it('resolves @stream + @read to redis:stream:read', () => {
    const hooks = resolveIbiHooks(new Set(['@read', '@stream', '@fast']));
    expect(hooks).toEqual(['redis:stream:read']);
  });

  it('resolves @stream + @write to redis:stream:write', () => {
    const hooks = resolveIbiHooks(new Set(['@write', '@stream', '@fast']));
    expect(hooks).toEqual(['redis:stream:write']);
  });

  it('resolves @bitmap to redis:string (bitmap is string-encoded)', () => {
    const hooks = resolveIbiHooks(new Set(['@write', '@bitmap']));
    expect(hooks).toEqual(['redis:string:write']);
  });

  it('resolves @hyperloglog to redis:string (hll is string-encoded)', () => {
    const hooks = resolveIbiHooks(new Set(['@write', '@hyperloglog', '@fast']));
    expect(hooks).toEqual(['redis:string:write']);
  });

  it('resolves @pubsub to redis:pubsub', () => {
    const hooks = resolveIbiHooks(new Set(['@pubsub', '@slow']));
    expect(hooks).toEqual(['redis:pubsub']);
  });

  it('resolves @transaction to redis:tx', () => {
    const hooks = resolveIbiHooks(new Set(['@fast', '@transaction']));
    expect(hooks).toEqual(['redis:tx']);
  });

  it('resolves @scripting to redis:script', () => {
    const hooks = resolveIbiHooks(new Set(['@slow', '@scripting']));
    expect(hooks).toEqual(['redis:script']);
  });

  it('resolves @keyspace to redis:key', () => {
    const hooks = resolveIbiHooks(new Set(['@keyspace', '@write']));
    expect(hooks).toEqual(['redis:key']);
  });

  it('resolves @connection to redis:connection', () => {
    const hooks = resolveIbiHooks(new Set(['@fast', '@connection']));
    expect(hooks).toEqual(['redis:connection']);
  });

  it('resolves @admin without family to redis:server', () => {
    const hooks = resolveIbiHooks(new Set(['@admin', '@slow']));
    expect(hooks).toEqual(['redis:server']);
  });

  it('resolves @dangerous without family to redis:server', () => {
    const hooks = resolveIbiHooks(new Set(['@slow', '@dangerous']));
    expect(hooks).toEqual(['redis:server']);
  });

  it('resolves @slow alone to redis:server', () => {
    const hooks = resolveIbiHooks(new Set(['@slow']));
    expect(hooks).toEqual(['redis:server']);
  });

  it('resolves multi-category command (SORT has @set @sortedset @list)', () => {
    const hooks = resolveIbiHooks(
      new Set(['@write', '@set', '@sortedset', '@list'])
    );
    expect(hooks).toContain('redis:set:write');
    expect(hooks).toContain('redis:zset:write');
    expect(hooks).toContain('redis:list:write');
    expect(hooks).toHaveLength(3);
  });

  it('resolves @admin + @connection to redis:connection (not server)', () => {
    const hooks = resolveIbiHooks(new Set(['@admin', '@slow', '@connection']));
    expect(hooks).toEqual(['redis:connection']);
  });

  it('deduplicates when @bitmap and @string both present', () => {
    // Hypothetical: if a command had both @bitmap and @string
    const hooks = resolveIbiHooks(new Set(['@read', '@bitmap', '@string']));
    expect(hooks).toEqual(['redis:string:read']);
  });
});

describe('IbiHookManager', () => {
  describe('hook access', () => {
    it('returns hook instance for valid name', () => {
      const mgr = new IbiHookManager();
      const hook = mgr.hook('redis:command');
      expect(hook).toBeDefined();
      expect(typeof hook.tap).toBe('function');
    });

    it('throws for unknown hook name', () => {
      const mgr = new IbiHookManager();
      expect(() => mgr.hook('redis:unknown' as IbiHookName)).toThrow(
        'Unknown IBI hook'
      );
    });

    it('returns same instance on repeated access', () => {
      const mgr = new IbiHookManager();
      expect(mgr.hook('redis:command')).toBe(mgr.hook('redis:command'));
    });
  });

  describe('hasHooks', () => {
    it('returns false when no hooks registered', () => {
      const mgr = new IbiHookManager();
      expect(mgr.hasHooks).toBe(false);
    });

    it('returns true when a hook is registered', () => {
      const mgr = new IbiHookManager();
      mgr.hook('redis:command').tap(async (_ctx, next) => next());
      expect(mgr.hasHooks).toBe(true);
    });

    it('returns false after untapping all hooks', () => {
      const mgr = new IbiHookManager();
      const fn = async (_ctx: CommandHookCtx, next: () => Promise<Reply>) =>
        next();
      mgr.hook('redis:command').tap(fn);
      mgr.hook('redis:command').untap(fn);
      expect(mgr.hasHooks).toBe(false);
    });
  });

  describe('execute', () => {
    it('executes baseFn directly when no hooks registered', async () => {
      const mgr = new IbiHookManager();
      const ctx = makeCtx();
      const result = await mgr.execute(ctx, ['redis:string:read'], () =>
        bulkReply('hello')
      );
      expect(result).toEqual(bulkReply('hello'));
    });

    it('fires redis:command hook on every command', async () => {
      const mgr = new IbiHookManager();
      const seen: string[] = [];
      mgr.hook('redis:command').tap(async (ctx, next) => {
        seen.push(ctx.command);
        return next();
      });

      const ctx1 = makeCtx({ command: 'GET' });
      await mgr.execute(ctx1, ['redis:string:read'], () => bulkReply('v'));

      const ctx2 = makeCtx({
        command: 'HSET',
        meta: {
          categories: new Set(['@write', '@hash', '@fast']),
          flags: new Set(['write', 'fast']),
        },
      });
      await mgr.execute(ctx2, ['redis:hash:write'], () => statusReply('OK'));

      expect(seen).toEqual(['GET', 'HSET']);
    });

    it('fires family-specific hook for matching commands', async () => {
      const mgr = new IbiHookManager();
      const stringReads: string[] = [];
      mgr.hook('redis:string:read').tap(async (ctx, next) => {
        stringReads.push(ctx.command);
        return next();
      });

      const ctx = makeCtx({ command: 'GET' });
      await mgr.execute(ctx, ['redis:string:read'], () => bulkReply('v'));
      expect(stringReads).toEqual(['GET']);
    });

    it('does not fire unrelated family hooks', async () => {
      const mgr = new IbiHookManager();
      const hashWrites = vi.fn();
      mgr.hook('redis:hash:write').tap(async (_ctx, next) => {
        hashWrites();
        return next();
      });

      const ctx = makeCtx({ command: 'GET' });
      await mgr.execute(ctx, ['redis:string:read'], () => bulkReply('v'));
      expect(hashWrites).not.toHaveBeenCalled();
    });

    it('fires multiple family hooks for multi-category commands', async () => {
      const mgr = new IbiHookManager();
      const fired: string[] = [];
      mgr.hook('redis:set:write').tap(async (_ctx, next) => {
        fired.push('set:write');
        return next();
      });
      mgr.hook('redis:list:write').tap(async (_ctx, next) => {
        fired.push('list:write');
        return next();
      });
      mgr.hook('redis:zset:write').tap(async (_ctx, next) => {
        fired.push('zset:write');
        return next();
      });

      const ctx = makeCtx({
        command: 'SORT',
        meta: {
          categories: new Set(['@write', '@set', '@sortedset', '@list']),
          flags: new Set(['write']),
        },
      });
      const familyHooks = resolveIbiHooks(ctx.meta.categories);
      await mgr.execute(ctx, familyHooks, () => statusReply('OK'));

      expect(fired).toContain('set:write');
      expect(fired).toContain('list:write');
      expect(fired).toContain('zset:write');
    });

    it('chains redis:command as outermost, family as inner', async () => {
      const mgr = new IbiHookManager();
      const order: string[] = [];

      mgr.hook('redis:command').tap(async (_ctx, next) => {
        order.push('command:pre');
        const r = await next();
        order.push('command:post');
        return r;
      });
      mgr.hook('redis:string:read').tap(async (_ctx, next) => {
        order.push('string:read:pre');
        const r = await next();
        order.push('string:read:post');
        return r;
      });

      const ctx = makeCtx();
      await mgr.execute(ctx, ['redis:string:read'], () => {
        order.push('base');
        return bulkReply('v');
      });

      expect(order).toEqual([
        'command:pre',
        'string:read:pre',
        'base',
        'string:read:post',
        'command:post',
      ]);
    });
  });

  describe('context population', () => {
    it('passes command name to hooks', async () => {
      const mgr = new IbiHookManager();
      let seenCommand = '';
      mgr.hook('redis:command').tap(async (ctx, next) => {
        seenCommand = ctx.command;
        return next();
      });

      await mgr.execute(
        makeCtx({ command: 'MGET' }),
        ['redis:string:read'],
        () => bulkReply('v')
      );
      expect(seenCommand).toBe('MGET');
    });

    it('passes args to hooks', async () => {
      const mgr = new IbiHookManager();
      let seenArgs: readonly string[] = [];
      mgr.hook('redis:command').tap(async (ctx, next) => {
        seenArgs = ctx.args;
        return next();
      });

      await mgr.execute(
        makeCtx({ args: ['k1', 'k2', 'k3'] }),
        ['redis:string:read'],
        () => bulkReply('v')
      );
      expect(seenArgs).toEqual(['k1', 'k2', 'k3']);
    });

    it('passes clientId to hooks', async () => {
      const mgr = new IbiHookManager();
      let seenId = -1;
      mgr.hook('redis:command').tap(async (ctx, next) => {
        seenId = ctx.clientId;
        return next();
      });

      await mgr.execute(makeCtx({ clientId: 42 }), ['redis:string:read'], () =>
        bulkReply('v')
      );
      expect(seenId).toBe(42);
    });

    it('passes db index to hooks', async () => {
      const mgr = new IbiHookManager();
      let seenDb = -1;
      mgr.hook('redis:command').tap(async (ctx, next) => {
        seenDb = ctx.db;
        return next();
      });

      await mgr.execute(makeCtx({ db: 3 }), ['redis:string:read'], () =>
        bulkReply('v')
      );
      expect(seenDb).toBe(3);
    });

    it('passes meta (categories and flags) to hooks', async () => {
      const mgr = new IbiHookManager();
      let seenCategories: ReadonlySet<string> | undefined;
      mgr.hook('redis:command').tap(async (ctx, next) => {
        seenCategories = ctx.meta.categories;
        return next();
      });

      const categories = new Set(['@write', '@string']);
      const flags = new Set(['write'] as const);
      await mgr.execute(
        makeCtx({
          meta: {
            categories,
            flags,
          },
        }),
        ['redis:string:write'],
        () => statusReply('OK')
      );

      expect(seenCategories).toBe(categories);
    });
  });

  describe('interception', () => {
    it('sim can short-circuit to return custom reply', async () => {
      const mgr = new IbiHookManager();
      mgr.hook('redis:command').tap(async (_ctx, _next) => {
        return errorReply('ERR', 'blocked by sim');
      });

      const baseFn = vi.fn(() => bulkReply('v'));
      const result = await mgr.execute(
        makeCtx(),
        ['redis:string:read'],
        baseFn
      );
      expect(result).toEqual(errorReply('ERR', 'blocked by sim'));
      expect(baseFn).not.toHaveBeenCalled();
    });

    it('sim can transform reply via family hook', async () => {
      const mgr = new IbiHookManager();
      mgr.hook('redis:string:read').tap(async (_ctx, next) => {
        const reply = await next();
        if (reply.kind === 'bulk' && reply.value !== null) {
          return bulkReply(reply.value.toUpperCase());
        }
        return reply;
      });

      const result = await mgr.execute(makeCtx(), ['redis:string:read'], () =>
        bulkReply('hello')
      );
      expect(result).toEqual(bulkReply('HELLO'));
    });

    it('sim can intercept via family hook and short-circuit', async () => {
      const mgr = new IbiHookManager();
      mgr.hook('redis:hash:write').tap(async (_ctx, _next) => {
        return errorReply('ERR', 'hash writes disabled');
      });

      const baseFn = vi.fn(() => statusReply('OK'));
      const result = await mgr.execute(
        makeCtx({
          command: 'HSET',
          meta: {
            categories: new Set(['@write', '@hash']),
            flags: new Set(['write']),
          },
        }),
        ['redis:hash:write'],
        baseFn
      );
      expect(result).toEqual(errorReply('ERR', 'hash writes disabled'));
      expect(baseFn).not.toHaveBeenCalled();
    });

    it('sim can throw to fail the command', async () => {
      const mgr = new IbiHookManager();
      mgr.hook('redis:command').tap(async (_ctx, _next) => {
        throw new Error('sim failure');
      });

      await expect(
        mgr.execute(makeCtx(), ['redis:string:read'], () => bulkReply('v'))
      ).rejects.toThrow('sim failure');
    });

    it('redis:command hook can intercept before family hook runs', async () => {
      const mgr = new IbiHookManager();
      const familyFn = vi.fn(
        async (_ctx: CommandHookCtx, next: () => Promise<Reply>) => next()
      );

      mgr.hook('redis:command').tap(async (_ctx, _next) => {
        return errorReply('ERR', 'blocked at command level');
      });
      mgr.hook('redis:string:read').tap(familyFn);

      const baseFn = vi.fn(() => bulkReply('v'));
      const result = await mgr.execute(
        makeCtx(),
        ['redis:string:read'],
        baseFn
      );

      expect(result).toEqual(errorReply('ERR', 'blocked at command level'));
      expect(familyFn).not.toHaveBeenCalled();
      expect(baseFn).not.toHaveBeenCalled();
    });

    it('family hook can intercept while redis:command still sees result', async () => {
      const mgr = new IbiHookManager();
      let commandSaw: Reply | null = null;

      mgr.hook('redis:command').tap(async (_ctx, next) => {
        const r = await next();
        commandSaw = r;
        return r;
      });
      mgr.hook('redis:string:read').tap(async (_ctx, _next) => {
        return bulkReply('intercepted');
      });

      const result = await mgr.execute(makeCtx(), ['redis:string:read'], () =>
        bulkReply('original')
      );

      expect(result).toEqual(bulkReply('intercepted'));
      expect(commandSaw).toEqual(bulkReply('intercepted'));
    });
  });

  describe('all hook families', () => {
    const families: [string, IbiHookName, Set<string>][] = [
      ['string:read', 'redis:string:read', new Set(['@read', '@string'])],
      ['string:write', 'redis:string:write', new Set(['@write', '@string'])],
      ['hash:read', 'redis:hash:read', new Set(['@read', '@hash'])],
      ['hash:write', 'redis:hash:write', new Set(['@write', '@hash'])],
      ['list:read', 'redis:list:read', new Set(['@read', '@list'])],
      ['list:write', 'redis:list:write', new Set(['@write', '@list'])],
      ['set:read', 'redis:set:read', new Set(['@read', '@set'])],
      ['set:write', 'redis:set:write', new Set(['@write', '@set'])],
      ['zset:read', 'redis:zset:read', new Set(['@read', '@sortedset'])],
      ['zset:write', 'redis:zset:write', new Set(['@write', '@sortedset'])],
      ['stream:read', 'redis:stream:read', new Set(['@read', '@stream'])],
      ['stream:write', 'redis:stream:write', new Set(['@write', '@stream'])],
      ['pubsub', 'redis:pubsub', new Set(['@pubsub', '@slow'])],
      ['tx', 'redis:tx', new Set(['@fast', '@transaction'])],
      ['script', 'redis:script', new Set(['@slow', '@scripting'])],
      ['key', 'redis:key', new Set(['@keyspace', '@write'])],
      ['connection', 'redis:connection', new Set(['@fast', '@connection'])],
      ['server', 'redis:server', new Set(['@admin', '@slow'])],
    ];

    for (const [label, hookName, categories] of families) {
      it(`fires ${label} hook correctly`, async () => {
        const mgr = new IbiHookManager();
        const fired = vi.fn();
        mgr.hook(hookName).tap(async (_ctx, next) => {
          fired();
          return next();
        });

        const familyHooks = resolveIbiHooks(categories);
        expect(familyHooks).toContain(hookName);

        const ctx = makeCtx({
          meta: {
            categories,
            flags: new Set(),
          },
        });
        await mgr.execute(ctx, familyHooks, () => statusReply('OK'));
        expect(fired).toHaveBeenCalledOnce();
      });
    }
  });
});
